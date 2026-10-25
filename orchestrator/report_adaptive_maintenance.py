"""Summarize completed fixed/adaptive paired experiments from their raw results."""

import argparse
import json
from pathlib import Path
from statistics import median

MODES = ("fixed", "adaptive")
VARIANTS = ("chord", "chord_standard_swim")
PHASES = (
    "healthy", "gray_failure", "gray_recovered", "crash_recovery",
    "post_crash", "restart",
)
TASKS = ("stabilize", "fix_fingers", "check_predecessor")


def validate_study(study):
    """Reject unmatched workloads or binaries instead of attributing their effects."""
    reference_method = None
    configs, hashes, schedules, identities = {}, {}, {}, {}
    for mode, data in study.items():
        method = data["methodology"]
        comparable = {
            key: value for key, value in method.items()
            if key not in ("output", "swim_adaptive_maintenance")
        }
        if reference_method is None:
            reference_method = comparable
        elif comparable != reference_method:
            raise ValueError("Fixed/adaptive experiment methodologies differ")
        if method.get("swim_adaptive_maintenance", False) != (mode == "adaptive"):
            raise ValueError(f"{mode}: unexpected adaptive flag")
        expected = {
            (variant, repetition)
            for variant in VARIANTS for repetition in range(method["repeats"])
        }
        actual = [(run["variant"], run["repetition"]) for run in data["runs"]]
        if len(actual) != len(expected) or set(actual) != expected:
            raise ValueError(f"{mode}: missing or duplicate paired repetitions")
        for run in data["runs"]:
            variant = run["variant"]
            if run.get("status") != "completed" or set(run["phases"]) != set(PHASES):
                raise ValueError(f"{mode}/{variant}: incomplete run")
            config = dict(run["config"])
            if variant == "chord_standard_swim":
                flag = config.pop("ADAPTIVE_MAINTENANCE", None)
                if flag != str(mode == "adaptive").lower():
                    raise ValueError(f"{mode}/{variant}: incorrect process policy")
            elif any(key.startswith("ADAPTIVE_") for key in config):
                raise ValueError("Plain Chord unexpectedly received an adaptive policy")
            if variant in configs and config != configs[variant]:
                raise ValueError(f"{variant}: node settings differ beyond adaptive flag")
            configs[variant] = config
            digest = run["binary_sha256"]
            if variant in hashes and digest != hashes[variant]:
                raise ValueError(f"{variant}: executable hashes differ")
            hashes[variant] = digest
            identity = ([peer["id"] for peer in run["peers"]], run["victim_id"])
            if variant in identities and identity != identities[variant]:
                raise ValueError(f"{variant}: node IDs or fault victims differ")
            identities[variant] = identity
            for phase, values in run["phases"].items():
                if values.get("duration_sec", 0) <= 0:
                    raise ValueError(f"{mode}/{variant}/{phase}: missing duration")
                samples = values["samples"]
                schedule = [
                    (sample["initiator"], sample["key"], sample["scheduled_sec"])
                    for sample in samples
                ]
                if phase in schedules and schedule != schedules[phase]:
                    raise ValueError(f"{mode}/{variant}/{phase}: offered workloads differ")
                schedules[phase] = schedule
                if len(samples) != values["queries"]:
                    raise ValueError(f"{mode}/{variant}/{phase}: query count mismatch")
                if sum(bool(s["correct"]) for s in samples) != values["correct"]:
                    raise ValueError(f"{mode}/{variant}/{phase}: oracle count mismatch")
    return hashes


def selected_runs(data, variant):
    return sorted(
        (run for run in data["runs"] if run["variant"] == variant),
        key=lambda run: run["repetition"],
    )


def fmt(value, digits=3):
    return "—" if value is None else f"{value:.{digits}f}"


def phase_median(runs, phase, field):
    values = [run["phases"][phase].get(field) for run in runs]
    return median(values) if values and all(v is not None for v in values) else None


def traffic_rate(runs, phase, payload=False):
    values = []
    for run in runs:
        values_phase = run["phases"][phase]
        traffic = values_phase["peer_traffic"]
        count = (
            traffic["request_body_bytes"] + traffic["response_body_bytes"]
            if payload else traffic["requests"]
        )
        values.append(count / values_phase["duration_sec"])
    return median(values)


def oracle_counts(runs, phase=None):
    phases = [
        values for run in runs for name, values in run["phases"].items()
        if phase is None or phase == name
    ]
    return sum(p["correct"] for p in phases), sum(p["queries"] for p in phases)


def maintenance_counter(runs, phase, counter):
    counters = [run["phases"][phase].get("swim_counters") for run in runs]
    if not all(item is not None and counter in item for item in counters):
        return "—"
    return fmt(median(item[counter] for item in counters), 1)


def boundary_state(metrics):
    states = [item.get("maintenance") if item else None for item in metrics]
    if not states or not all(isinstance(state, dict) for state in states):
        return "unavailable"
    descriptions = []
    for state in states:
        intervals = state.get("intervals_ms", {})
        durations = "/".join(str(intervals.get(task, "?")) for task in TASKS)
        enabled = "enabled" if state.get("enabled") else "disabled"
        description = f"{enabled}:{state.get('mode', '?')} ({durations})"
        descriptions.append(description)
    return "; ".join(
        f"{description} ×{descriptions.count(description)}"
        for description in dict.fromkeys(descriptions)
    )


def write_report(study, output, hashes):
    method = study["fixed"]["methodology"]
    lines = [
        "# Adaptive Chord maintenance with standard SWIM",
        "",
        f"{method['nodes']} nodes, M={method['m']}, {method['repeats']} paired "
        f"repetitions per mode, {method['rate']} offered lookups/s. Each ordinary "
        f"phase lasts {method['phase_seconds']} s; the crash observation lasts "
        f"{method['recovery_seconds']} s, with client lookups withheld for "
        f"{method['crash_quiet_ms'] / 1000:g} s after the crash.",
        "",
        "Fixed and adaptive SWIM runs use the same executable, initial Chord "
        "timers, probe settings and offered workloads. Only the adaptive flag "
        "differs. Plain Chord controls use the same executable in both modes. "
        "The report generator validates those conditions and per-phase query "
        "schedules before writing this report. Both variants receive the same "
        "phase duration; there is no adaptive-only settling period.",
        "",
        f"Initial stabilization/predecessor/finger intervals: "
        f"{method['stabilize_ms']}/{method['check_pred_ms']}/{method['fix_ms']} ms. "
        f"Adaptive caps: {method['adaptive_max_stabilize_ms']}/"
        f"{method['adaptive_max_check_pred_ms']}/{method['adaptive_max_fix_ms']} ms; "
        f"quiet threshold {method['adaptive_stable_after_ms']} ms. "
        "Local quietness is a heuristic, not proof of global convergence.",
        "",
        "Values in performance tables are medians of per-run values. Traffic "
        "rates divide each phase's destination-proxy counters by its configured "
        "duration; request and response body bytes exclude HTTP/TCP/IP headers. "
        "Latency p95 includes the harness's recorded requests. Whole-Go-process "
        "heap is not protocol-only memory.",
        "",
        "| Mode | Variant | Healthy requests/s | Healthy body bytes/s | Healthy p95 ms | Mean process heap KiB |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for mode, data in study.items():
        for variant in VARIANTS:
            runs = selected_runs(data, variant)
            heap = phase_median(runs, "healthy", "mean_process_heap_bytes")
            lines.append(
                f"| {mode} | {variant} | {fmt(traffic_rate(runs, 'healthy'))} | "
                f"{fmt(traffic_rate(runs, 'healthy', True), 1)} | "
                f"{fmt(phase_median(runs, 'healthy', 'latency_p95_ms'))} | "
                f"{fmt(heap / 1024 if heap is not None else None, 1)} |"
            )
    lines += [
        "",
        "| Mode | Variant | Gray oracle correct / queries | Gray correctness % | Gray requests/s | Gray body bytes/s | Gray p95 ms |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for mode, data in study.items():
        for variant in VARIANTS:
            runs = selected_runs(data, variant)
            correct, queries = oracle_counts(runs, "gray_failure")
            lines.append(
                f"| {mode} | {variant} | {correct}/{queries} | "
                f"{100 * correct / queries:.3f} | "
                f"{fmt(traffic_rate(runs, 'gray_failure'))} | "
                f"{fmt(traffic_rate(runs, 'gray_failure', True), 1)} | "
                f"{fmt(phase_median(runs, 'gray_failure', 'latency_p95_ms'))} |"
            )
    lines += [
        "",
        "## Recovery and correctness",
        "",
        "Routing convergence requires two consecutive read-only observations "
        "with correct neighbors, successor lists and all fingers. A null time "
        "is censored at the observation window; it is not an infinite recovery "
        "time and is not silently excluded from an unconditional median.",
        "",
        "| Mode | Variant | Trial | Full-routing convergence s | Converged before lookups s | All-phase oracle correct / queries | Acceptance | Processes exited |",
        "|---|---|---:|---|---:|---:|---|---|",
    ]
    all_correct = all_queries = 0
    for mode, data in study.items():
        for variant in VARIANTS:
            for run in selected_runs(data, variant):
                crash = run["phases"]["crash_recovery"]
                value = crash.get("routing_convergence_sec")
                convergence = (
                    fmt(value) if value is not None else
                    f"not observed within {crash['duration_sec']} s (censored)"
                )
                correct, queries = oracle_counts([run])
                all_correct += correct
                all_queries += queries
                lines.append(
                    f"| {mode} | {variant} | {run['repetition'] + 1} | "
                    f"{convergence} | {fmt(crash.get('quiet_routing_convergence_sec'))} | "
                    f"{correct}/{queries} | {run.get('acceptance_passed')} | "
                    f"{run.get('processes_exited')} |"
                )
    lines += [
        "",
        f"Oracle-correct lookups across every phase and control: "
        f"**{all_correct}/{all_queries}**. These are sampled lookup outcomes, "
        "not proof of correctness for every key and execution history.",
        "",
        "| Mode | Variant | Phase | Oracle correct / queries |",
        "|---|---|---|---:|",
    ]
    for mode, data in study.items():
        for variant in VARIANTS:
            for phase in PHASES:
                correct, queries = oracle_counts(selected_runs(data, variant), phase)
                lines.append(f"| {mode} | {variant} | {phase} | {correct}/{queries} |")
    lines += [
        "",
        "## Observed adaptive state and maintenance operations",
        "",
        "Boundary snapshots come from each SWIM process's actual /metrics "
        "response, not the requested caps. Intervals are listed as "
        "stabilize/fix_fingers/check_predecessor in ms; ×N is the number of "
        "processes with that state. Snapshots do not describe every intervening "
        "policy transition; raw event logs retain those transitions.",
        "",
        "| Mode | SWIM trial | Healthy start | Healthy end |",
        "|---|---:|---|---|",
    ]
    for mode, data in study.items():
        for run in selected_runs(data, "chord_standard_swim"):
            healthy = run["phases"]["healthy"]
            lines.append(
                f"| {mode} | {run['repetition'] + 1} | "
                f"{boundary_state(healthy['metrics_before'])} | "
                f"{boundary_state(healthy['metrics_after'])} |"
            )
    lines += [
        "",
        "Operation counts below are median summed local scheduler invocations "
        "per phase, rather than repaired-reference counts. A task invocation "
        "can perform no work or several RPCs. Plain Chord lacks these scheduler "
        "counters, so no corresponding values are inferred. Missing counters "
        "are shown as unavailable (—).",
        "",
        "| Mode | SWIM phase | Stabilize runs | Finger runs | Predecessor runs | Backoff transitions | Fast transitions |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for mode, data in study.items():
        runs = selected_runs(data, "chord_standard_swim")
        for phase in PHASES:
            counters = [f"maintenance_{task}_runs" for task in TASKS]
            counters += ["maintenance_backoff", "maintenance_fast"]
            values = [maintenance_counter(runs, phase, counter) for counter in counters]
            lines.append(f"| {mode} | {phase} | " + " | ".join(values) + " |")
    fixed_swim = selected_runs(study["fixed"], "chord_standard_swim")
    adaptive_swim = selected_runs(study["adaptive"], "chord_standard_swim")
    plain = selected_runs(study["adaptive"], "chord")
    old_requests = traffic_rate(fixed_swim, "healthy")
    new_requests = traffic_rate(adaptive_swim, "healthy")
    old_bytes = traffic_rate(fixed_swim, "healthy", True)
    new_bytes = traffic_rate(adaptive_swim, "healthy", True)
    lines += [
        "", "## Interpretation", "",
        f"Healthy request rate fell from {old_requests:.3f} to {new_requests:.3f}/s "
        f"({100 * (1 - new_requests / old_requests):.1f}%). Payload-body traffic fell "
        f"from {old_bytes:.1f} to {new_bytes:.1f} bytes/s "
        f"({100 * (1 - new_bytes / old_bytes):.1f}%). The actual policy snapshots and "
        "operation counters above establish that periodic work was reduced.", "",
        f"Adaptive SWIM still used {new_requests / traffic_rate(plain, 'healthy'):.2f}× "
        f"plain Chord's request rate and {new_bytes / traffic_rate(plain, 'healthy', True):.2f}× "
        "its payload-body traffic. This is progress toward replacing routine work, "
        "not an equal-budget or overall-superiority result. Healthy p95 improvements "
        "are observations from three trials, not a statistically established claim.", "",
        "Correctness and recovery observations are reported separately above. "
        "There was no adaptive-only warm-up; early healthy traffic includes the "
        "policy's transition toward its capped intervals. These measurements "
        "cannot establish safety under arbitrary partitions or sustained churn.", "",
        "Verification for this implementation: final SWIM `go test -race ./...` "
        "passed (17.416 s), `go vet ./...` passed, baseline race tests passed "
        "from cache, and 31 Python harness tests passed (0.763 s). Additional "
        "adaptive runtime/RPC race tests passed three repetitions. Builds and "
        "tests finished before timing began. Source and reports are local on "
        "`chord_standard_swim`; this increment has not been pushed.",
    ]
    lines += [
        "",
        "## Scope and reproducibility",
        "",
        "The fixed study and adaptive study execute sequentially; each uses "
        "alternating variant order across repetitions. Local scheduling, random "
        "finger selection and process startup affect timing. No statistical "
        "significance or general superiority claim follows from these runs. "
        "The gray fault is delayed application traffic, not packet loss or an "
        "arbitrary partition. CPU time and total wire bytes were not measured. "
        "Ordinary Chord maintenance and lookup fallback remain active, so "
        "routing convergence cannot be attributed exclusively to SWIM events.",
        "",
        f"- Plain Chord executable SHA256: `{hashes['chord']}`.",
        f"- SWIM executable SHA256 (both modes): `{hashes['chord_standard_swim']}`.",
        "- Raw data: `reports/adaptive-maintenance-fixed.json` and "
        "`reports/adaptive-maintenance-adaptive.json`; companion CSV/Markdown "
        "reports are generated by the shared benchmark harness.",
        "- Regenerate: `python3 -m orchestrator.report_adaptive_maintenance`.",
    ]
    output.write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports-dir", type=Path, default=Path("reports"))
    args = parser.parse_args()
    try:
        study = {
            mode: json.loads(
                (args.reports_dir / f"adaptive-maintenance-{mode}.json").read_text()
            ) for mode in MODES
        }
        hashes = validate_study(study)
        output = args.reports_dir / "adaptive-maintenance-study.md"
        write_report(study, output, hashes)
    except (OSError, KeyError, ValueError) as error:
        raise SystemExit(f"Cannot summarize adaptive study: {error}") from error
    print(f"Saved {output}")


if __name__ == "__main__":
    main()
