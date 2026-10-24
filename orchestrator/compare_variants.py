"""Paired local Chord/SWIM benchmark; no changes to either node implementation.

python3 -m orchestrator.compare_variants --chord-binary /tmp/chord --swim-binary /tmp/swim
"""

import argparse
import concurrent.futures
import contextlib
import csv
import datetime
import hashlib
import http.client
import http.server
import json
import os
import random
import socket
import statistics
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from orchestrator import runner
from orchestrator.standard_swim_verify import summarize, wait_for

PHASES = (
    "healthy",
    "gray_failure",
    "gray_recovered",
    "crash_recovery",
    "post_crash",
    "restart",
)


class PeerProxy:
    """Delay incoming peer RPCs, never management/client lookup requests.

    Every node advertises a proxy endpoint and listens on a private backend port.
    This is application-level latency injection, not packet loss or TCP netem.
    Counts each request once at its destination, including failed attempts.
    """

    def __init__(self, backend):
        self.backend = backend
        self.lock = threading.Lock()
        self.delay = 0
        self.closed = threading.Event()
        self.slots = threading.BoundedSemaphore(64)
        self.counts = {
            "requests": 0,
            "request_body_bytes": 0,
            "response_body_bytes": 0,
            "errors": 0,
            "delayed_requests": 0,
            "rejected": 0,
        }
        proxy = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                self.forward()

            def do_POST(self):
                self.forward()

            def forward(self):
                if not proxy.slots.acquire(blocking=False):
                    with proxy.lock:
                        proxy.counts["rejected"] += 1
                    self.send_error(503)
                    return
                conn = None
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    if not 0 <= length <= 8 * 1024 * 1024:
                        self.send_error(413)
                        return
                    self.connection.settimeout(3)
                    body = self.rfile.read(length)
                    with proxy.lock:
                        delay = proxy.delay
                        proxy.counts["requests"] += 1
                        proxy.counts["request_body_bytes"] += len(body)
                        proxy.counts["delayed_requests"] += bool(delay)
                    if proxy.closed.wait(delay):
                        return
                    conn = http.client.HTTPConnection(
                        "127.0.0.1", proxy.backend, timeout=2
                    )
                    conn.request(
                        self.command,
                        self.path,
                        body=body,
                        headers={
                            "Content-Type": self.headers.get(
                                "Content-Type", "application/json"
                            )
                        },
                    )
                    reply = conn.getresponse()
                    payload = reply.read(8 * 1024 * 1024 + 1)
                    if len(payload) > 8 * 1024 * 1024:
                        raise ValueError("oversize proxy response")
                    with proxy.lock:
                        proxy.counts["response_body_bytes"] += len(payload)
                    self.send_response(reply.status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (OSError, ValueError, http.client.HTTPException):
                    with proxy.lock:
                        proxy.counts["errors"] += 1
                    with contextlib.suppress(OSError):
                        self.send_error(502)
                finally:
                    if conn:
                        conn.close()
                    proxy.slots.release()

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_port

    def snapshot(self):
        with self.lock:
            return dict(self.counts)

    def set_delay(self, seconds):
        with self.lock:
            self.delay = seconds

    def close(self):
        self.closed.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)


def owner_for(peers, key):
    return next(
        (peer for peer in sorted(peers, key=lambda p: p["id"]) if peer["id"] >= key),
        min(peers, key=lambda p: p["id"]),
    )


def query(peer, key, alive, epoch, scheduled):
    expected = owner_for(alive, key)
    start = time.monotonic()
    reply = runner.get_json(
        f"http://127.0.0.1:{peer['port']}/lookup?id={key}", timeout=2
    )
    owner = reply.get("successor") if reply else None
    try:
        correct = bool(
            reply
            and reply.get("success")
            and owner
            and int(owner["id"]) == expected["id"]
            and owner["addr"] == expected["addr"]
        )
    except (ValueError, KeyError, TypeError):
        correct = False
    return {
        "initiator": peer["id"],
        "key": key,
        "expected_owner": expected["id"],
        "correct": correct,
        "rpc_success": bool(reply and reply.get("success")),
        "owner": owner,
        "hops": reply.get("hops") if reply else None,
        "latency_ms": (time.monotonic() - start) * 1000,
        "scheduled_sec": scheduled,
        "started_sec": start - epoch,
        "completed_sec": time.monotonic() - epoch,
    }


def recovery_time(samples, nodes, window=1.0):
    """Two consecutive complete one-second cohorts, >=99% correct PER survivor.

    Include completion time, so a late response cannot backdate recovery.
    A failed/omitted scheduled request prevents its cohort from passing.
    """
    cohorts = {}
    for sample in samples:
        cohorts.setdefault(int(sample["scheduled_sec"] / window), []).append(sample)
    previous = None
    for index, batch in sorted(cohorts.items()):
        groups = [[s for s in batch if s["initiator"] == node] for node in nodes]
        passed = all(
            group and sum(s["correct"] for s in group) / len(group) >= 0.99
            for group in groups
        )
        if passed and previous == index - 1:
            both = cohorts[index - 1] + batch
            return max((index + 1) * window, max(s["completed_sec"] for s in both))
        previous = index if passed else None
    return None


def workload(peers, alive, keys, seconds, rate, epoch=None):
    """Fixed offered schedule; bounded in-flight requests, explicit overload failures."""
    epoch = time.monotonic() if epoch is None else epoch
    futures, samples = [], []
    slots = threading.BoundedSemaphore(32)
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        for i in range(int(seconds * rate)):
            scheduled = i / rate
            time.sleep(max(0, epoch + scheduled - time.monotonic()))
            peer = peers[i % len(peers)]
            key = keys[(i // len(peers)) % len(keys)]
            if slots.acquire(blocking=False):
                future = pool.submit(query, peer, key, alive, epoch, scheduled)
                future.add_done_callback(lambda _: slots.release())
                futures.append(future)
            else:
                samples.append(
                    {
                        "initiator": peer["id"],
                        "key": key,
                        "correct": False,
                        "rpc_success": False,
                        "hops": None,
                        "latency_ms": None,
                        "scheduled_sec": scheduled,
                        "completed_sec": time.monotonic() - epoch,
                        "harness_overload": True,
                    }
                )
        samples.extend(f.result() for f in futures)
    return sorted(samples, key=lambda s: s["scheduled_sec"])


def run_variant(variant, binary, args, repetition, directory):
    processes, proxies, reservations = [], [], []
    logs = []
    result = {
        "variant": variant,
        "repetition": repetition,
        "phases": {},
        "binary_sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
        "status": "incomplete",
    }
    config = {
        "M": str(args.m),
        "R": str(min(4, args.nodes - 1)),
        "STABILIZE_INTERVAL_MS": str(args.stabilize_ms),
        "FIX_FINGERS_INTERVAL_MS": str(args.fix_ms),
        "CHECK_PRED_INTERVAL_MS": str(args.stabilize_ms),
        "RPC_TIMEOUT_MS": "250",
        "SWIM_INTERVAL_MS": "200",
        "SWIM_DIRECT_TIMEOUT_MS": "50",
        "SWIM_INDIRECT_HELPERS": "2",
        "SWIM_SUSPICION_TIMEOUT_MS": "1200",
        "SWIM_MAX_MEMBERS": "128",
        "LOG_LEVEL": "info",
    }
    result["config"] = config
    try:
        with contextlib.ExitStack() as handles:
            for _ in range(args.nodes):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                reservations.append(sock)
                proxies.append(PeerProxy(sock.getsockname()[1]))
            peers = [
                {
                    "id": i * (1 << args.m) // args.nodes,
                    "port": s.getsockname()[1],
                    "addr": f"127.0.0.1:{proxies[i].port}",
                }
                for i, s in enumerate(reservations)
            ]
            victim_index = args.nodes // 2
            result["victim_id"] = peers[victim_index]["id"]
            result["peers"] = peers
            survivors = [p for i, p in enumerate(peers) if i != victim_index]
            keys = list(range(1 << args.m))
            random.Random(args.seed).shuffle(keys)
            # Include boundaries early even with short phases.
            keys = list(
                dict.fromkeys(
                    [p["id"] for p in peers]
                    + [(p["id"] - 1) % (1 << args.m) for p in peers]
                    + keys
                )
            )

            def start(index):
                env = {
                    k: v
                    for k, v in os.environ.items()
                    if k not in config and not k.startswith("SWIM_")
                }
                env.update(
                    config,
                    PORT=str(peers[index]["port"]),
                    NODE_ID=str(peers[index]["id"]),
                    ADVERTISE_ADDR=peers[index]["addr"],
                    BOOTSTRAP_ADDR=peers[0]["addr"] if index else "",
                    SWIM_STATE_FILE=str(
                        directory / f"{variant}-{repetition}-{index}.state"
                    ),
                )
                path = directory / f"{variant}-{repetition}-{index}.log"
                logs.append(path)
                log = handles.enter_context(path.open("a"))
                return subprocess.Popen([binary], env=env, stdout=log, stderr=log)

            def converged(active):
                states = [
                    runner.get_json(f"http://127.0.0.1:{p['port']}/state", timeout=0.5)
                    for p in active
                ]
                if not all(states) or not runner.ring_converged(states):
                    return False
                if variant == "chord_standard_swim":
                    for peer in active:
                        members = runner.get_json(
                            f"http://127.0.0.1:{peer['port']}/membership", timeout=0.5
                        )
                        if not members or {
                            int(m["peer"]["id"])
                            for m in members
                            if m["state"] == "ALIVE"
                        } != {p["id"] for p in active}:
                            return False
                return True

            def snapshots(active):
                return [
                    runner.get_json(f"http://127.0.0.1:{p['port']}/metrics")
                    for p in active
                ]

            def phase(name, active, seconds, action=None, end_action=None):
                print(
                    f"[{repetition + 1}/{args.repeats}] {variant}: {name}", flush=True
                )
                before = [p.snapshot() for p in proxies]
                metrics_before = snapshots(active)
                started_utc = datetime.datetime.now(datetime.UTC).isoformat()
                epoch = time.monotonic()
                if action:
                    action()
                timer = threading.Timer(seconds, end_action) if end_action else None
                if timer:
                    timer.start()
                try:
                    samples = workload(active, active, keys, seconds, args.rate, epoch)
                    # Finish the full fault interval even when the final reply is fast.
                    time.sleep(max(0, epoch + seconds - time.monotonic()))
                finally:
                    if timer:
                        timer.join(timeout=3)
                        end_action()
                after = [p.snapshot() for p in proxies]
                metrics_after = snapshots(active)
                traffic = {
                    k: sum(a[k] - b[k] for a, b in zip(after, before))
                    for k in before[0]
                }
                # Skipped requests count against correctness but have no measured
                # latency. Never invent a zero-millisecond network observation.
                summary = summarize(
                    [s for s in samples if not s.get("harness_overload")]
                )
                summary["queries"] = len(samples)
                summary["correctness_pct"] = (
                    sum(s["correct"] for s in samples) / len(samples) * 100
                )
                summary.update(
                    duration_sec=seconds,
                    samples=samples,
                    peer_traffic=traffic,
                    started_utc=started_utc,
                    elapsed_sec=time.monotonic() - epoch,
                    metrics_before=metrics_before,
                    metrics_after=metrics_after,
                    harness_overloads=sum(
                        s.get("harness_overload", False) for s in samples
                    ),
                )
                summary["membership_entries"] = (
                    [
                        m.get("swim", {}).get("membership_records")
                        for m in metrics_after
                        if m
                    ]
                    if variant == "chord_standard_swim"
                    else None
                )
                heaps = [m["storage"]["alloc_bytes"] for m in metrics_after if m]
                summary["mean_process_heap_bytes"] = (
                    statistics.mean(heaps) if heaps else None
                )
                summary["chord_repair_counts"] = counter_delta(
                    metrics_before, metrics_after, "repair_counts"
                )
                summary["swim_counters"] = (
                    counter_delta(
                        [m.get("swim", {}) if m else {} for m in metrics_before],
                        [m.get("swim", {}) if m else {} for m in metrics_after],
                        "counters",
                    )
                    if variant == "chord_standard_swim"
                    else None
                )
                result["phases"][name] = summary
                return samples

            try:
                for i, reservation in enumerate(reservations):
                    reservation.close()
                    processes.append(start(i))
                wait_for(lambda: converged(peers), "initial convergence", seconds=60)
                phase("healthy", peers, args.phase_seconds)
                # The oracle still includes the live, delayed victim.
                phase(
                    "gray_failure",
                    peers,
                    args.phase_seconds,
                    action=lambda: proxies[victim_index].set_delay(
                        args.gray_delay_ms / 1000
                    ),
                    end_action=lambda: proxies[victim_index].set_delay(0),
                )
                wait_for(lambda: converged(peers), "gray fault healing", seconds=60)
                phase("gray_recovered", peers, args.phase_seconds)

                def crash():
                    processes[victim_index].kill()
                    processes[victim_index].wait(timeout=5)

                samples = phase(
                    "crash_recovery", survivors, args.recovery_seconds, action=crash
                )
                result["phases"]["crash_recovery"]["recovery_time_sec"] = recovery_time(
                    samples, [p["id"] for p in survivors]
                )
                wait_for(
                    lambda: converged(survivors), "post-crash convergence", seconds=60
                )
                phase("post_crash", survivors, args.phase_seconds)
                processes[victim_index] = start(victim_index)
                wait_for(lambda: converged(peers), "restart convergence", seconds=60)
                phase("restart", peers, args.phase_seconds)
                result["status"] = "completed"
                result["acceptance_passed"] = all(
                    result["phases"][name]["correctness_pct"] == 100
                    for name in ("healthy", "gray_recovered", "post_crash", "restart")
                ) and not any(p["harness_overloads"] for p in result["phases"].values())
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.terminate()
                for process in processes:
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                        result["forced_shutdown"] = True
    except (
        OSError,
        ValueError,
        RuntimeError,
        AssertionError,
        subprocess.SubprocessError,
    ) as exc:
        result["status"] = "failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        for sock in reservations:
            sock.close()
        for proxy in proxies:
            proxy.close()
        result["processes_exited"] = all(p.poll() is not None for p in processes)
        events = []
        for path in set(logs):
            text = path.read_text()
            if "DATA RACE" in text or "panic:" in text:
                result["runtime_error"] = True
            for line in text.splitlines():
                with contextlib.suppress(json.JSONDecodeError):
                    events.append(json.loads(line))
        result["swim_events"] = events if variant == "chord_standard_swim" else None
        result["acceptance_passed"] = bool(
            result.get("acceptance_passed")
            and not result.get("runtime_error")
            and not result.get("forced_shutdown")
            and result["processes_exited"]
        )
    return result


def counter_delta(before, after, field):
    """Same live process cohort before/after; don't treat failed scrapes as zeros."""
    if not all(before) or not all(after):
        return None
    keys = set().union(*(m.get(field, {}) for m in before + after))
    return {
        key: sum(
            a.get(field, {}).get(key, 0) - b.get(field, {}).get(key, 0)
            for a, b in zip(after, before)
        )
        for key in sorted(keys)
    }


def comparison_rows(runs):
    rows = []
    for variant in ("chord", "chord_standard_swim"):
        selected = [r for r in runs if r["variant"] == variant]
        for phase in PHASES:
            values = [r["phases"][phase] for r in selected if phase in r["phases"]]
            row = {"variant": variant, "phase": phase, "completed_runs": len(values)}
            for field in (
                "correctness_pct",
                "latency_p50_ms",
                "latency_p95_ms",
                "latency_p99_ms",
                "mean_hops",
                "recovery_time_sec",
                "mean_process_heap_bytes",
            ):
                observations = [v[field] for v in values if v.get(field) is not None]
                row[field] = (
                    {
                        "median": statistics.median(observations),
                        "min": min(observations),
                        "max": max(observations),
                    }
                    if observations
                    else None
                )
            rows.append(row)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chord-binary", required=True)
    parser.add_argument("--swim-binary", required=True)
    parser.add_argument("--output", default="reports/chord-swim-comparison.json")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--nodes", type=int, default=4, choices=range(4, 9))
    parser.add_argument("--m", type=int, default=4, choices=range(3, 13))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--phase-seconds", type=int, default=4)
    parser.add_argument("--recovery-seconds", type=int, default=6)
    parser.add_argument("--rate", type=int, default=24)
    parser.add_argument("--gray-delay-ms", type=int, default=100)
    parser.add_argument("--stabilize-ms", type=int, default=200)
    parser.add_argument("--fix-ms", type=int, default=100)
    args = parser.parse_args()
    if (
        min(
            args.repeats,
            args.phase_seconds,
            args.rate,
            args.gray_delay_ms,
            args.stabilize_ms,
            args.fix_ms,
        )
        < 1
        or args.recovery_seconds < 2
    ):
        parser.error("positive parameters required; recovery-seconds must be >=2")
    if args.rate < args.nodes:
        parser.error("rate must be >= nodes so every survivor is sampled each second")
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    binaries = {
        "chord": str(Path(args.chord_binary).resolve()),
        "chord_standard_swim": str(Path(args.swim_binary).resolve()),
    }
    result = {
        "schema_version": 1,
        "run_at_utc": datetime.datetime.now(datetime.UTC).isoformat(),
        "methodology": vars(args),
        "runs": [],
        "fault_model": "100% of incoming peer RPCs to one live victim delayed before forwarding; client lookups/management bypass proxy; no packet loss; after healing kill same victim",
        "recovery_rule": "two consecutive one-second scheduled cohorts >=99% oracle-correct per survivor; completion time included; null means not observed within fixed window",
        "traffic_units": "destination proxy request attempts and JSON body bytes, including SWIM; no HTTP/TCP/IP overhead; proxy errors include client disconnects",
        "limitations": [
            "Local proxy overhead affects both variants; not equivalent to netem packet delay.",
            "Small-ring sampled lookup results do not prove arbitrary-ring or partition correctness.",
            "Sequential alternating paired runs; no statistical significance claim.",
            "Null metrics are unsupported/unobserved, never inferred zero.",
        ],
    }
    with tempfile.TemporaryDirectory(prefix="chord-swim-compare-") as directory:
        for repetition in range(args.repeats):
            order = list(binaries) if repetition % 2 == 0 else list(reversed(binaries))
            for variant in order:
                result["runs"].append(
                    run_variant(
                        variant, binaries[variant], args, repetition, Path(directory)
                    )
                )
                result["comparison"] = comparison_rows(result["runs"])
                output.write_text(json.dumps(result, indent=2) + "\n")
    result["acceptance_passed"] = all(r["acceptance_passed"] for r in result["runs"])
    output.write_text(json.dumps(result, indent=2) + "\n")
    write_markdown(output.with_suffix(".md"), result)
    write_csv(output.with_suffix(".csv"), result)
    print(f"Saved {output} and {output.with_suffix('.md')}")
    if not result["acceptance_passed"]:
        raise SystemExit(
            "At least one paired run failed acceptance; inspect preserved result"
        )


def write_markdown(path, result):
    lines = [
        "# Chord / Chord + standard SWIM comparison",
        "",
        f"Generated {result['run_at_utc']}. Acceptance passed: {result['acceptance_passed']}.",
        "",
        "Same offered lookup schedule, IDs, Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order.",
        "",
        "Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.",
        "",
        "| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in result["comparison"]:
        values = [
            f"{row[field]['median']:.3f}" if row[field] else "—"
            for field in (
                "correctness_pct",
                "latency_p50_ms",
                "latency_p95_ms",
                "latency_p99_ms",
                "mean_hops",
                "recovery_time_sec",
            )
        ]
        lines.append(
            f"| {row['phase']} | {row['variant']} | {row['completed_runs']} | "
            + " | ".join(values)
            + " |"
        )
    lines += [
        "",
        f"Fault: {result['fault_model']}.",
        "",
        f"Recovery: {result['recovery_rule']}.",
        "",
        f"Traffic: {result['traffic_units']}.",
        "",
        "Gray-failure oracle includes the delayed live node. Only the hard-crash phase removes that node from the oracle. Gray recovery is checked before the crash; restart uses the same identity/address and persisted SWIM incarnation.",
        "",
        "SWIM events/metrics have no baseline equivalent and are null for Chord. Whole-process memory snapshots are provided, not SWIM-only memory. Lookup latency includes failed requests; mean hops includes correct requests only.",
        "",
    ]
    lines += [f"- {item}" for item in result["limitations"]]
    lines += [
        "",
        "## Peer traffic and memory",
        "",
        "Per-phase medians across runs. Traffic is measured at proxies; Go heap is a whole-process snapshot averaged over active nodes. Neither includes proxy Python memory.",
        "",
        "| Phase | Variant | Peer requests | Body bytes | Mean Go heap KiB |",
        "|---|---|---:|---:|---:|",
    ]
    for row in result["comparison"]:
        phases = [
            run["phases"][row["phase"]]
            for run in result["runs"]
            if run["variant"] == row["variant"] and row["phase"] in run["phases"]
        ]
        if not phases:
            continue
        messages = statistics.median(p["peer_traffic"]["requests"] for p in phases)
        payload = statistics.median(
            p["peer_traffic"]["request_body_bytes"]
            + p["peer_traffic"]["response_body_bytes"]
            for p in phases
        )
        heaps = [
            p["mean_process_heap_bytes"] / 1024
            for p in phases
            if p.get("mean_process_heap_bytes") is not None
        ]
        heap = f"{statistics.median(heaps):.1f}" if heaps else "—"
        lines.append(
            f"| {row['phase']} | {row['variant']} | {messages:.0f} | {payload:.0f} | {heap} |"
        )
    lines += [
        "",
        "The recovery statistic has one-second cohorts and a two-second minimum. Equal values at that floor cannot establish equal detection/repair speed. Inspect raw request completion times and SWIM events for finer timing; lookup-triggered Chord repair can finish before SWIM confirmation.",
        "",
        "Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.",
    ]
    lines += [
        "",
        "## SWIM diagnostics",
        "",
        "Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.",
        "",
        "| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |",
        "|---|---:|---:|---:|---:|",
    ]
    for phase in PHASES:
        counters = [
            (run["phases"].get(phase, {}).get("swim_counters") or {})
            for run in result["runs"]
            if run["variant"] == "chord_standard_swim"
        ]
        values = [
            sum(c.get(key, 0) for c in counters)
            for key in (
                "peer_suspected",
                "peer_refuted",
                "peer_dead",
                "references_repaired",
            )
        ]
        lines.append(f"| {phase} | " + " | ".join(str(v) for v in values) + " |")
    path.write_text("\n".join(lines) + "\n")


def write_csv(path, result):
    fields = [
        "repetition",
        "variant",
        "phase",
        "queries",
        "correct",
        "correctness_pct",
        "rpc_successes",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        "mean_hops",
        "recovery_time_sec",
        "peer_requests",
        "peer_body_bytes",
        "mean_process_heap_bytes",
        "harness_overloads",
    ]
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for run in result["runs"]:
            for name in PHASES:
                if name not in run["phases"]:
                    continue
                phase = run["phases"][name]
                row = {field: phase.get(field) for field in fields}
                traffic = phase["peer_traffic"]
                row.update(
                    repetition=run["repetition"],
                    variant=run["variant"],
                    phase=name,
                    peer_requests=traffic["requests"],
                    peer_body_bytes=traffic["request_body_bytes"]
                    + traffic["response_body_bytes"],
                )
                writer.writerow(row)


if __name__ == "__main__":
    main()
