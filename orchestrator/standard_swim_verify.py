"""Local oracle benchmark for the standard full-membership SWIM variant.

Build with: (cd chord_standard_swim && go build -race -o /tmp/chord-standard-swim .)
Run with: python3 -m orchestrator.standard_swim_verify --binary /tmp/chord-standard-swim
"""

import argparse
import concurrent.futures
import contextlib
import datetime
import hashlib
import json
import math
import os
import random
import socket
import subprocess
import tempfile
import time
from pathlib import Path

from orchestrator import runner


def wait_for(predicate, description, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise TimeoutError(description)


def percentile(values, fraction):
    return (
        sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]
        if values
        else None
    )


def lookup_batch(peers, keys):
    """Independent oracle, including exact node identity and endpoint."""
    peers = sorted(peers, key=lambda peer: peer["id"])
    samples = []

    def query(probe):
        initiator, key = probe
        expected = next((peer for peer in peers if peer["id"] >= key), peers[0])
        start = time.monotonic()
        reply = runner.get_json(
            f"http://127.0.0.1:{initiator['port']}/lookup?id={key}", timeout=2
        )
        owner = reply.get("successor") if reply else None
        correct = bool(
            reply
            and reply.get("success")
            and owner
            and int(owner["id"]) == expected["id"]
            and owner["addr"] == expected["addr"]
        )
        return {
            "initiator": initiator["id"],
            "key": key,
            "expected_owner": expected["id"],
            "owner": int(owner["id"]) if owner else None,
            "correct": correct,
            "rpc_success": bool(reply and reply.get("success")),
            "hops": reply.get("hops") if reply else None,
            "latency_ms": (time.monotonic() - start) * 1000,
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        samples.extend(pool.map(query, [(peer, key) for peer in peers for key in keys]))
    return samples


def summarize(samples):
    latencies = [sample["latency_ms"] for sample in samples]
    hops = [sample["hops"] for sample in samples if sample["correct"]]
    return {
        "queries": len(samples),
        "correct": sum(sample["correct"] for sample in samples),
        "rpc_successes": sum(sample["rpc_success"] for sample in samples),
        "correctness_pct": sum(sample["correct"] for sample in samples)
        / len(samples)
        * 100
        if samples
        else None,
        "latency_p50_ms": percentile(latencies, 0.50),
        "latency_p95_ms": percentile(latencies, 0.95),
        "latency_p99_ms": percentile(latencies, 0.99),
        "mean_hops": sum(hops) / len(hops) if hops else None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--nodes", type=int, default=4, choices=range(4, 9))
    parser.add_argument("--m", type=int, default=8)
    parser.add_argument("--fix-ms", type=int, default=500)
    parser.add_argument(
        "--observe-ms",
        type=int,
        default=0,
        help="quiescent observation before querying; use 0 for continuous recovery samples",
    )
    parser.add_argument("--output", default="reports/standard-swim-results.json")
    args = parser.parse_args()
    if args.fix_ms < 1 or not 0 <= args.observe_ms <= 10000:
        parser.error("fix-ms must be positive and observe-ms must be in [0,10000]")
    if not 3 <= args.m <= 16:
        parser.error("local verifier supports 3 <= m <= 16")
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    trace_path = output.with_name(output.stem + "-trace.jsonl")
    binary = str(Path(args.binary).resolve())
    count = args.nodes
    identifiers = [index * (1 << args.m) // count for index in range(count)]
    config = {
        "M": str(args.m),
        "R": "4",
        "STABILIZE_INTERVAL_MS": "3000",
        "FIX_FINGERS_INTERVAL_MS": str(args.fix_ms),
        "CHECK_PRED_INTERVAL_MS": "3000",
        "RPC_TIMEOUT_MS": "250",
        "SWIM_INTERVAL_MS": "200",
        "SWIM_DIRECT_TIMEOUT_MS": "50",
        "SWIM_SUSPICION_TIMEOUT_MS": "1200",
        "SWIM_INDIRECT_HELPERS": "2",
        "SWIM_MAX_PIGGYBACK": "6",
        "SWIM_MAX_UPDATES": "128",
        "SWIM_MAX_MEMBERS": "128",
        "LOG_LEVEL": "info",
    }
    processes = []
    reservations = []
    result = {
        "variant": "chord_standard_swim",
        "run_at_utc": datetime.datetime.now(datetime.UTC).isoformat(),
        "config": config,
        "nodes": count,
        "binary_sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
        "query_start_delay_ms": args.observe_ms,
    }
    events = []
    killed_wall = None
    try:
        with (
            tempfile.TemporaryDirectory(prefix="gossipmesh-standard-") as directory,
            contextlib.ExitStack() as handles,
        ):
            try:
                for _ in range(count):
                    reservation = socket.socket()
                    reservation.bind(("127.0.0.1", 0))
                    reservations.append(reservation)
                ports = [reservation.getsockname()[1] for reservation in reservations]
                peers = [
                    {
                        "id": identifiers[index],
                        "port": port,
                        "addr": f"127.0.0.1:{port}",
                    }
                    for index, port in enumerate(ports)
                ]

                def start(index):
                    env = dict(os.environ, **config)
                    env.update(
                        PORT=str(ports[index]),
                        NODE_ID=str(identifiers[index]),
                        ADVERTISE_ADDR=peers[index]["addr"],
                        BOOTSTRAP_ADDR="" if index == 0 else peers[0]["addr"],
                        SWIM_STATE_FILE=str(
                            Path(directory) / f"node-{index}-incarnation.json"
                        ),
                    )
                    log = handles.enter_context(
                        open(Path(directory) / f"node-{index}.jsonl", "a")
                    )
                    return subprocess.Popen([binary], env=env, stdout=log, stderr=log)

                def converged(indices, expected_alive):
                    states = [
                        runner.get_json(
                            f"http://127.0.0.1:{ports[index]}/state", timeout=0.5
                        )
                        for index in indices
                    ]
                    if not all(states) or not runner.ring_converged(states):
                        return False
                    for index in indices:
                        members = runner.get_json(
                            f"http://127.0.0.1:{ports[index]}/membership", timeout=0.5
                        )
                        if not members:
                            return False
                        alive = {
                            int(member["peer"]["id"])
                            for member in members
                            if member["state"] == "ALIVE"
                        }
                        if alive != expected_alive or len(members) != count:
                            return False
                    return True

                for index, reservation in enumerate(reservations):
                    reservation.close()
                    processes.append(start(index))
                print(
                    "Waiting for full membership and oracle-checked Chord tables...",
                    flush=True,
                )
                wait_for(
                    lambda: converged(range(count), set(identifiers)),
                    "initial convergence",
                )
                keyspace = 1 << args.m
                rng = random.Random(42)
                keys = (
                    list(range(keyspace))
                    if args.m <= 8
                    else [rng.randrange(keyspace) for _ in range(256)] + identifiers
                )
                healthy_samples = lookup_batch(peers, keys)
                result["healthy"] = summarize(healthy_samples)
                result["healthy_samples"] = healthy_samples
                assert all(sample["correct"] for sample in healthy_samples), (
                    "healthy owner mismatch"
                )
                result["healthy_metrics"] = [
                    runner.get_json(f"http://127.0.0.1:{port}/metrics")
                    for port in ports
                ]
                victim_index = count // 2
                victim = peers[victim_index]
                result["victim"] = victim
                result["victim_initial_incarnation"] = result["healthy_metrics"][
                    victim_index
                ]["swim"]["self_incarnation"]
                survivor_indices = [
                    index for index in range(count) if index != victim_index
                ]
                survivors = [peers[index] for index in survivor_indices]
                before = [
                    runner.get_json(f"http://127.0.0.1:{ports[index]}/state")
                    for index in survivor_indices
                ]
                result["references_before_crash"] = {
                    state["id"]: {
                        "successor": state.get("successor")
                        and int(state["successor"]["id"]) == victim["id"],
                        "fingers": [
                            finger["index"]
                            for finger in state["fingers"]
                            if int(finger["id"]) == victim["id"]
                        ],
                    }
                    for state in before
                }
                assert (
                    sum(
                        len(refs["fingers"])
                        for refs in result["references_before_crash"].values()
                    )
                    >= 2
                )
                killed_at = time.monotonic()
                killed_wall = datetime.datetime.now(datetime.UTC).timestamp()
                processes[victim_index].kill()
                processes[victim_index].wait()
                if args.observe_ms:
                    time.sleep(args.observe_ms / 1000)
                print(
                    f"Killed peer {victim['id']}; sampling correctness during recovery...",
                    flush=True,
                )
                recovery_samples = []
                observations = {}
                stable = 0
                recovered_at = None
                probe_keys = sorted(
                    set(
                        identifiers
                        + [(identifier - 1) % keyspace for identifier in identifiers]
                        + [rng.randrange(keyspace) for _ in range(16)]
                    )
                )
                deadline = killed_at + 20
                while time.monotonic() < deadline:
                    batch = lookup_batch(survivors, probe_keys)
                    elapsed = time.monotonic() - killed_at
                    for sample in batch:
                        sample["observed_after_crash_sec"] = elapsed
                    recovery_samples.extend(batch)
                    stable = (
                        stable + 1
                        if sum(sample["correct"] for sample in batch) / len(batch)
                        >= 0.99
                        else 0
                    )
                    if stable >= 2 and recovered_at is None:
                        recovered_at = elapsed
                    for index in survivor_indices:
                        members = (
                            runner.get_json(
                                f"http://127.0.0.1:{ports[index]}/membership",
                                timeout=0.5,
                            )
                            or []
                        )
                        record = next(
                            (
                                member
                                for member in members
                                if int(member["peer"]["id"]) == victim["id"]
                            ),
                            None,
                        )
                        if record:
                            observations.setdefault(
                                str(identifiers[index]), {}
                            ).setdefault(record["state"], elapsed)
                    if recovered_at is not None and all(
                        "DEAD" in observations.get(str(identifiers[index]), {})
                        for index in survivor_indices
                    ):
                        break
                    time.sleep(0.05)
                result["recovery"] = {
                    "recovered": recovered_at is not None,
                    "time_sec": recovered_at,
                    "threshold": "at least 99% oracle-correct in two consecutive batches across all survivors",
                    "sampling": "ownership boundaries and 16 seeded random keys; samples do not prove all-key correctness",
                    "observations_sec": observations,
                    **summarize(recovery_samples),
                }
                result["recovery_samples"] = recovery_samples
                assert recovered_at is not None, "routing recovery timed out"
                assert all(
                    "DEAD" in observations.get(str(identifiers[index]), {})
                    for index in survivor_indices
                ), "death did not disseminate to every survivor"
                wait_for(
                    lambda: converged(
                        survivor_indices, {peer["id"] for peer in survivors}
                    ),
                    "post-crash convergence",
                )
                post_samples = lookup_batch(survivors, keys)
                result["post_crash"] = summarize(post_samples)
                assert all(sample["correct"] for sample in post_samples), (
                    "post-crash owner mismatch"
                )
                result["post_crash_metrics"] = [
                    runner.get_json(f"http://127.0.0.1:{ports[index]}/metrics")
                    for index in survivor_indices
                ]
                processes[victim_index] = start(victim_index)
                print(
                    "Restarted the same identity/address; waiting for rejoin...",
                    flush=True,
                )
                wait_for(
                    lambda: converged(range(count), set(identifiers)),
                    "restart convergence",
                )
                restart_samples = lookup_batch(peers, keys)
                result["restart"] = summarize(restart_samples)
                assert all(sample["correct"] for sample in restart_samples), (
                    "restart owner mismatch"
                )
                result["restart_metrics"] = [
                    runner.get_json(f"http://127.0.0.1:{port}/metrics")
                    for port in ports
                ]
                restart_view = runner.get_json(
                    f"http://127.0.0.1:{ports[0]}/membership"
                )
                result["restart_incarnation"] = next(
                    member["incarnation"]
                    for member in restart_view
                    if int(member["peer"]["id"]) == victim["id"]
                )
                assert result["restart_incarnation"] > 0
                result["verification_passed"] = True
            finally:
                for reservation in reservations:
                    reservation.close()
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
                log_errors = []
                for path in Path(directory).glob("node-*.jsonl"):
                    text = path.read_text()
                    if "DATA RACE" in text or "panic:" in text:
                        log_errors.append(text)
                    for line in text.splitlines():
                        try:
                            event = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        events.append(event)
                if log_errors:
                    raise AssertionError(
                        "race or panic in node logs: " + "\n".join(log_errors)
                    )
                if result.get("forced_shutdown"):
                    raise AssertionError(
                        "a surviving process did not shut down cleanly"
                    )
    finally:
        events.sort(key=lambda event: event.get("time", ""))
        victim_id = str(result.get("victim", {}).get("id", ""))
        selected = [
            event
            for event in events
            if event.get("target_id") == victim_id
            and event.get("type")
            in {
                "direct_timeout",
                "direct_error",
                "ping_req_issued",
                "indirect_timeout",
                "indirect_error",
                "indirect_cancelled",
                "peer_suspected",
                "peer_dead",
                "membership_event_handled",
                "references_identified",
                "finger_dirty",
                "finger_repaired",
                "successor_promoted",
                "successor_repair_validated",
                "successor_list_refreshed",
                "predecessor_cleared",
                "repair_completed",
                "repair_pending",
                "peer_rejoined",
            }
        ]
        result["trace_file"] = str(trace_path.relative_to(output.parent))
        result["trace_event_counts"] = {
            kind: sum(event["type"] == kind for event in selected)
            for kind in sorted({event["type"] for event in selected})
        }
        result["processes_exited"] = all(
            process.poll() is not None for process in processes
        )
        result["clean_shutdown_events"] = sum(
            event.get("type") == "node_stopped" for event in events
        )
        result["integration_trace_verified"] = (
            result["trace_event_counts"].get("peer_dead", 0) >= count - 1
            and result["trace_event_counts"].get("finger_repaired", 0) > 0
            and result["trace_event_counts"].get("membership_event_handled", 0)
            >= count - 1
        )
        result["verification_passed"] = bool(
            result.get("verification_passed")
            and result["integration_trace_verified"]
            and result["processes_exited"]
            and not result.get("forced_shutdown")
        )
        # Local processes share a clock; these observational latencies are not
        # distributed-clock guarantees or network-wide causality measurements.
        if "victim" in result and killed_wall is not None:

            def wall(event):
                return datetime.datetime.fromisoformat(event["time"]).timestamp()

            suspects = [
                wall(event) - killed_wall
                for event in selected
                if event["type"] == "peer_suspected" and wall(event) >= killed_wall
            ]
            deaths = [
                wall(event) - killed_wall
                for event in selected
                if event["type"] == "peer_dead"
                and event.get("details", {}).get("incarnation")
                == result["victim_initial_incarnation"]
                and wall(event) >= killed_wall
            ]
            result["swim_timing"] = {
                "first_suspicion_sec": min(suspects) if suspects else None,
                "first_confirmation_sec": min(deaths) if deaths else None,
                "confirmation_spread_sec": max(deaths) - min(deaths)
                if deaths
                else None,
            }
        with trace_path.open("w") as stream:
            for event in selected:
                stream.write(json.dumps(event) + "\n")
        output.write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: result[key]
                for key in [
                    "healthy",
                    "recovery",
                    "post_crash",
                    "restart",
                    "swim_timing",
                    "trace_event_counts",
                    "verification_passed",
                ]
            },
            indent=2,
        )
    )
    print(f"Saved {output} and {trace_path}")
    if not result.get("verification_passed"):
        raise AssertionError(
            "acceptance did not verify the complete SWIM-to-Chord repair trace"
        )


if __name__ == "__main__":
    main()
