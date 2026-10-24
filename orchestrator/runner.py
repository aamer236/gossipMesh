#!/usr/bin/env python3
"""
Orchestrator Runner for GossipMesh / Chord Benchmarking.
Automates Docker container lifecycle, fault injection, and telemetry collection.
"""

import argparse
import concurrent.futures
import csv
import http.client
import json
import math
import os
import random
import subprocess
import time
import urllib.error
import urllib.request

COMPOSE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "docker-compose.yml"
)
METRICS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "metrics"
)
NODE_PORTS = [8000, 8001, 8002, 8003, 8004, 8005, 8006, 8007]
NODE_NAMES = [f"baseline-node-{i}" for i in range(len(NODE_PORTS))]


def run_cmd(cmd, check=True):
    print(f"[Runner CMD] {cmd}")
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True, check=False)
    if check and res.returncode != 0:
        print(f"Error running command: {cmd}\nStderr: {res.stderr}")
        raise RuntimeError(f"Command failed: {cmd}")
    return res


def get_json(url, timeout=3.0):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Orchestrator"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status == 200:
                return json.loads(resp.read().decode())
    except (OSError, ValueError, http.client.HTTPException):
        return None
    return None


def wait_for_cluster(ports, max_retries=30):
    print(f"[*] Waiting for cluster ({len(ports)} nodes) to be responsive...")
    for attempt in range(1, max_retries + 1):
        responsive = 0
        for port in ports:
            health = get_json(f"http://127.0.0.1:{port}/health")
            if health and health.get("status") == "healthy":
                responsive += 1
        if responsive == len(ports):
            print(f"[+] All {responsive} nodes are healthy and responsive.")
            return True
        time.sleep(1)
    raise TimeoutError(f"Cluster did not become ready within {max_retries} seconds.")


def ring_converged(states):
    """External oracle only: never supplies routes to a participant."""
    if not states:
        return False
    states = sorted(states, key=lambda state: int(state["id"]))
    ids = [int(state["id"]) for state in states]
    addrs = [state["addr"] for state in states]
    m = states[0]["m"]
    if len(set(ids)) != len(ids) or len(set(addrs)) != len(addrs):
        return False
    if any(state["m"] != m or not 0 <= int(state["id"]) < 1 << m for state in states):
        return False

    def matches(ref, target):
        return (
            ref is not None
            and int(ref["id"]) == int(target["id"])
            and ref["addr"] == target["addr"]
        )

    for i, state in enumerate(states):
        if not matches(state.get("successor"), states[(i + 1) % len(states)]):
            return False
        if len(states) > 1 and not matches(state.get("predecessor"), states[i - 1]):
            return False
        fingers = {finger["index"]: finger for finger in state["fingers"]}
        if set(fingers) != set(range(m)):
            return False
        for index, finger in fingers.items():
            start = (int(state["id"]) + (1 << index)) % (1 << m)
            owner = next(
                (other for other in states if int(other["id"]) >= start), states[0]
            )
            if not matches(finger, owner):
                return False
        count = min(state["r"], len(states) - 1) if len(states) > 1 else 1
        successors = state["successor_list"]
        if len(successors) != count:
            return False
        if any(
            not matches(ref, states[(i + offset + 1) % len(states)])
            for offset, ref in enumerate(successors)
        ):
            return False
    return True


def wait_for_ring_stabilization(ports, max_seconds=300, poll_interval=0.25):
    print("[*] Waiting for correct neighbors, successor lists and all fingers...")
    deadline = time.monotonic() + max_seconds
    consecutive = 0
    while time.monotonic() < deadline:
        states = [
            get_json(
                f"http://127.0.0.1:{port}/state",
                timeout=min(1.0, max(0.01, deadline - time.monotonic())),
            )
            for port in ports
        ]
        if all(states) and ring_converged(states):
            consecutive += 1
            if consecutive >= 2:
                print("[+] Routing tables converged.")
                return True
        else:
            consecutive = 0
        time.sleep(min(poll_interval, max(0, deadline - time.monotonic())))
    raise TimeoutError("Ring did not converge before the benchmark deadline")


def get_live_nodes(ports, timeout=3.0, deadline=None):
    live = []
    for port in ports:
        remaining = (
            timeout if deadline is None else min(timeout, deadline - time.monotonic())
        )
        if remaining <= 0:
            break
        state = get_json(f"http://127.0.0.1:{port}/state", timeout=remaining)
        if state:
            live.append(
                {
                    "port": port,
                    "id": int(state["id"]),
                    "addr": state["addr"],
                    "succ": state.get("successor"),
                    "pred": state.get("predecessor"),
                    "m": state["m"],
                }
            )
    live.sort(key=lambda n: n["id"])
    return live


def oracle_successor(live_nodes, key_id, m=160):
    """Calculates ground-truth successor on active ring."""
    if not live_nodes:
        return None
    two_m = 1 << m
    k = key_id % two_m
    # Find the first node whose ID >= k
    for n in live_nodes:
        if n["id"] >= k:
            return n["id"]
    # Wrap-around to smallest ID
    return live_nodes[0]["id"]


def test_lookups(ports, live_nodes, num_queries=50):
    """Evaluates lookup correctness and latency."""
    if not live_nodes:
        raise RuntimeError("No live nodes available for benchmark")
    m = live_nodes[0]["m"]
    two_m = 1 << m
    latencies = []
    correct_count = 0
    total_count = 0

    import random

    rng = random.Random(42)

    for i in range(num_queries):
        query_key = f"benchmark-key-{i}-{rng.randint(0, 1000000)}"
        import hashlib

        key_id = (
            int.from_bytes(hashlib.sha1(query_key.encode()).digest(), "big") % two_m
        )
        expected_succ_id = oracle_successor(live_nodes, key_id, m)

        # Pick random live port to initiate lookup
        initiator = rng.choice(live_nodes)
        url = f"http://127.0.0.1:{initiator['port']}/lookup?key={query_key}"

        start = time.perf_counter()
        resp = get_json(url, timeout=3.0)
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        total_count += 1
        if resp and resp.get("success"):
            latencies.append(elapsed_ms)
            succ = resp.get("successor")
            if succ and int(succ.get("id", -1)) == expected_succ_id:
                correct_count += 1
        else:
            latencies.append(elapsed_ms)

    latencies.sort()
    n = len(latencies)
    p50 = latencies[max(0, math.ceil(n * 0.50) - 1)] if n else 0
    p95 = latencies[max(0, math.ceil(n * 0.95) - 1)] if n else 0
    p99 = latencies[max(0, math.ceil(n * 0.99) - 1)] if n else 0

    correctness_pct = (correct_count / total_count * 100.0) if total_count else 0
    return {
        "queries": total_count,
        "correct": correct_count,
        "correctness_pct": correctness_pct,
        "latency_p50_ms": p50,
        "latency_p95_ms": p95,
        "latency_p99_ms": p99,
    }


def inject_hard_crash(container_name):
    print(f"[*] [Fault Injection] Hard crash on {container_name}")
    run_cmd(f"docker kill --signal=KILL {container_name}")


def restart_node(container_name):
    print(f"[*] [Fault Recovery] Restarting container {container_name}")
    run_cmd(f"docker start {container_name}")


def inject_gray_failure(container_name, delay="500ms", jitter="50ms"):
    print(f"[*] [Fault Injection] Injecting delay {delay}±{jitter} on {container_name}")
    run_cmd(
        f"docker exec {container_name} tc qdisc add dev eth0 root netem delay {delay} {jitter} distribution normal"
    )


def clear_gray_failure(container_name):
    print(f"[*] Clearing netem delay on {container_name}")
    run_cmd(f"docker exec {container_name} tc qdisc del dev eth0 root", check=False)


def collect_cluster_metrics(ports):
    all_metrics = []
    total_msgs_sent = 0
    total_bytes_sent = 0
    total_alloc_bytes = 0

    for port in ports:
        m = get_json(f"http://127.0.0.1:{port}/metrics")
        if m:
            net = m.get("network", {})
            st = m.get("storage", {})
            total_msgs_sent += net.get("msgs_sent", 0)
            total_bytes_sent += net.get("bytes_sent", 0)
            total_alloc_bytes += st.get("alloc_bytes", 0)
            all_metrics.append({"port": port, "metrics": m})

    return {
        "node_count": len(all_metrics),
        "total_msgs_sent": total_msgs_sent,
        "total_bytes_sent": total_bytes_sent,
        "avg_alloc_bytes": (total_alloc_bytes / len(all_metrics)) if all_metrics else 0,
        "raw": all_metrics,
    }


def measure_recovery_time(
    target_crashed_container, crashed_port, remaining_ports, max_seconds=20
):
    """Require >=99% oracle-correct probes across every survivor, twice.

    This is an operational sample, not proof that 99% of all possible keys work.
    Null recovery time explicitly represents a timeout.
    """
    inject_hard_crash(target_crashed_container)
    started = time.monotonic()
    deadline = started + max_seconds
    consecutive = 0
    last_pct = 0.0
    last_queries = 0
    while time.monotonic() < deadline:
        live = get_live_nodes(remaining_ports, timeout=0.5, deadline=deadline)
        if len(live) != len(remaining_ports):
            consecutive = 0
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))
            continue
        m = live[0]["m"]
        rng = random.Random(42)
        # Include each ownership boundary, its preceding key, and random keys.
        keys = [
            key for node in live for key in (node["id"], (node["id"] - 1) % (1 << m))
        ]
        keys += [rng.randrange(1 << m) for _ in range(16)]
        probes = [(node, key) for node in live for key in keys]

        def check(probe, live=live, m=m):
            node, key = probe
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            reply = get_json(
                f"http://127.0.0.1:{node['port']}/lookup?id={key}",
                timeout=min(0.5, remaining),
            )
            expected_id = oracle_successor(live, key, m)
            expected = next(other for other in live if other["id"] == expected_id)
            owner = reply.get("successor") if reply else None
            return bool(
                reply
                and reply.get("success")
                and owner
                and int(owner.get("id", -1)) == expected_id
                and owner.get("addr") == expected["addr"]
            )

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(16, len(probes))
        ) as pool:
            correct = sum(pool.map(check, probes))
        last_queries = len(probes)
        last_pct = correct / last_queries * 100
        consecutive = consecutive + 1 if last_pct >= 99 else 0
        if consecutive >= 2 and time.monotonic() <= deadline:
            elapsed = time.monotonic() - started
            print(f"[+] Oracle-verified recovery in {elapsed:.2f} seconds")
            return {
                "recovered": True,
                "time_sec": elapsed,
                "correctness_pct": last_pct,
                "queries": last_queries,
            }
        time.sleep(min(0.25, max(0, deadline - time.monotonic())))
    print(f"[-] Recovery not verified within {max_seconds} seconds")
    return {
        "recovered": False,
        "time_sec": None,
        "correctness_pct": last_pct,
        "queries": last_queries,
    }


def main():
    parser = argparse.ArgumentParser(description="GossipMesh Benchmark Orchestrator")
    parser.add_argument(
        "--skip-build", action="store_true", help="Skip docker-compose build"
    )
    parser.add_argument(
        "--down-on-exit",
        action="store_true",
        help="Stop containers when benchmark completes",
    )
    parser.add_argument(
        "--num-queries", type=int, default=50, help="Number of benchmark queries"
    )
    args = parser.parse_args()
    if args.num_queries < 1:
        parser.error("--num-queries must be positive")

    os.makedirs(METRICS_DIR, exist_ok=True)
    print("==========================================================")
    print("      GossipMesh Case 1 (Baseline Chord) Benchmark        ")
    print("==========================================================")

    # 1. Start containers
    build_flag = "" if args.skip_build else "--build"
    print("[*] Launching cluster via docker compose...")
    run_cmd(f"docker compose -f {COMPOSE_FILE} up -d {build_flag}")

    try:
        # 2. Wait for cluster and ring convergence
        wait_for_cluster(NODE_PORTS)
        wait_for_ring_stabilization(NODE_PORTS)

        # 3. Healthy Phase Benchmark
        print("\n--- PHASE 1: Healthy Ring Benchmark ---")
        live = get_live_nodes(NODE_PORTS)
        print(f"[*] Detected {len(live)} live nodes in the Chord ring:")
        for n in live:
            succ_str = n["succ"]["addr"] if n.get("succ") else "none"
            print(f"    Node ID: {n['id']} | Addr: {n['addr']} | Successor: {succ_str}")

        healthy_results = test_lookups(NODE_PORTS, live, num_queries=args.num_queries)
        print(f"[+] Healthy Correctness: {healthy_results['correctness_pct']:.2f}%")
        print(
            f"[+] Healthy Latency p50: {healthy_results['latency_p50_ms']:.2f}ms, p95: {healthy_results['latency_p95_ms']:.2f}ms, p99: {healthy_results['latency_p99_ms']:.2f}ms"
        )

        # 4. Fault Injection: Gray Failure (Latency Spike)
        print("\n--- PHASE 2: Gray Failure Injection (Netem 500ms delay) ---")
        inject_gray_failure("baseline-node-3", delay="300ms", jitter="30ms")
        gray_results = test_lookups(NODE_PORTS, live, num_queries=args.num_queries)
        print(f"[+] Gray Failure Correctness: {gray_results['correctness_pct']:.2f}%")
        print(
            f"[+] Gray Failure Latency p50: {gray_results['latency_p50_ms']:.2f}ms, p95: {gray_results['latency_p95_ms']:.2f}ms, p99: {gray_results['latency_p99_ms']:.2f}ms"
        )
        clear_gray_failure("baseline-node-3")
        wait_for_ring_stabilization(NODE_PORTS)

        # 5. Fault Injection: Hard Crash & Recovery Time
        print("\n--- PHASE 3: Hard Crash & Recovery Time Measurement ---")
        crashed_name = "baseline-node-2"
        crashed_port = 8002
        remaining_ports = [p for p in NODE_PORTS if p != crashed_port]

        recovery = measure_recovery_time(
            crashed_name, crashed_port, remaining_ports, max_seconds=20
        )
        recovery_time_sec = recovery["time_sec"]
        wait_for_ring_stabilization(remaining_ports)

        post_crash_live = get_live_nodes(remaining_ports)
        post_crash_results = test_lookups(
            remaining_ports, post_crash_live, num_queries=args.num_queries
        )
        print(
            f"[+] Post-Crash Correctness: {post_crash_results['correctness_pct']:.2f}%"
        )
        print(
            f"[+] Post-Crash Latency p50: {post_crash_results['latency_p50_ms']:.2f}ms, p95: {post_crash_results['latency_p95_ms']:.2f}ms, p99: {post_crash_results['latency_p99_ms']:.2f}ms"
        )

        # Restart crashed node
        restart_node(crashed_name)
        wait_for_cluster(NODE_PORTS)
        wait_for_ring_stabilization(NODE_PORTS)

        # 6. Overall Telemetry & Overhead
        print("\n--- PHASE 4: Telemetry & Overhead Collection ---")
        telemetry = collect_cluster_metrics(NODE_PORTS)
        print(f"[+] Total RPC Messages Sent: {telemetry['total_msgs_sent']}")
        print(f"[+] Total Network Bytes Sent: {telemetry['total_bytes_sent']} bytes")
        print(
            f"[+] Average Node Memory Alloc: {telemetry['avg_alloc_bytes'] / 1024:.2f} KB"
        )

        # Compile final results
        final_summary = {
            "case": "baseline_chord",
            "verification_passed": healthy_results["correctness_pct"] == 100
            and post_crash_results["correctness_pct"] == 100
            and recovery["recovered"],
            "healthy": healthy_results,
            "gray_failure": gray_results,
            "recovery_time_sec": recovery_time_sec,
            "recovery": recovery,
            "post_crash": post_crash_results,
            "telemetry": telemetry,
        }

        json_out = os.path.join(METRICS_DIR, "baseline_benchmark_results.json")
        csv_out = os.path.join(METRICS_DIR, "baseline_benchmark_results.csv")

        with open(json_out, "w") as f:
            json.dump(final_summary, f, indent=2)
        print(f"\n[+] Saved detailed JSON metrics to {json_out}")

        with open(csv_out, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "Phase",
                    "Correctness (%)",
                    "Latency p50 (ms)",
                    "Latency p95 (ms)",
                    "Latency p99 (ms)",
                    "Recovery Time (s)",
                ]
            )
            writer.writerow(
                [
                    "Healthy",
                    f"{healthy_results['correctness_pct']:.2f}",
                    f"{healthy_results['latency_p50_ms']:.2f}",
                    f"{healthy_results['latency_p95_ms']:.2f}",
                    f"{healthy_results['latency_p99_ms']:.2f}",
                    "N/A",
                ]
            )
            writer.writerow(
                [
                    "Gray Failure",
                    f"{gray_results['correctness_pct']:.2f}",
                    f"{gray_results['latency_p50_ms']:.2f}",
                    f"{gray_results['latency_p95_ms']:.2f}",
                    f"{gray_results['latency_p99_ms']:.2f}",
                    "N/A",
                ]
            )
            writer.writerow(
                [
                    "Post-Crash",
                    f"{post_crash_results['correctness_pct']:.2f}",
                    f"{post_crash_results['latency_p50_ms']:.2f}",
                    f"{post_crash_results['latency_p95_ms']:.2f}",
                    f"{post_crash_results['latency_p99_ms']:.2f}",
                    f"{recovery_time_sec:.2f}"
                    if recovery_time_sec is not None
                    else "TIMEOUT",
                ]
            )
        print(f"[+] Saved summary CSV metrics to {csv_out}")

        print("\n==========================================================")
        print("          Benchmark Run Completed                ")
        print("==========================================================")

    finally:
        if args.down_on_exit:
            print("[*] Cleaning up containers...")
            run_cmd(f"docker compose -f {COMPOSE_FILE} down", check=False)


if __name__ == "__main__":
    main()
