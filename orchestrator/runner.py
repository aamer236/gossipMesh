#!/usr/bin/env python3
"""
Orchestrator Runner for GossipMesh / Chord Benchmarking.
Automates Docker container lifecycle, fault injection, and telemetry collection.
"""

import argparse
import csv
import json
import math
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

COMPOSE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docker-compose.yml")
METRICS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "metrics")
NODE_PORTS = [8000, 8001, 8002, 8003, 8004, 8005, 8006, 8007]
NODE_NAMES = [f"baseline-node-{i}" for i in range(len(NODE_PORTS))]


def run_cmd(cmd, check=True):
    print(f"[Runner CMD] {cmd}")
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True)
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
    except Exception:
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


def wait_for_ring_stabilization(ports, wait_time=8):
    print(f"[*] Allowing ring stabilization ({wait_time}s)...")
    time.sleep(wait_time)


def get_live_nodes(ports):
    live = []
    for port in ports:
        state = get_json(f"http://127.0.0.1:{port}/state")
        if state:
            live.append({
                "port": port,
                "id": int(state["id"]),
                "addr": state["addr"],
                "succ": state.get("successor"),
                "pred": state.get("predecessor"),
            })
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
    two_m = 1 << 160
    latencies = []
    correct_count = 0
    total_count = 0

    import random
    rng = random.Random(42)

    for i in range(num_queries):
        query_key = f"benchmark-key-{i}-{rng.randint(0, 1000000)}"
        import hashlib
        key_id = int.from_bytes(hashlib.sha1(query_key.encode()).digest(), "big") % two_m
        expected_succ_id = oracle_successor(live_nodes, key_id)

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
    p50 = latencies[int(n * 0.50)] if n else 0
    p95 = latencies[min(int(n * 0.95), n - 1)] if n else 0
    p99 = latencies[min(int(n * 0.99), n - 1)] if n else 0

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
    run_cmd(f"docker stop {container_name}")


def restart_node(container_name):
    print(f"[*] [Fault Recovery] Restarting container {container_name}")
    run_cmd(f"docker start {container_name}")


def inject_gray_failure(container_name, delay="500ms", jitter="50ms"):
    print(f"[*] [Fault Injection] Injecting delay {delay}±{jitter} on {container_name}")
    run_cmd(f"docker exec {container_name} tc qdisc add dev eth0 root netem delay {delay} {jitter} distribution normal")


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


def measure_recovery_time(target_crashed_container, crashed_port, remaining_ports, max_seconds=20):
    print(f"[*] Measuring recovery time after crashing {target_crashed_container}...")
    inject_hard_crash(target_crashed_container)
    start_time = time.time()

    recovered_at = None
    time.sleep(0.5)  # give a moment for OS stop

    # Poll lookups until 5 consecutive lookups succeed
    for sec in range(max_seconds * 2):
        live = get_live_nodes(remaining_ports)
        if not live:
            time.sleep(0.5)
            continue

        consecutive_ok = 0
        for _ in range(5):
            initiator = live[0]
            resp = get_json(f"http://127.0.0.1:{initiator['port']}/lookup?key=recovery-probe-{sec}-{_}", timeout=1.0)
            if resp and resp.get("success"):
                consecutive_ok += 1

        if consecutive_ok == 5:
            recovered_at = time.time() - start_time
            print(f"[+] Ring recovered in {recovered_at:.2f} seconds!")
            break
        time.sleep(0.5)

    if recovered_at is None:
        recovered_at = max_seconds
        print(f"[-] Ring did not fully recover within {max_seconds} seconds.")

    return recovered_at


def main():
    parser = argparse.ArgumentParser(description="GossipMesh Benchmark Orchestrator")
    parser.add_argument("--skip-build", action="store_true", help="Skip docker-compose build")
    parser.add_argument("--down-on-exit", action="store_true", help="Stop containers when benchmark completes")
    parser.add_argument("--num-queries", type=int, default=50, help="Number of benchmark queries")
    args = parser.parse_args()

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
        wait_for_ring_stabilization(NODE_PORTS, wait_time=8)

        # 3. Healthy Phase Benchmark
        print("\n--- PHASE 1: Healthy Ring Benchmark ---")
        live = get_live_nodes(NODE_PORTS)
        print(f"[*] Detected {len(live)} live nodes in the Chord ring:")
        for n in live:
            succ_str = n['succ']['addr'] if n.get('succ') else 'none'
            print(f"    Node ID: {n['id']} | Addr: {n['addr']} | Successor: {succ_str}")

        healthy_results = test_lookups(NODE_PORTS, live, num_queries=args.num_queries)
        print(f"[+] Healthy Correctness: {healthy_results['correctness_pct']:.2f}%")
        print(f"[+] Healthy Latency p50: {healthy_results['latency_p50_ms']:.2f}ms, p95: {healthy_results['latency_p95_ms']:.2f}ms, p99: {healthy_results['latency_p99_ms']:.2f}ms")

        # 4. Fault Injection: Gray Failure (Latency Spike)
        print("\n--- PHASE 2: Gray Failure Injection (Netem 500ms delay) ---")
        inject_gray_failure("baseline-node-3", delay="300ms", jitter="30ms")
        gray_results = test_lookups(NODE_PORTS, live, num_queries=args.num_queries)
        print(f"[+] Gray Failure Correctness: {gray_results['correctness_pct']:.2f}%")
        print(f"[+] Gray Failure Latency p50: {gray_results['latency_p50_ms']:.2f}ms, p95: {gray_results['latency_p95_ms']:.2f}ms, p99: {gray_results['latency_p99_ms']:.2f}ms")
        clear_gray_failure("baseline-node-3")
        time.sleep(2)

        # 5. Fault Injection: Hard Crash & Recovery Time
        print("\n--- PHASE 3: Hard Crash & Recovery Time Measurement ---")
        crashed_name = "baseline-node-2"
        crashed_port = 8002
        remaining_ports = [p for p in NODE_PORTS if p != crashed_port]

        recovery_time_sec = measure_recovery_time(crashed_name, crashed_port, remaining_ports, max_seconds=20)
        time.sleep(3)  # Let stabilization settle

        post_crash_live = get_live_nodes(remaining_ports)
        post_crash_results = test_lookups(remaining_ports, post_crash_live, num_queries=args.num_queries)
        print(f"[+] Post-Crash Correctness: {post_crash_results['correctness_pct']:.2f}%")
        print(f"[+] Post-Crash Latency p50: {post_crash_results['latency_p50_ms']:.2f}ms, p95: {post_crash_results['latency_p95_ms']:.2f}ms, p99: {post_crash_results['latency_p99_ms']:.2f}ms")

        # Restart crashed node
        restart_node(crashed_name)
        time.sleep(4)

        # 6. Overall Telemetry & Overhead
        print("\n--- PHASE 4: Telemetry & Overhead Collection ---")
        telemetry = collect_cluster_metrics(NODE_PORTS)
        print(f"[+] Total RPC Messages Sent: {telemetry['total_msgs_sent']}")
        print(f"[+] Total Network Bytes Sent: {telemetry['total_bytes_sent']} bytes")
        print(f"[+] Average Node Memory Alloc: {telemetry['avg_alloc_bytes'] / 1024:.2f} KB")

        # Compile final results
        final_summary = {
            "case": "baseline_chord",
            "healthy": healthy_results,
            "gray_failure": gray_results,
            "recovery_time_sec": recovery_time_sec,
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
            writer.writerow(["Phase", "Correctness (%)", "Latency p50 (ms)", "Latency p95 (ms)", "Latency p99 (ms)", "Recovery Time (s)"])
            writer.writerow(["Healthy", f"{healthy_results['correctness_pct']:.2f}", f"{healthy_results['latency_p50_ms']:.2f}", f"{healthy_results['latency_p95_ms']:.2f}", f"{healthy_results['latency_p99_ms']:.2f}", "N/A"])
            writer.writerow(["Gray Failure", f"{gray_results['correctness_pct']:.2f}", f"{gray_results['latency_p50_ms']:.2f}", f"{gray_results['latency_p95_ms']:.2f}", f"{gray_results['latency_p99_ms']:.2f}", "N/A"])
            writer.writerow(["Post-Crash", f"{post_crash_results['correctness_pct']:.2f}", f"{post_crash_results['latency_p50_ms']:.2f}", f"{post_crash_results['latency_p95_ms']:.2f}", f"{post_crash_results['latency_p99_ms']:.2f}", f"{recovery_time_sec:.2f}"])
        print(f"[+] Saved summary CSV metrics to {csv_out}")

        print("\n==========================================================")
        print("          Benchmark Finished Successfully!                ")
        print("==========================================================")

    finally:
        if args.down_on_exit:
            print("[*] Cleaning up containers...")
            run_cmd(f"docker compose -f {COMPOSE_FILE} down", check=False)


if __name__ == "__main__":
    main()
