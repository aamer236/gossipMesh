"""Reproducible Chord-only multi-seed lookup, convergence and recovery experiment."""

import argparse
import gc
import json
import math
import platform
import random
import statistics
import sys
import tracemalloc
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any

from chord import ChordError
from simulator import ChordSimulation


def distribution(values: list[float] | list[int]) -> dict[str, float | None]:
    if not values:
        return dict.fromkeys(("mean", "median", "p95", "p99", "min", "max"))
    ordered = sorted(values)
    return {
        "mean": statistics.mean(ordered),
        "median": statistics.median(ordered),
        "p95": ordered[math.ceil(len(ordered) * 0.95) - 1],
        "p99": ordered[math.ceil(len(ordered) * 0.99) - 1],
        "min": ordered[0],
        "max": ordered[-1],
    }


def sample_queries(
    simulation: ChordSimulation, rng: random.Random, count: int
) -> dict[str, Any]:
    live = simulation.live_nodes()
    oracle = simulation.oracle()
    samples = []
    hops: list[int] = []
    latencies: list[float] = []
    rpcs: list[int] = []
    successful_latencies: list[float] = []
    failures = incorrect = 0
    initial_calls = simulation.transport.calls.copy()
    for _ in range(count):
        source = rng.choice(live)
        key = rng.getrandbits(simulation.space.m)
        start = simulation.clock.now
        calls = simulation.transport.calls.copy()
        owner = hop_count = None
        error = None
        try:
            result = source.lookup_id(key)
            owner = result.owner.id
            hop_count = result.hops
            status = "correct" if owner == oracle.owner(key) else "incorrect_owner"
            if status == "incorrect_owner":
                incorrect += 1
            else:
                hops.append(result.hops)
                successful_latencies.append(simulation.clock.now - start)
        except ChordError as failure:
            status = "failed"
            failures += 1
            error = str(failure)
        elapsed = simulation.clock.now - start
        rpc_count = sum((simulation.transport.calls - calls).values())
        latencies.append(elapsed)
        rpcs.append(rpc_count)
        samples.append(
            {
                "source": source.peer.id,
                "key": key,
                "expected_owner": oracle.owner(key),
                "owner": owner,
                "status": status,
                "hops": hop_count,
                "latency": elapsed,
                "rpc_count": rpc_count,
                "error": error,
            }
        )
    return {
        "queries": count,
        "correct_lookups": len(hops),
        "failed_lookups": failures,
        "incorrect_owners": incorrect,
        "success_rate": len(hops) / count,
        "incorrect_owner_rate": incorrect / count,
        "failure_rate": failures / count,
        "hops_correct_lookups": distribution(hops),
        "latency_all_attempts": distribution(latencies),
        "latency_correct_lookups": distribution(successful_latencies),
        "rpcs_per_attempt": distribution(rpcs),
        "rpc_calls": dict(simulation.transport.calls - initial_calls),
        "samples": samples,
    }


def counters(simulation: ChordSimulation) -> dict[str, Any]:
    return {
        "rpc_calls": simulation.transport.calls.copy(),
        "events": simulation.events.copy(),
        "maintenance_calls": simulation.maintenance_calls,
        "dropped_requests": simulation.transport.dropped,
        "service_time": simulation.transport.service_time,
    }


def overhead(simulation: ChordSimulation, before: dict[str, Any]) -> dict[str, Any]:
    maintenance = simulation.maintenance_calls - before["maintenance_calls"]
    return {
        "rpc_calls": dict(simulation.transport.calls - before["rpc_calls"]),
        "event_counts": dict(simulation.events - before["events"]),
        "stabilization_attempts": maintenance,
        "finger_repair_attempts": maintenance,
        "predecessor_check_attempts": maintenance,
        "dropped_requests": simulation.transport.dropped - before["dropped_requests"],
        "rpc_service_time": simulation.transport.service_time - before["service_time"],
    }


def python_bytes(value: Any, seen: set[int]) -> int:
    if id(value) in seen:
        return 0
    seen.add(id(value))
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        size += sum(
            python_bytes(k, seen) + python_bytes(v, seen) for k, v in value.items()
        )
    elif isinstance(value, (list, tuple)):
        size += sum(python_bytes(v, seen) for v in value)
    return size


def routing_memory(simulation: ChordSimulation) -> dict[str, Any]:
    # Measure an exported materialized snapshot graph, not hidden node internals
    # or process RSS. Object sizes are Python/platform-specific observations.
    states = [asdict(node.state) for node in simulation.live_nodes()]
    pointers = [
        len(s["fingers"]) + len(s["successors"]) + int(s["predecessor"] is not None)
        for s in states
    ]
    sizes = [python_bytes(state, set()) for state in states]
    return {
        "scope": "materialized public routing snapshots; excludes process/core/transport heap",
        "snapshot_python_bytes_per_node": distribution(sizes),
        "snapshot_python_bytes_total": python_bytes(states, set()),
        "snapshot_json_bytes_total": len(json.dumps(states, sort_keys=True).encode()),
        "routing_pointer_slots_per_node": distribution(pointers),
    }


def failure_phase(
    simulation: ChordSimulation,
    rng: random.Random,
    failed: list[int],
    *,
    probes: int,
    max_rounds: int,
    period: float,
) -> dict[str, Any]:
    for identifier in failed:
        simulation.crash(identifier)
    initial = counters(simulation)
    start_time, start_round = simulation.clock.now, simulation.round
    trace = []
    probe_time = 0.0
    maintenance_rpcs: Counter[str] = Counter()
    for _ in range(max_rounds + 1):
        diagnostic = simulation.diagnostics()
        start_probe = simulation.clock.now
        observed = sample_queries(simulation, rng, probes)
        probe_time += simulation.clock.now - start_probe
        trace.append({"diagnostics": asdict(diagnostic), "lookups": observed})
        if diagnostic.converged or simulation.round - start_round >= max_rounds:
            break
        calls = simulation.transport.calls.copy()
        simulation.run_rounds(1, period=period)
        maintenance_rpcs.update(simulation.transport.calls - calls)
    converged = simulation.diagnostics().converged
    elapsed = simulation.clock.now - start_time
    return {
        "failed_nodes": failed,
        "converged": converged,
        "rounds_observed": simulation.round - start_round,
        "recovery_rounds": simulation.round - start_round if converged else None,
        "elapsed_including_probes": elapsed,
        "probe_service_time": probe_time,
        "recovery_time_excluding_probes": elapsed - probe_time if converged else None,
        "maintenance_rpc_calls": dict(maintenance_rpcs),
        "overhead_including_probes": overhead(simulation, initial),
        "trace": trace,
        "limitation": None
        if converged
        else "budget exhausted; local routes may be insufficient",
    }


def run_case(
    count: int,
    seed: int,
    *,
    m: int = 16,
    queries: int = 1000,
    latency: float = 1.0,
    timeout: float = 5.0,
    period: float = 1.0,
    failures: int = 2,
    successor_list_size: int = 4,
) -> dict[str, Any]:
    if type(count) is not int or not 1 <= count <= (1 << m):
        raise ValueError("node count must fit the identifier space")
    if type(queries) is not int or queries < 1:
        raise ValueError("queries must be positive")
    if type(failures) is not int or failures < 0:
        raise ValueError("failure count must be nonnegative")
    rng = random.Random(seed)
    simulation = ChordSimulation(
        m,
        seed=seed,
        latency=latency,
        timeout=timeout,
        successor_list_size=successor_list_size,
    )
    ids: set[int] = set()
    while len(ids) < count:
        ids.add(rng.getrandbits(m))
    join_order = sorted(ids)
    rng.shuffle(join_order)
    before = counters(simulation)
    for identifier in join_order:
        simulation.add_node(identifier)
    join = {"elapsed": simulation.clock.now, "overhead": overhead(simulation, before)}
    initial_diagnostic = asdict(simulation.diagnostics())
    before = counters(simulation)
    start = simulation.clock.now
    budget = 4 * count + 2 * m
    rounds = simulation.converge(max_rounds=budget, period=period)
    initialization = {
        "converged": simulation.diagnostics().converged,
        "rounds": rounds,
        "elapsed": simulation.clock.now - start,
        "overhead": overhead(simulation, before),
        "trace": [initial_diagnostic, *simulation.trace()],
    }
    healthy = sample_queries(simulation, rng, queries)
    memory = routing_memory(simulation)
    sorted_ids = sorted(ids)
    anchor = rng.randrange(count)
    failed = [sorted_ids[(anchor + i) % count] for i in range(min(failures, count - 1))]
    failure = (
        failure_phase(
            simulation,
            rng,
            failed,
            probes=min(queries, 16),
            max_rounds=budget,
            period=period,
        )
        if failed
        else None
    )
    restoration = None
    if failed:
        for identifier in failed:
            simulation.recover(identifier)
        before = counters(simulation)
        start = simulation.clock.now
        restored_rounds = simulation.converge(max_rounds=budget, period=period)
        restoration = {
            "converged": simulation.diagnostics().converged,
            "rounds": restored_rounds,
            "elapsed": simulation.clock.now - start,
            "overhead": overhead(simulation, before),
            "lookups": sample_queries(simulation, rng, min(queries, 100)),
        }
    return {
        "nodes": count,
        "seed": seed,
        "m": m,
        "queries": queries,
        "join_order": join_order,
        "join": join,
        "initial_convergence": initialization,
        "healthy": healthy,
        "memory": memory,
        "adjacent_failure": failure,
        "restoration": restoration,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=int, nargs="+", default=[16, 32, 64, 128])
    parser.add_argument("--seeds", type=int, nargs="+", default=[7, 42, 20261024])
    parser.add_argument("--m", type=int, default=16)
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--rpc-latency", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--period", type=float, default=1.0)
    parser.add_argument("--failures", type=int, default=2)
    parser.add_argument("--successor-list-size", type=int, default=4)
    parser.add_argument(
        "--trace-memory",
        action="store_true",
        help="also measure traced Python allocations for the entire case workload",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    configuration = vars(args).copy()
    configuration.pop("output")
    runs = []
    for count in args.sizes:
        for seed in args.seeds:
            if args.trace_memory:
                if tracemalloc.is_tracing():
                    parser.error(
                        "--trace-memory needs exclusive ownership of tracemalloc"
                    )
                gc.collect()
                tracemalloc.start()
            try:
                result = run_case(
                    count,
                    seed,
                    m=args.m,
                    queries=args.queries,
                    latency=args.rpc_latency,
                    timeout=args.timeout,
                    period=args.period,
                    failures=args.failures,
                    successor_list_size=args.successor_list_size,
                )
            finally:
                if args.trace_memory:
                    current, peak = tracemalloc.get_traced_memory()
                    tracemalloc.stop()
            if args.trace_memory:
                result["memory"]["python_allocations_workload"] = {
                    "current_bytes": current,
                    "peak_bytes": peak,
                    "scope": "all traced Python allocations during this case, including simulator, diagnostic history, lookup samples and exported snapshots; excludes native allocations and process RSS",
                }
            runs.append(result)
            if args.output:
                hops = result["healthy"]["hops_correct_lookups"]
                print(
                    f"nodes={count} seed={seed} success={result['healthy']['success_rate']:.3f} "
                    f"mean_hops={hops['mean']:.3f} p99={hops['p99']} "
                    f"join_rounds={result['initial_convergence']['rounds']}",
                    flush=True,
                )
    payload = {
        "schema_version": 1,
        "configuration": configuration,
        "python": platform.python_version(),
        "latency_unit": "virtual service units",
        "model": "serialized atomic RPCs; nearest-rank percentiles; no wall-clock sleeps",
        "runs": runs,
    }
    encoded = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded)
        print(f"Raw samples and traces: {args.output}")
    else:
        print(encoded, end="")
    if any(run["healthy"]["success_rate"] != 1 for run in runs):
        raise SystemExit("healthy-ring ownership check failed; inspect raw results")


if __name__ == "__main__":
    main()
