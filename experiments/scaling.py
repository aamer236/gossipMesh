"""Seeded, failure-free Chord experiment; all global oracles live here.

Run: python3 -m experiments.scaling --sizes 16 64 128 --queries 1000
This measures overlay hops, not physical network latency or message complexity.
"""

import argparse
import json
import math
import random
import statistics
from bisect import bisect_left

from chord import ChordNode, IdentifierSpace, InMemoryTransport, LookupFailed, Peer


def run_experiment(
    count: int, *, m: int = 16, queries: int = 1000, seed: int = 20261024
) -> dict[str, int | float]:
    space = IdentifierSpace(m)
    if type(count) is not int or not 1 <= count <= space.size:
        raise ValueError("node count must fit the identifier space")
    if type(queries) is not int or queries < 1:
        raise ValueError("queries must be positive")
    rng = random.Random(seed)
    # Sampling range is bounded by node count, not ring size. This also works
    # for 160/256-bit IDs where len(range(2**m)) exceeds Python's sequence limit.
    identifiers: set[int] = set()
    while len(identifiers) < count:
        identifiers.add(rng.getrandbits(m))
    join_order = sorted(identifiers)
    rng.shuffle(join_order)
    transport = InMemoryTransport(space)
    nodes = []
    for identifier in join_order:
        node = ChordNode(Peer(identifier, f"node-{identifier}"), space, transport)
        transport.register(node)
        if nodes:
            node.join(nodes[0].peer)
        else:
            node.create()
        nodes.append(node)
    ordered = sorted(nodes, key=lambda n: n.peer.id)
    ordered_ids = [n.peer.id for n in ordered]

    def owner(key: int) -> Peer:
        return ordered[bisect_left(ordered_ids, key) % count].peer

    def converged() -> bool:
        for i, node in enumerate(ordered):
            state = node.state
            if (
                state.successor != ordered[(i + 1) % count].peer
                or state.predecessor != ordered[(i - 1) % count].peer
                or any(f.peer != owner(f.start) for f in state.fingers)
            ):
                return False
        return True

    ticks = 0
    while not converged():
        if ticks >= 4 * count + 2 * m:
            raise RuntimeError("maintenance did not converge within experiment budget")
        for node in nodes:
            node.maintain()
        ticks += 1
    transport.calls.clear()
    hops = []
    incorrect = failures = 0
    for _ in range(queries):
        node = rng.choice(nodes)
        key = rng.getrandbits(m)
        try:
            result = node.lookup_id(key)
        except LookupFailed:
            failures += 1
            continue
        if result.owner != owner(key):
            incorrect += 1
        else:
            hops.append(result.hops)
    sorted_hops = sorted(hops)
    return {
        "nodes": count,
        "m": m,
        "seed": seed,
        "queries": queries,
        "convergence_ticks": ticks,
        "stabilization_operations": ticks * count,
        "finger_repair_attempts": ticks * count,
        "finger_correctness": 1.0,
        "success_rate": len(hops) / queries,
        "failed_lookups": failures,
        "incorrect_owners": incorrect,
        "mean_hops": statistics.mean(hops) if hops else 0.0,
        "p95_hops": sorted_hops[math.ceil(0.95 * len(hops)) - 1] if hops else 0,
        "route_rpcs": transport.calls["route"],
        "ping_rpcs": transport.calls["ping"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", type=int, default=[16, 64, 128])
    parser.add_argument("--m", type=int, default=16)
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=20261024)
    args = parser.parse_args()
    print(
        json.dumps(
            [
                run_experiment(size, m=args.m, queries=args.queries, seed=args.seed)
                for size in args.sizes
            ],
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
