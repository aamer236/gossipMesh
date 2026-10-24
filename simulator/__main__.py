"""Run a deterministic join → lookup → crash → repair → recover demonstration."""

import json
from dataclasses import asdict

from chord import LookupFailed

from .ring import ChordSimulation


def main() -> None:
    simulation = ChordSimulation(m=8, latency=1)
    for identifier in range(0, 256, 8):
        simulation.add_node(identifier)
    join_rounds = simulation.converge()
    source = simulation.nodes[0]
    route = source.lookup_id(123)
    simulation.crash(8)
    try:
        source.lookup_id(8)
    except LookupFailed as error:
        before_repair = str(error)
    else:
        raise AssertionError("expected stale immediate successor to cause failure")
    recovery_rounds = simulation.converge()
    owner_after_crash = source.find_successor(8).id
    simulation.recover(8)
    restoration_rounds = simulation.converge()
    print(
        json.dumps(
            {
                "model": "one process; virtual time; no sockets or sleeps",
                "nodes": 32,
                "join_rounds": join_rounds,
                "lookup_123": {
                    "owner": route.owner.id,
                    "path": [p.id for p in route.path],
                    "hops": route.hops,
                },
                "lookup_8_before_repair": before_repair,
                "recovery_rounds": recovery_rounds,
                "owner_8_after_crash": owner_after_crash,
                "restoration_rounds": restoration_rounds,
                "owner_8_after_restoration": source.find_successor(8).id,
                "final_diagnostics": asdict(simulation.diagnostics()),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
