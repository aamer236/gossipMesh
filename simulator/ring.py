"""One-process Chord orchestration with external diagnostics and virtual events."""

from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any

from chord import ChordNode, Event, IdentifierSpace, InvalidState, Peer

from .clock import VirtualClock, duration
from .oracle import RingOracle
from .transport import SimulatedTransport


@dataclass(frozen=True)
class Diagnostics:
    round: int
    time: float
    online: int
    incorrect_fingers: int
    incorrect_successors: int
    incorrect_predecessors: int
    incorrect_lists: int
    maintenance_errors: int = 0

    @property
    def converged(self) -> bool:
        return self.online > 0 and not any(
            (
                self.incorrect_fingers,
                self.incorrect_successors,
                self.incorrect_predecessors,
                self.incorrect_lists,
            )
        )


class ChordSimulation:
    def __init__(
        self,
        m: int = 16,
        *,
        seed: int = 0,
        latency: float = 0.0,
        timeout: float = 5.0,
        drop_rate: float = 0.0,
        successor_list_size: int = 4,
        max_hops: int = 512,
    ) -> None:
        self.space = IdentifierSpace(m)
        self.clock = VirtualClock()
        self.transport = SimulatedTransport(
            self.space,
            self.clock,
            seed=seed,
            latency=latency,
            timeout=timeout,
            drop_rate=drop_rate,
        )
        self.nodes: dict[int, ChordNode] = {}
        self.offline: set[int] = set()
        self.round = 0
        self.list_size = successor_list_size
        self.max_hops = max_hops
        self.events: Counter[str] = Counter()
        self.maintenance_calls = 0
        self.history: list[Diagnostics] = []

    def _observe(self, event: Event) -> None:
        self.events[event.kind.value] += 1

    def add_node(self, identifier: int, bootstrap: int | None = None) -> ChordNode:
        self.space.validate(identifier)
        if identifier in self.nodes:
            raise InvalidState("node ID already registered; use recover or rejoin")
        live = self.live_nodes()
        if self.nodes and not live:
            raise InvalidState(
                "no bootstrap available; explicit new-ring decision required"
            )
        bootstrap_node = (
            self.nodes[bootstrap]
            if bootstrap is not None
            else (live[0] if live else None)
        )
        node = ChordNode(
            Peer(identifier, f"node-{identifier}"),
            self.space,
            self.transport,
            successor_list_size=self.list_size,
            max_hops=self.max_hops,
            on_event=self._observe,
        )
        # A failed lazy join does not leave an unusable transport registration.
        # Join only contacts other nodes; self becomes addressable on success.
        if bootstrap_node is None:
            node.create()
        else:
            node.join(bootstrap_node.peer)
        self.transport.register(node)
        self.nodes[identifier] = node
        return node

    def live_nodes(self) -> list[ChordNode]:
        return [
            node
            for identifier, node in self.nodes.items()
            if identifier not in self.offline and node.state.active
        ]

    def oracle(self) -> RingOracle:
        return RingOracle(node.peer.id for node in self.live_nodes())

    def crash(self, identifier: int) -> None:
        node = self.nodes[identifier]
        self.transport.set_available(node.peer, False)
        self.offline.add(identifier)

    def recover(self, identifier: int) -> None:
        node = self.nodes[identifier]
        if not node.state.active:
            raise InvalidState("withdrawn node needs rejoin(), not crash recovery")
        self.transport.set_available(node.peer, True)
        self.offline.discard(identifier)

    def leave(self, identifier: int) -> None:
        self.nodes[identifier].leave()

    def rejoin(self, identifier: int, bootstrap: int) -> None:
        node = self.nodes[identifier]
        if node.state.active:
            raise InvalidState("active crashed node needs recover(), not rejoin()")
        self.transport.set_available(node.peer, True)
        self.offline.discard(identifier)
        node.join(self.nodes[bootstrap].peer)

    def diagnostics(self, errors: int = 0) -> Diagnostics:
        live = self.live_nodes()
        oracle = self.oracle()
        fingers = successors = predecessors = lists = 0
        for node in live:
            state = node.state
            index = oracle.ids.index(node.peer.id)
            successor = oracle.ids[(index + 1) % len(live)]
            predecessor = oracle.ids[(index - 1) % len(live)]
            successors += int(
                state.successor is None or state.successor.id != successor
            )
            predecessors += int(
                state.predecessor is None or state.predecessor.id != predecessor
            )
            fingers += sum(
                f.peer is None or f.peer.id != oracle.owner(f.start)
                for f in state.fingers
            )
            count = min(self.list_size, max(1, len(live) - 1))
            expected = tuple(
                oracle.ids[(index + offset) % len(live)]
                for offset in range(1, count + 1)
            )
            lists += int(tuple(peer.id for peer in state.successors) != expected)
        return Diagnostics(
            self.round,
            self.clock.now,
            len(live),
            fingers,
            successors,
            predecessors,
            lists,
            errors,
        )

    def maintenance_round(self) -> None:
        self.round += 1
        errors = 0
        for node in self.live_nodes():
            report = node.maintain()
            errors += len(report.errors)
            self.maintenance_calls += 1
        self.history.append(self.diagnostics(errors))

    def run_rounds(self, count: int, *, period: float = 1.0) -> None:
        if type(count) is not int or count < 0:
            raise ValueError("round count must be nonnegative")
        period = duration(period)
        for _ in range(count):
            deadline = self.clock.now + period
            self.clock.call_at(deadline, self.maintenance_round)
            self.clock.run_until(deadline)

    def converge(self, *, max_rounds: int = 1000, period: float = 1.0) -> int:
        if type(max_rounds) is not int or max_rounds < 0:
            raise ValueError("max_rounds must be nonnegative")
        start = self.round
        if self.diagnostics().converged:
            return 0
        for _ in range(max_rounds):
            self.run_rounds(1, period=period)
            if self.history[-1].converged:
                return self.round - start
        raise RuntimeError(
            f"ring did not converge within {max_rounds} maintenance rounds"
        )

    def trace(self) -> list[dict[str, Any]]:
        return [asdict(item) for item in self.history]
