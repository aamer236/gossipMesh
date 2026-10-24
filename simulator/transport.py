"""Deterministic synchronous RPC service costs and request loss, no sockets."""

import random

from chord import ChordNode, IdentifierSpace, InMemoryTransport, Peer, PeerUnavailable

from .clock import VirtualClock, duration


class SimulatedTransport(InMemoryTransport):
    """Each RPC consumes latency units; a dropped request consumes timeout units.

    Costs include request/reply service time, not one-way wire delay. A request
    either reaches its handler atomically or is dropped before handler execution.
    Nested ping RPCs add their own costs. No concurrent in-flight delivery model.
    """

    def __init__(
        self,
        space: IdentifierSpace,
        clock: VirtualClock,
        *,
        seed: int = 0,
        latency: float = 1.0,
        timeout: float = 5.0,
        drop_rate: float = 0.0,
    ) -> None:
        super().__init__(space)
        self.clock = clock
        self.latency = duration(latency)
        self.timeout = duration(timeout)
        self._rng = random.Random(seed)
        self._latencies: dict[tuple[Peer, Peer], float] = {}
        self._drops: dict[tuple[Peer, Peer], int] = {}
        self.dropped = 0
        self.service_time = 0.0
        self.set_drop_rate(drop_rate)

    def set_drop_rate(self, rate: float) -> None:
        rate = duration(rate)
        if rate > 1:
            raise ValueError("drop rate must be in [0, 1]")
        self.drop_rate = rate

    def set_latency(self, source: Peer, target: Peer, latency: float) -> None:
        self._latencies[source, target] = duration(latency)

    def drop_next(self, source: Peer, target: Peer, count: int = 1) -> None:
        if type(count) is not int or count < 1:
            raise ValueError("drop count must be positive")
        pair = (source, target)
        self._drops[pair] = self._drops.get(pair, 0) + count

    def _deliver(self, source: Peer, target: Peer) -> ChordNode:
        pair = (source, target)
        pending = self._drops.get(pair, 0)
        lost = pending > 0
        if lost:
            self._drops[pair] = pending - 1
        elif self.drop_rate:
            lost = self._rng.random() < self.drop_rate
        cost = self.timeout if lost else self._latencies.get(pair, self.latency)
        self.clock.consume(cost)
        self.service_time += cost
        if lost:
            self.dropped += 1
            raise PeerUnavailable(
                f"simulated request dropped: {source.endpoint} → {target.endpoint}"
            )
        return super()._deliver(source, target)
