"""Deterministic synchronous message delivery, separate from Chord routing."""

from collections import Counter

from .identifiers import IdentifierSpace
from .model import InvalidState, Peer, PeerUnavailable, RouteStep, RoutingState
from .node import ChordNode


class InMemoryTransport:
    """Endpoint registry for delivery ONLY; no sorting or ownership oracle.

    Offline endpoints and directed blocked links model crashes/partitions. RPCs
    complete immediately or fail. A network adapter must implement timeouts.
    """

    def __init__(self, space: IdentifierSpace) -> None:
        self.space = space
        self._nodes: dict[str, ChordNode] = {}
        self._offline: set[Peer] = set()
        self._blocked_links: set[tuple[Peer, Peer]] = set()
        self.calls: Counter[str] = Counter()

    def register(self, node: ChordNode) -> None:
        if node.space != self.space:
            raise ValueError("identifier spaces must match")
        if any(
            existing.peer.id == node.peer.id
            or existing.peer.endpoint == node.peer.endpoint
            for existing in self._nodes.values()
        ):
            raise InvalidState("duplicate node ID or endpoint")
        self._nodes[node.peer.endpoint] = node

    def set_available(self, peer: Peer, available: bool) -> None:
        """Crash/recover the endpoint without altering its saved Chord state."""
        if peer.endpoint not in self._nodes or self._nodes[peer.endpoint].peer != peer:
            raise ValueError("unknown peer")
        if type(available) is not bool:
            raise ValueError("available must be a bool")
        if available:
            self._offline.discard(peer)
        else:
            self._offline.add(peer)

    def set_link(self, source: Peer, target: Peer, available: bool) -> None:
        if type(available) is not bool:
            raise ValueError("available must be a bool")
        if available:
            self._blocked_links.discard((source, target))
        else:
            self._blocked_links.add((source, target))

    def _deliver(self, source: Peer, target: Peer) -> ChordNode:
        node = self._nodes.get(target.endpoint)
        if (
            source in self._offline
            or target in self._offline
            or (source, target) in self._blocked_links
            or node is None
            or node.peer != target
            or not node.state.active
        ):
            raise PeerUnavailable(f"unreachable endpoint: {target.endpoint}")
        return node

    def ping(self, source: Peer, target: Peer) -> bool:
        self.calls["ping"] += 1
        try:
            self._deliver(source, target)
        except PeerUnavailable:
            return False
        return True

    def read_state(self, source: Peer, target: Peer) -> RoutingState:
        self.calls["read_state"] += 1
        return self._deliver(source, target).state

    def route(self, source: Peer, target: Peer, identifier: int) -> RouteStep:
        self.calls["route"] += 1
        return self._deliver(source, target).route_step(identifier)

    def notify(self, source: Peer, target: Peer) -> None:
        self.calls["notify"] += 1
        self._deliver(source, target).notify(source)
