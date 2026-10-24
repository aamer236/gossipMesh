"""Local Chord state and the section 5 stabilization protocol.

No operation reads global membership. Calls are serialized by the host scheduler;
this synchronous implementation does not promise safety under concurrent threads.
"""

from collections.abc import Callable

from .identifiers import IdentifierSpace
from .model import (
    ChordError,
    Event,
    EventKind,
    Finger,
    InvalidState,
    LookupFailed,
    LookupResult,
    MaintenanceResult,
    Peer,
    PeerUnavailable,
    RouteStep,
    RoutingState,
    Transport,
)


class ChordNode:
    def __init__(
        self,
        peer: Peer,
        space: IdentifierSpace,
        transport: Transport,
        *,
        successor_list_size: int = 4,
        max_hops: int = 512,
        on_event: Callable[[Event], None] | None = None,
    ) -> None:
        space.validate(peer.id)
        if not isinstance(peer.endpoint, str) or not peer.endpoint:
            raise ValueError("endpoint must be a nonempty string")
        if type(successor_list_size) is not int or successor_list_size < 1:
            raise ValueError("successor_list_size must be positive")
        if type(max_hops) is not int or max_hops < 1:
            raise ValueError("max_hops must be positive")
        self._peer = peer
        self._space = space
        self._transport = transport
        self._list_size = successor_list_size
        self._max_hops = max_hops
        self._on_event = on_event
        self._active = False
        self._singleton = False
        self._predecessor: Peer | None = None
        self._successors: tuple[Peer, ...] = ()
        self._fingers: list[Peer | None] = [None] * space.m
        self._blocked: set[Peer] = set()
        self._hint_versions: dict[Peer, int] = {}
        self._next_finger = 0

    @property
    def peer(self) -> Peer:
        return self._peer

    @property
    def space(self) -> IdentifierSpace:
        return self._space

    @property
    def state(self) -> RoutingState:
        return RoutingState(
            self.peer,
            self._predecessor,
            self._successors,
            tuple(
                Finger(self.space.finger_start(self.peer.id, i), peer)
                for i, peer in enumerate(self._fingers)
            ),
            self._active,
        )

    def _emit(
        self,
        kind: EventKind,
        peer: Peer | None = None,
        previous: Peer | None = None,
        index: int | None = None,
        detail: str = "",
    ) -> None:
        if self._on_event is not None:
            self._on_event(Event(kind, self.peer, peer, previous, index, detail))

    def _require_active(self) -> None:
        if not self._active:
            raise InvalidState("node has not joined or has left the ring")

    def _validate_peer(self, peer: Peer) -> None:
        self.space.validate(peer.id)
        if not isinstance(peer.endpoint, str) or not peer.endpoint:
            raise ValueError("endpoint must be a nonempty string")
        known = [self.peer, self._predecessor, *self._successors, *self._fingers]
        for other in known:
            if other is not None and (
                (other.id == peer.id and other.endpoint != peer.endpoint)
                or (other.endpoint == peer.endpoint and other.id != peer.id)
            ):
                raise InvalidState("conflicting node ID/endpoint binding")

    def _reachable(self, peer: Peer) -> bool:
        return peer not in self._blocked and (
            self._active if peer == self.peer else self._transport.ping(self.peer, peer)
        )

    def _set_predecessor(self, peer: Peer | None) -> None:
        if peer is not None:
            self._validate_peer(peer)
        old = self._predecessor
        self._predecessor = peer
        if old != peer:
            self._emit(EventKind.PREDECESSOR_CHANGED, peer, old)

    def _set_successors(self, peers: list[Peer]) -> None:
        unique: dict[int, Peer] = {}
        for peer in peers:
            self._validate_peer(peer)
            if peer == self.peer and not self._singleton:
                continue
            if peer.id in unique and unique[peer.id] != peer:
                raise InvalidState("conflicting successor IDs")
            unique[peer.id] = peer
        ordered = sorted(
            unique.values(), key=lambda p: self.space.distance(self.peer.id, p.id)
        )
        old = self._successors[0] if self._successors else None
        self._successors = tuple(ordered[: self._list_size])
        new = self._successors[0] if self._successors else None
        self._fingers[0] = new
        if old != new:
            self._emit(EventKind.SUCCESSOR_CHANGED, new, old)

    def create(self) -> None:
        """Create a new singleton ring; register the endpoint before calling."""
        if self._active:
            raise InvalidState("node is already active")
        self._blocked.clear()
        self._hint_versions.clear()
        self._active = self._singleton = True
        self._next_finger = 0
        self._fingers = [self.peer] * self.space.m
        self._set_predecessor(self.peer)
        self._set_successors([self.peer])
        self._emit(EventKind.JOINED)

    def join(self, existing_node: Peer) -> None:
        """Lazy join: successor known immediately; other state converges later."""
        if self._active:
            raise InvalidState("node is already active")
        self._validate_peer(existing_node)
        if existing_node == self.peer:
            raise InvalidState("use create() to create a new ring")
        # Joining starts a fresh lifecycle. Old detector hints must not veto an
        # explicitly supplied, reachable bootstrap after a local leave.
        self._blocked.clear()
        self._hint_versions.clear()
        result = self._lookup(self.peer.id, existing_node, self._max_hops)
        if result.owner.id == self.peer.id:
            raise InvalidState("node ID already belongs to another node")
        self._singleton = False
        self._next_finger = 0
        self._fingers = [None] * self.space.m
        self._set_predecessor(None)
        self._set_successors([result.owner])
        self._active = True
        self._emit(EventKind.JOINED, result.owner)

    def leave(self) -> None:
        """Stop participating; neighbors recover through ordinary maintenance.

        No atomic neighbor splice or application data transfer is promised.
        """
        self._require_active()
        self._active = False
        self._emit(EventKind.LEFT)

    def closest_preceding_finger(self, identifier: int) -> Peer:
        self._require_active()
        self.space.validate(identifier)
        candidates = [p for p in self._fingers if p is not None]
        # Select by distance, since stale fingers need not be ordered by ID.
        candidates.sort(
            key=lambda p: self.space.distance(self.peer.id, p.id), reverse=True
        )
        for peer in candidates:
            if self.space.contains(
                peer.id, self.peer.id, identifier
            ) and self._reachable(peer):
                return peer
        return self.peer

    def route_step(self, identifier: int) -> RouteStep:
        """RPC handler; never resolves a route using transport registry state."""
        self._require_active()
        self.space.validate(identifier)
        successor = self.state.successor
        if successor is None or not self._reachable(successor):
            raise PeerUnavailable("immediate successor unavailable; run stabilization")
        if successor == self.peer and not self._singleton:
            raise LookupFailed("ring transition has no usable successor")
        if self.space.contains(
            identifier, self.peer.id, successor.id, right_closed=True
        ):
            return RouteStep(successor, self.peer)
        return RouteStep(successor, self.closest_preceding_finger(identifier))

    def _lookup(self, identifier: int, start: Peer, budget: int) -> LookupResult:
        visited: list[Peer] = []
        current = start
        try:
            for _ in range(budget):
                self._validate_peer(current)
                if current in visited or current in self._blocked:
                    raise LookupFailed("routing cycle or excluded peer")
                visited.append(current)
                step = (
                    self.route_step(identifier)
                    if current == self.peer
                    else self._transport.route(self.peer, current, identifier)
                )
                self._validate_peer(step.successor)
                if self.space.contains(
                    identifier, current.id, step.successor.id, right_closed=True
                ):
                    if step.successor in self._blocked or not self._reachable(
                        step.successor
                    ):
                        raise PeerUnavailable("responsible successor unavailable")
                    path = tuple(visited)
                    if path[-1] != step.successor:
                        path += (step.successor,)
                    return LookupResult(step.successor, current, path)
                next_peer = step.preceding
                self._validate_peer(next_peer)
                if next_peer == current:
                    next_peer = step.successor
                if not self.space.contains(next_peer.id, current.id, identifier):
                    raise LookupFailed("route makes no clockwise progress")
                current = next_peer
            raise LookupFailed(f"lookup exceeded {budget} routing steps")
        except (PeerUnavailable, InvalidState, LookupFailed) as error:
            raise LookupFailed(str(error), tuple(visited)) from error

    def lookup_id(
        self, identifier: int, *, max_hops: int | None = None
    ) -> LookupResult:
        self._require_active()
        self.space.validate(identifier)
        budget = self._max_hops if max_hops is None else max_hops
        if type(budget) is not int or budget < 1:
            raise ValueError("max_hops must be positive")
        try:
            result = self._lookup(identifier, self.peer, budget)
        except LookupFailed as error:
            self._emit(EventKind.LOOKUP_FAILED, detail=str(error))
            raise
        self._emit(EventKind.LOOKUP_SUCCEEDED, result.owner)
        return result

    def lookup(self, key: str | bytes) -> LookupResult:
        return self.lookup_id(self.space.key_id(key))

    def find_successor(self, identifier: int) -> Peer:
        return self.lookup_id(identifier).owner

    def find_predecessor(self, identifier: int) -> Peer:
        return self.lookup_id(identifier).predecessor

    def notify(self, candidate: Peer) -> None:
        """Accept only a reachable, closer predecessor; idempotent hints."""
        self._require_active()
        self._validate_peer(candidate)
        if candidate == self.peer or not self._reachable(candidate):
            return
        if (
            self._predecessor is None
            or not self._reachable(self._predecessor)
            or self.space.contains(candidate.id, self._predecessor.id, self.peer.id)
        ):
            self._singleton = False
            self._set_predecessor(candidate)

    def check_predecessor(self) -> None:
        self._require_active()
        if self._predecessor is not None and not self._reachable(self._predecessor):
            self._set_predecessor(None)

    def stabilize(self) -> None:
        """Discover closer successors, refresh the list, and notify successor."""
        self._require_active()
        successor = self.state.successor
        if successor == self.peer and not self._singleton:
            successor = self._predecessor
        if successor is None or not self._reachable(successor):
            # Finger fallback is an extension; these are local hints, not members
            # fetched from a registry. A skipped peer is rediscovered by stabilize.
            candidates = {
                p for p in (*self._successors, *self._fingers) if p is not None
            }
            candidates.discard(self.peer)
            successor = next(
                (
                    p
                    for p in sorted(
                        candidates,
                        key=lambda p: self.space.distance(self.peer.id, p.id),
                    )
                    if self._reachable(p)
                ),
                None,
            )
        if successor is None:
            raise PeerUnavailable("no known reachable successor; bootstrap required")
        if successor == self.peer:
            return  # Only a known singleton may own the full ring.
        state = self._transport.read_state(self.peer, successor)
        candidate = state.predecessor
        if (
            candidate is not None
            and self.space.contains(candidate.id, self.peer.id, successor.id)
            and self._reachable(candidate)
        ):
            successor = candidate
            state = self._transport.read_state(self.peer, successor)
        tails = [
            p
            for p in state.successors
            if p != self.peer
            and self.space.distance(self.peer.id, p.id)
            > self.space.distance(self.peer.id, successor.id)
            and self._reachable(p)
        ]
        self._set_successors([successor, *tails])
        self._transport.notify(self.peer, successor)

    def fix_fingers(self, index: int | None = None) -> None:
        """Repair one designated finger; omitted index rotates deterministically."""
        self._require_active()
        selected = self._next_finger if index is None else index
        start = self.space.finger_start(self.peer.id, selected)
        # Fairness across failed requests: retain the old entry on failure, but
        # do not let one unreachable region starve every other finger.
        self._next_finger = (selected + 1) % self.space.m
        owner = self.find_successor(start)
        if selected == 0:
            tails = [
                p
                for p in self._successors
                if self.space.distance(self.peer.id, p.id)
                > self.space.distance(self.peer.id, owner.id)
                and self._reachable(p)
            ]
            self._set_successors([owner, *tails])
        else:
            self._fingers[selected] = owner
        self._emit(EventKind.FINGER_REPAIRED, owner, index=selected)

    def maintain(self) -> MaintenanceResult:
        """One scheduler tick; expected failures are reported, not swallowed."""
        errors = []
        for operation in (self.check_predecessor, self.stabilize, self.fix_fingers):
            try:
                operation()
            except ChordError as error:
                errors.append(f"{operation.__name__}: {error}")
        return MaintenanceResult(tuple(errors))

    def set_peer_available(
        self, peer: Peer, available: bool, *, version: int | None = None
    ) -> None:
        """Local, reversible reachability override; no permanent-death inference.

        Versioned hints ignore older/equal versions, including delayed failures
        after recovery. Unversioned hints are authoritative local overrides.
        An available hint only permits probing again; it does not install routes.
        """
        self._require_active()
        self._validate_peer(peer)
        if peer == self.peer:
            raise ValueError("use leave() or the transport for local availability")
        if type(available) is not bool:
            raise ValueError("available must be a bool")
        if version is not None:
            if type(version) is not int or version < 0:
                raise ValueError("version must be a nonnegative integer")
            if version <= self._hint_versions.get(peer, -1):
                return
            self._hint_versions[peer] = version
        if available:
            if peer in self._blocked:
                self._blocked.remove(peer)
                self._emit(EventKind.PEER_AVAILABLE, peer)
        elif peer not in self._blocked:
            self._blocked.add(peer)
            self._emit(EventKind.PEER_UNAVAILABLE, peer)

    def repair_peer(self, peer: Peer) -> MaintenanceResult:
        """Repair only references to this peer, plus the successor backbone.

        Does not mark the peer dead. Call set_peer_available separately for an
        externally validated hint. Duplicate requests are safe.
        """
        self._require_active()
        self._validate_peer(peer)
        affected = [i for i, entry in enumerate(self._fingers) if entry == peer]
        self._emit(EventKind.REPAIR_REQUESTED, peer)
        errors = []
        for operation in (self.check_predecessor, self.stabilize):
            try:
                operation()
            except ChordError as error:
                errors.append(f"{operation.__name__}: {error}")
        for index in affected:
            try:
                self.fix_fingers(index)
            except ChordError as error:
                errors.append(f"finger {index}: {error}")
        return MaintenanceResult(tuple(errors))
