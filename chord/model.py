"""Immutable messages, transport contract, and public outcomes."""

from dataclasses import dataclass
from enum import Enum
from typing import Protocol


class ChordError(Exception):
    """Expected routing/lifecycle failure."""


class PeerUnavailable(ChordError):
    """An endpoint cannot currently answer a request."""


class InvalidState(ChordError):
    """Operation incompatible with the node lifecycle or peer identity."""


class LookupFailed(ChordError):
    def __init__(self, reason: str, path: tuple["Peer", ...] = ()) -> None:
        super().__init__(reason)
        self.path = path


@dataclass(frozen=True)
class Peer:
    id: int
    endpoint: str


@dataclass(frozen=True)
class Finger:
    start: int
    peer: Peer | None


@dataclass(frozen=True)
class RoutingState:
    peer: Peer
    predecessor: Peer | None
    successors: tuple[Peer, ...]
    fingers: tuple[Finger, ...]
    active: bool

    @property
    def successor(self) -> Peer | None:
        return self.successors[0] if self.successors else None


@dataclass(frozen=True)
class RouteStep:
    successor: Peer
    preceding: Peer


@dataclass(frozen=True)
class LookupResult:
    owner: Peer
    predecessor: Peer
    path: tuple[Peer, ...]

    @property
    def hops(self) -> int:
        """Overlay edges, including the final edge to the owner (not ping RPCs)."""
        return len(self.path) - 1


class EventKind(str, Enum):
    JOINED = "joined"
    LEFT = "left"
    SUCCESSOR_CHANGED = "successor_changed"
    PREDECESSOR_CHANGED = "predecessor_changed"
    PEER_UNAVAILABLE = "peer_unavailable"
    PEER_AVAILABLE = "peer_available"
    FINGER_REPAIRED = "finger_repaired"
    REPAIR_REQUESTED = "repair_requested"
    LOOKUP_SUCCEEDED = "lookup_succeeded"
    LOOKUP_FAILED = "lookup_failed"


@dataclass(frozen=True)
class Event:
    kind: EventKind
    node: Peer
    peer: Peer | None = None
    previous: Peer | None = None
    index: int | None = None
    detail: str = ""


@dataclass(frozen=True)
class MaintenanceResult:
    errors: tuple[str, ...]


class Transport(Protocol):
    """Adapters must bound RPC work/time and return immutable messages.

    Liveness is a reachability hint, never a proof of permanent death. An adapter
    must authenticate endpoint/ID bindings and reject mixed identifier spaces.
    """

    def ping(self, source: Peer, target: Peer) -> bool: ...

    def read_state(self, source: Peer, target: Peer) -> RoutingState: ...

    def route(self, source: Peer, target: Peer, identifier: int) -> RouteStep: ...

    def notify(self, source: Peer, target: Peer) -> None: ...
