"""32 real Chord nodes with explicit ticks and read-only visualization diagnostics."""

from collections import deque
from typing import Any

from chord import ChordNode, Event, EventKind, IdentifierSpace, InMemoryTransport, Peer


class Simulation:
    NODE_COUNT = 32

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.space = IdentifierSpace(8)
        self.transport = InMemoryTransport(self.space)
        self.nodes: dict[int, ChordNode] = {}
        self.offline: set[int] = set()
        self.tick = 0
        self.paused = False
        self.interval = 1.0
        self.events: deque[dict[str, Any]] = deque(maxlen=100)
        self.sequence = 0
        self.checks = 0
        self.repairs = 0
        self.last_checks: dict[int, dict[str, Any]] = {}
        self.last_finger: dict[int, int] = {}
        self._warming = True
        for identifier in range(0, self.space.size, 8):
            node = ChordNode(
                Peer(identifier, f"node-{identifier}"),
                self.space,
                self.transport,
                on_event=self._on_event,
            )
            self.transport.register(node)
            if self.nodes:
                node.join(self.nodes[0].peer)
            else:
                node.create()
            self.nodes[identifier] = node
        # Only orchestration inspects global membership. All updates still go
        # through distributed maintenance; no initial routing tables are injected.
        for _ in range(128):
            for node in self.nodes.values():
                node.maintain()
            if self._converged():
                break
        else:
            raise RuntimeError("initial 32-node ring failed to converge")
        self._warming = False
        self.last_finger.clear()
        self.transport.calls.clear()
        self._log("ready", None, "32 nodes joined. Routing tables converged.")

    def _on_event(self, event: Event) -> None:
        if event.kind == EventKind.FINGER_REPAIRED and event.index is not None:
            self.last_finger[event.node.id] = event.index
        if self._warming:
            return
        if event.kind in (EventKind.SUCCESSOR_CHANGED, EventKind.PREDECESSOR_CHANGED):
            label = (
                "Successor"
                if event.kind == EventKind.SUCCESSOR_CHANGED
                else "Predecessor"
            )
            previous = "none" if event.previous is None else f"{event.previous.id:03d}"
            current = "none" if event.peer is None else f"{event.peer.id:03d}"
            self._log(
                event.kind.value, event.node.id, f"{label} {previous} → {current}"
            )

    def _log(self, kind: str, node: int | None, message: str) -> None:
        self.sequence += 1
        self.events.append(
            {
                "sequence": self.sequence,
                "tick": self.tick,
                "kind": kind,
                "node": node,
                "message": message,
            }
        )

    def _node(self, identifier: int) -> ChordNode:
        if type(identifier) is not int or identifier not in self.nodes:
            raise ValueError("unknown node ID")
        return self.nodes[identifier]

    def crash(self, identifier: int) -> None:
        node = self._node(identifier)
        if identifier in self.offline:
            return
        self.transport.set_available(node.peer, False)
        self.offline.add(identifier)
        self._log(
            "crash",
            identifier,
            "Endpoint crashed. Peers discover it through periodic checks.",
        )
        # Deliberately no broadcast, pointer mutation, or immediate repair here.

    def restore(self, identifier: int) -> None:
        node = self._node(identifier)
        if identifier not in self.offline:
            return
        self.transport.set_available(node.peer, True)
        self.offline.remove(identifier)
        self._log(
            "restore", identifier, "Endpoint restored with its saved routing state."
        )

    def configure(
        self, *, paused: bool | None = None, interval: float | None = None
    ) -> None:
        if paused is not None and type(paused) is not bool:
            raise ValueError("paused must be a boolean")
        if interval is not None and (
            type(interval) not in (int, float) or interval not in (0.5, 1.0, 2.0)
        ):
            raise ValueError("interval must be 0.5, 1, or 2 seconds")
        if paused is not None:
            self.paused = paused
        if interval is not None:
            self.interval = interval

    def advance(self) -> None:
        """One explicit maintenance round, callable without clocks or sleeps."""
        self.tick += 1
        changes = errors = alive = 0
        for identifier, node in self.nodes.items():
            if identifier in self.offline:
                continue
            alive += 1
            before = node.state
            report = node.maintain()
            after = node.state
            repaired = [
                i
                for i, (old, new) in enumerate(zip(before.fingers, after.fingers))
                if old.peer != new.peer
            ]
            changes += len(repaired)
            errors += len(report.errors)
            self.last_checks[identifier] = {
                "tick": self.tick,
                "errors": list(report.errors),
                "finger_index": self.last_finger.get(identifier),
                "changed_fingers": repaired,
            }
            for i in repaired:
                target = after.fingers[i].peer
                self._log(
                    "finger_changed",
                    identifier,
                    f"Finger {i} → {target.id:03d}" if target else f"Finger {i} → none",
                )
        self.checks += alive * 3
        self.repairs += changes
        self._log(
            "maintenance",
            None,
            f"Checked {alive} nodes · {changes} finger updates · {errors} operation errors",
        )

    def _expected_owner(self, identifier: int) -> Peer | None:
        live = [
            node.peer for key, node in self.nodes.items() if key not in self.offline
        ]
        if not live:
            return None
        return next((peer for peer in live if peer.id >= identifier), live[0])

    def _converged(self) -> bool:
        live = [node for key, node in self.nodes.items() if key not in self.offline]
        if not live:
            return False
        for i, node in enumerate(live):
            state = node.state
            if state.successor != live[(i + 1) % len(live)].peer:
                return False
            if state.predecessor != live[(i - 1) % len(live)].peer:
                return False
            if any(f.peer != self._expected_owner(f.start) for f in state.fingers):
                return False
        return True

    def snapshot(self) -> dict[str, Any]:
        """A global read-only observer; diagnostic knowledge NEVER repairs routes."""
        result = []
        stale = correct = total = 0
        for identifier, node in self.nodes.items():
            state = node.state
            online = identifier not in self.offline
            fingers = []
            for i, finger in enumerate(state.fingers):
                target = finger.peer.id if finger.peer else None
                unavailable = target is None or target in self.offline
                accurate = online and finger.peer == self._expected_owner(finger.start)
                if online:
                    total += 1
                    correct += int(accurate)
                    stale += int(unavailable)
                fingers.append(
                    {
                        "index": i,
                        "start": finger.start,
                        "target": target,
                        "stale": unavailable,
                        "correct": accurate,
                    }
                )
            result.append(
                {
                    "id": identifier,
                    "online": online,
                    "successor": state.successor.id if state.successor else None,
                    "predecessor": state.predecessor.id if state.predecessor else None,
                    "successors": [peer.id for peer in state.successors],
                    "fingers": fingers,
                    "last_check": self.last_checks.get(identifier),
                }
            )
        return {
            "node_count": self.NODE_COUNT,
            "m": self.space.m,
            "size": self.space.size,
            "tick": self.tick,
            "paused": self.paused,
            "interval": self.interval,
            "online": self.NODE_COUNT - len(self.offline),
            "stale_fingers": stale,
            "correct_fingers": correct,
            "total_fingers": total,
            "converged": self._converged(),
            "checks": self.checks,
            "repairs": self.repairs,
            "rpc_calls": dict(self.transport.calls),
            "nodes": result,
            "events": list(reversed(self.events)),
        }
