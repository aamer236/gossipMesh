"""Deterministic virtual time. Callbacks are serialized; no wall-clock sleeps."""

import heapq
import math
from collections.abc import Callable
from dataclasses import dataclass, field


def duration(value: float) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError("virtual duration must be finite and nonnegative")
    return float(value)


@dataclass(order=True)
class ScheduledEvent:
    time: float
    sequence: int
    callback: Callable[[], None] = field(compare=False, repr=False)
    cancelled: bool = field(default=False, compare=False)

    def cancel(self) -> None:
        self.cancelled = True


class VirtualClock:
    """Equal deadlines execute FIFO. RPC costs never dispatch reentrant events.

    A callback can consume time beyond a run deadline. It completes atomically;
    queued callbacks retain deadline order but may consequently start late.
    """

    def __init__(self) -> None:
        self.now = 0.0
        self._sequence = 0
        self._events: list[ScheduledEvent] = []

    def consume(self, elapsed: float) -> None:
        self.now += duration(elapsed)

    def call_at(self, when: float, callback: Callable[[], None]) -> ScheduledEvent:
        when = duration(when)
        if when < self.now:
            raise ValueError("cannot schedule a new event in the past")
        event = ScheduledEvent(when, self._sequence, callback)
        self._sequence += 1
        heapq.heappush(self._events, event)
        return event

    def call_later(self, delay: float, callback: Callable[[], None]) -> ScheduledEvent:
        return self.call_at(self.now + duration(delay), callback)

    def run_until(self, until: float, *, max_events: int = 10000) -> int:
        until = duration(until)
        if until < self.now:
            raise ValueError("cannot run time backwards")
        if type(max_events) is not int or max_events < 1:
            raise ValueError("max_events must be positive")
        count = 0
        while self._events and self._events[0].time <= until:
            if self._events[0].cancelled:
                heapq.heappop(self._events)
                continue
            if count >= max_events:
                raise RuntimeError("scheduled event budget exhausted")
            event = heapq.heappop(self._events)
            self.now = max(self.now, event.time)
            event.callback()
            count += 1
        self.now = max(self.now, until)
        return count
