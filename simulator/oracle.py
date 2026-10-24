"""Global observer used only by tests/experiments, never by Chord routing."""

from bisect import bisect_left
from collections.abc import Iterable


class RingOracle:
    def __init__(self, identifiers: Iterable[int]) -> None:
        self.ids = tuple(sorted(identifiers))
        if len(set(self.ids)) != len(self.ids):
            raise ValueError("oracle identifiers must be unique")

    def owner(self, identifier: int) -> int | None:
        if not self.ids:
            return None
        return self.ids[bisect_left(self.ids, identifier) % len(self.ids)]

    def predecessor(self, identifier: int) -> int | None:
        owner = self.owner(identifier)
        if owner is None:
            return None
        return self.ids[(self.ids.index(owner) - 1) % len(self.ids)]
