"""Identifiers and clockwise intervals; no ring enumeration is needed."""

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class IdentifierSpace:
    """SHA-256 mapped to the low m bits; IDs themselves must be in range."""

    m: int = 160

    def __post_init__(self) -> None:
        if type(self.m) is not int or not 1 <= self.m <= 256:
            raise ValueError("m must be an integer in [1, 256]")

    @property
    def size(self) -> int:
        return 1 << self.m

    def validate(self, identifier: int) -> int:
        if type(identifier) is not int or not 0 <= identifier < self.size:
            raise ValueError(f"identifier must be in [0, {self.size})")
        return identifier

    def add(self, identifier: int, offset: int) -> int:
        self.validate(identifier)
        if type(offset) is not int:
            raise ValueError("offset must be an integer")
        return (identifier + offset) % self.size

    def distance(self, start: int, end: int) -> int:
        return (self.validate(end) - self.validate(start)) % self.size

    def finger_start(self, identifier: int, index: int) -> int:
        if type(index) is not int or not 0 <= index < self.m:
            raise ValueError("finger index out of range")
        return self.add(identifier, 1 << index)

    def _hash(self, domain: bytes, value: str | bytes) -> int:
        data = value.encode("utf-8") if isinstance(value, str) else value
        if not isinstance(data, bytes):
            raise TypeError("hash input must be str or bytes")
        return int.from_bytes(hashlib.sha256(domain + data).digest(), "big") % self.size

    def node_id(self, identity: str | bytes) -> int:
        return self._hash(b"chord:node\0", identity)

    def key_id(self, key: str | bytes) -> int:
        return self._hash(b"chord:key\0", key)

    def contains(
        self,
        value: int,
        start: int,
        end: int,
        *,
        left_closed: bool = False,
        right_closed: bool = False,
    ) -> bool:
        """A clockwise arc. Equal endpoints mean one full turn.

        (a,a) excludes a; (a,a], [a,a), [a,a] include the whole ring.
        This explicit convention supports singleton Chord ownership.
        """
        distance = self.distance(start, value)
        length = self.distance(start, end)
        if length == 0:
            return distance != 0 or left_closed or right_closed
        return (
            0 < distance < length
            or (distance == 0 and left_closed)
            or (distance == length and right_closed)
        )
