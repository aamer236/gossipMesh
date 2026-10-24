import hashlib
import unittest

from chord.identifiers import IdentifierSpace


class IdentifierTests(unittest.TestCase):
    def test_hashing_and_truncation(self):
        for m in (1, 4, 160, 256):
            space = IdentifierSpace(m)
            expected = int.from_bytes(
                hashlib.sha256(b"chord:key\0hello").digest(), "big"
            ) % (1 << m)
            self.assertEqual(space.key_id("hello"), expected)
            self.assertEqual(space.key_id("hello"), space.key_id(b"hello"))
            self.assertLess(space.node_id("node"), space.size)
        self.assertNotEqual(
            IdentifierSpace().node_id("same"), IdentifierSpace().key_id("same")
        )

    def test_validation_and_arithmetic(self):
        space = IdentifierSpace(4)
        for bad in (0, -1, 257, True, 2.5):
            with self.assertRaises(ValueError):
                IdentifierSpace(bad)
        for bad in (-1, 16, True, 1.5):
            with self.assertRaises(ValueError):
                space.validate(bad)
        self.assertEqual(space.add(15, 3), 2)
        self.assertEqual(space.add(0, -1), 15)
        self.assertEqual(space.distance(12, 3), 7)
        self.assertEqual([space.finger_start(14, i) for i in range(4)], [15, 0, 2, 6])
        with self.assertRaises(ValueError):
            space.finger_start(0, 4)

    def test_all_intervals_exhaustively(self):
        space = IdentifierSpace(4)
        # An independent enumeration oracle walks one clockwise turn.
        for a in range(16):
            for b in range(16):
                interior = []
                cursor = (a + 1) % 16
                while cursor != b:
                    interior.append(cursor)
                    cursor = (cursor + 1) % 16
                for left in (False, True):
                    for right in (False, True):
                        expected = set(interior)
                        if left:
                            expected.add(a)
                        if right:
                            expected.add(b)
                        for value in range(16):
                            self.assertEqual(
                                space.contains(
                                    value, a, b, left_closed=left, right_closed=right
                                ),
                                value in expected,
                                (value, a, b, left, right),
                            )

    def test_wraparound_example(self):
        space = IdentifierSpace(4)
        self.assertEqual(
            [n for n in range(16) if space.contains(n, 12, 3, right_closed=True)],
            [0, 1, 2, 3, 13, 14, 15],
        )
