import random
import unittest
from dataclasses import FrozenInstanceError
from unittest.mock import patch

from chord import (
    ChordNode,
    IdentifierSpace,
    InMemoryTransport,
    InvalidState,
    LookupFailed,
    Peer,
    RouteStep,
)
from tests.support import consistent, converge, expected_owner, make_ring


class RoutingTests(unittest.TestCase):
    def assert_all_lookups(self, nodes):
        ordered = sorted((n.peer for n in nodes), key=lambda p: p.id)
        for node in nodes:
            for key in range(node.space.size):
                expected = expected_owner(nodes, key)
                result = node.lookup_id(key)
                self.assertEqual(result.owner, expected, (node.peer, key, result))
                previous = ordered[(ordered.index(expected) - 1) % len(ordered)]
                self.assertEqual(result.predecessor, previous)
                self.assertEqual(node.find_successor(key), expected)
                self.assertEqual(node.find_predecessor(key), previous)

    def test_empty_ring_and_unknown_bootstrap(self):
        space = IdentifierSpace(4)
        transport = InMemoryTransport(space)
        node = ChordNode(Peer(1, "a"), space, transport)
        transport.register(node)
        self.assertIsNone(node.state.successor)
        with self.assertRaises(InvalidState):
            node.lookup_id(0)
        with self.assertRaises(LookupFailed):
            node.join(Peer(2, "unknown"))
        self.assertFalse(node.state.active)
        with self.assertRaises(InvalidState):
            node.join(node.peer)

    def test_singleton(self):
        nodes, _ = make_ring([9])
        self.assertTrue(consistent(nodes))
        self.assert_all_lookups(nodes)
        for _ in range(10):
            self.assertEqual(nodes[0].maintain().errors, ())
        self.assertEqual(nodes[0].lookup("application-key").owner, nodes[0].peer)
        with self.assertRaises(InvalidState):
            nodes[0].create()

    def test_two_nodes_in_both_orders(self):
        for ids in ([0, 15], [15, 0], [3, 10]):
            nodes, _ = make_ring(ids)
            self.assertIsNone(nodes[-1].state.predecessor)
            self.assertTrue(all(f.peer is None for f in nodes[-1].state.fingers[1:]))
            converge(nodes)
            self.assert_all_lookups(nodes)

    def test_join_orders_sequential_and_batched(self):
        for ids in ([0, 3, 6, 9, 12, 15], [15, 12, 9, 6, 3, 0], [6, 0, 15, 3, 12, 9]):
            for sequential in (False, True):
                nodes, _ = make_ring(ids, sequential=sequential)
                converge(nodes)
                self.assertTrue(consistent(nodes))
                self.assert_all_lookups(nodes)

    def test_seeded_small_rings(self):
        rng = random.Random(20261024)
        for _ in range(20):
            nodes, _ = make_ring(rng.sample(range(32), rng.randrange(2, 16)), m=5)
            converge(nodes)
            self.assert_all_lookups(nodes)

    def test_closest_preceding_finger(self):
        nodes, transport = make_ring([0, 2, 4, 8, 12, 15])
        converge(nodes)
        node = nodes[0]
        self.assertEqual(node.closest_preceding_finger(7).id, 4)
        self.assertEqual(node.closest_preceding_finger(8).id, 4)
        self.assertEqual(node.closest_preceding_finger(0).id, 8)
        self.assertEqual(node.closest_preceding_finger(1), node.peer)
        transport.set_available(nodes[2].peer, False)
        self.assertEqual(node.closest_preceding_finger(7).id, 2)

    def test_bounded_lookup_and_bad_input(self):
        nodes, _ = make_ring([0, 2, 4, 8, 12, 15])
        converge(nodes)
        with self.assertRaises(LookupFailed) as caught:
            nodes[0].lookup_id(14, max_hops=1)
        self.assertEqual(len(caught.exception.path), 1)
        self.assertIn("exceeded", str(caught.exception))
        for key in (-1, 16):
            with self.assertRaises(ValueError):
                nodes[0].lookup_id(key)
        with self.assertRaises(ValueError):
            nodes[0].lookup_id(1, max_hops=0)

    def test_state_is_immutable_and_peers_validated(self):
        nodes, transport = make_ring([1])
        node = nodes[0]
        with self.assertRaises(FrozenInstanceError):
            node.state.active = False
        with self.assertRaises(InvalidState):
            transport.register(ChordNode(Peer(1, "other"), node.space, transport))
        with self.assertRaises(InvalidState):
            node.notify(Peer(1, "other"))
        with self.assertRaises(ValueError):
            transport.register(ChordNode(Peer(2, "b"), IdentifierSpace(5), transport))
        with self.assertRaises(ValueError):
            ChordNode(Peer(16, "bad"), node.space, transport)

    def test_unreachable_candidate_is_not_adopted(self):
        nodes, _ = make_ring([5])
        nodes[0].notify(Peer(3, "unknown"))
        self.assertEqual(nodes[0].state.predecessor, nodes[0].peer)

    def test_nonprogressing_remote_finger_is_rejected(self):
        nodes, transport = make_ring([0, 4, 8, 12, 15])
        converge(nodes)
        # The remote successor is valid, but its proposed finger overshoots 14.
        with patch.object(
            transport, "route", return_value=RouteStep(nodes[3].peer, nodes[4].peer)
        ):
            with self.assertRaises(LookupFailed) as error:
                nodes[0].lookup_id(14)
        self.assertIn("no clockwise progress", str(error.exception))
        self.assertEqual(len(error.exception.path), 2)

    def test_notification_preserves_closer_predecessor(self):
        nodes, transport = make_ring([0, 4, 8, 12])
        converge(nodes)
        source = nodes[2]
        source.notify(nodes[0].peer)
        source.notify(nodes[3].peer)
        self.assertEqual(source.state.predecessor, nodes[1].peer)
        newcomer = ChordNode(Peer(6, "six"), source.space, transport)
        transport.register(newcomer)
        newcomer.join(nodes[0].peer)
        source.notify(newcomer.peer)
        source.notify(nodes[1].peer)
        self.assertEqual(source.state.predecessor, newcomer.peer)
        converge([*nodes, newcomer])

    def test_correct_successor_backbone_routes_with_live_stale_fingers(self):
        nodes, transport = make_ring([0, 4, 8, 12])
        converge(nodes)
        newcomer = ChordNode(Peer(6, "newcomer"), nodes[0].space, transport)
        transport.register(newcomer)
        newcomer.join(nodes[0].peer)
        nodes.append(newcomer)
        # Repair only the successor backbone; intentionally leave higher fingers stale.
        for _ in range(16):
            for node in nodes:
                node.check_predecessor()
                node.stabilize()
        self.assertTrue(consistent(nodes, fingers=False))
        self.assertFalse(consistent(nodes))
        self.assert_all_lookups(nodes)
