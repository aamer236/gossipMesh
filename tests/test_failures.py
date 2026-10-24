import random
import unittest
from unittest.mock import patch

from chord import (
    ChordNode,
    EventKind,
    IdentifierSpace,
    InMemoryTransport,
    InvalidState,
    LookupFailed,
    Peer,
    PeerUnavailable,
)
from tests.support import consistent, converge, expected_owner, make_ring


class FailureTests(unittest.TestCase):
    def test_finger_rotation_advances_after_lookup_failure(self):
        node = self.nodes[0]
        before = node.state
        with patch.object(
            node, "find_successor", side_effect=LookupFailed("dropped")
        ) as lookup:
            for _ in range(node.space.m):
                with self.assertRaises(LookupFailed):
                    node.fix_fingers()
        starts = [call.args[0] for call in lookup.call_args_list]
        self.assertEqual(len(set(starts)), node.space.m)
        self.assertEqual(node.state, before)

    def test_explicit_rejoin_discards_previous_lifecycle_hints(self):
        node, bootstrap = self.nodes[:2]
        node.set_peer_available(bootstrap.peer, False, version=100)
        node.leave()
        converge([other for other in self.nodes if other != node])
        node.join(bootstrap.peer)
        converge(self.nodes)
        self.assert_ownership(self.nodes)

    def setUp(self):
        self.nodes, self.transport = make_ring(list(range(0, 32, 4)), m=5)
        converge(self.nodes)

    def assert_ownership(self, nodes):
        for node in nodes:
            for key in range(32):
                self.assertEqual(node.find_successor(key), expected_owner(nodes, key))

    def test_successor_failure_explicit_before_repair(self):
        source, failed = self.nodes[:2]
        self.transport.set_available(failed.peer, False)
        with self.assertRaises(LookupFailed) as error:
            source.lookup_id(7)
        self.assertIn("successor unavailable", str(error.exception))
        self.assertLessEqual(len(error.exception.path), 512)
        source.stabilize()
        self.assertEqual(source.state.successor, self.nodes[2].peer)
        live = [n for n in self.nodes if n != failed]
        converge(live)
        self.assert_ownership(live)

    def test_predecessor_failure(self):
        source, failed = self.nodes[2], self.nodes[1]
        self.transport.set_available(failed.peer, False)
        source.check_predecessor()
        self.assertIsNone(source.state.predecessor)
        converge([n for n in self.nodes if n != failed])
        self.assertEqual(source.state.predecessor, self.nodes[0].peer)

    def test_dead_finger_is_skipped(self):
        source, failed = self.nodes[0], self.nodes[4]
        self.assertEqual(source.closest_preceding_finger(27), failed.peer)
        self.transport.set_available(failed.peer, False)
        self.assertNotEqual(source.closest_preceding_finger(27), failed.peer)
        self.assertEqual(source.find_successor(27), self.nodes[7].peer)
        live = [n for n in self.nodes if n != failed]
        converge(live)
        self.assert_ownership(live)

    def test_multiple_successor_failures(self):
        for failed in self.nodes[1:4]:
            self.transport.set_available(failed.peer, False)
        self.nodes[0].stabilize()
        self.assertEqual(self.nodes[0].state.successor, self.nodes[4].peer)
        live = [self.nodes[0], *self.nodes[4:]]
        converge(live)
        self.assert_ownership(live)

    def test_no_usable_route_does_not_invent_singleton(self):
        source = self.nodes[0]
        for node in self.nodes[1:]:
            self.transport.set_available(node.peer, False)
        with self.assertRaises(PeerUnavailable):
            source.stabilize()
        self.assertTrue(source.maintain().errors)
        for key in range(32):
            with self.assertRaises(LookupFailed):
                source.lookup_id(key)
        self.assertNotEqual(source.state.successor, source.peer)

    def test_recovery_of_saved_state(self):
        failed = self.nodes[1]
        self.transport.set_available(failed.peer, False)
        converge([n for n in self.nodes if n != failed])
        self.transport.set_available(failed.peer, True)
        converge(self.nodes)
        self.assert_ownership(self.nodes)

    def test_duplicate_and_out_of_order_hints_and_repairs(self):
        source, failed = self.nodes[:2]
        events = []
        # Observe a fresh source through the public constructor.
        space = IdentifierSpace(5)
        transport = InMemoryTransport(space)
        observer = ChordNode(
            Peer(1, "observer"), space, transport, on_event=events.append
        )
        transport.register(observer)
        observer.create()
        other = ChordNode(Peer(9, "other"), space, transport)
        transport.register(other)
        other.join(observer.peer)
        converge([observer, other])
        observer.set_peer_available(other.peer, False, version=2)
        observer.set_peer_available(other.peer, False, version=2)
        self.assertEqual(sum(e.kind == EventKind.PEER_UNAVAILABLE for e in events), 1)
        observer.set_peer_available(other.peer, True, version=3)
        observer.set_peer_available(other.peer, False, version=1)
        self.assertEqual(observer.find_successor(9), other.peer)
        self.assertEqual(observer.repair_peer(other.peer).errors, ())
        self.assertEqual(observer.repair_peer(other.peer).errors, ())
        self.assertTrue(consistent([observer, other]))
        self.assertIn(EventKind.REPAIR_REQUESTED, [e.kind for e in events])
        self.assertIn(EventKind.LOOKUP_SUCCEEDED, [e.kind for e in events])
        self.assertIn(EventKind.SUCCESSOR_CHANGED, [e.kind for e in events])
        # Repair the affected references without editing private routing state.
        self.transport.set_available(failed.peer, False)
        source.set_peer_available(failed.peer, False)
        source.repair_peer(failed.peer)
        source.repair_peer(failed.peer)
        converge([n for n in self.nodes if n != failed])
        self.assertNotIn(failed.peer, [f.peer for f in source.state.fingers])

    def test_partial_partition_and_restored_link(self):
        source, successor = self.nodes[:2]
        self.transport.set_link(source.peer, successor.peer, False)
        with self.assertRaises(LookupFailed):
            source.find_successor(3)
        self.transport.set_link(source.peer, successor.peer, True)
        self.assertEqual(source.find_successor(3), successor.peer)

    def test_leave_and_rejoin(self):
        departing = self.nodes[2]
        departing.leave()
        with self.assertRaises(InvalidState):
            departing.find_successor(1)
        live = [n for n in self.nodes if n != departing]
        converge(live)
        departing.join(live[0].peer)
        converge(self.nodes)
        self.assert_ownership(self.nodes)

    def test_two_node_survivor_requires_external_bootstrap(self):
        nodes, transport = make_ring([1, 9])
        converge(nodes)
        transport.set_available(nodes[1].peer, False)
        with self.assertRaises(PeerUnavailable):
            nodes[0].stabilize()
        with self.assertRaises(LookupFailed):
            nodes[0].find_successor(3)
        transport.set_available(nodes[1].peer, True)
        converge(nodes)
        self.assertEqual(nodes[0].find_successor(3), nodes[1].peer)

    def test_two_node_local_withdrawal_has_no_atomic_graceful_handoff(self):
        nodes, _ = make_ring([1, 9])
        converge(nodes)
        nodes[1].leave()
        self.assertTrue(nodes[0].maintain().errors)
        with self.assertRaises(LookupFailed):
            nodes[0].find_successor(3)

    def test_finger_fallback_when_successor_list_exhausted(self):
        nodes, transport = make_ring(list(range(0, 32, 4)), m=5, list_size=1)
        converge(nodes, lists=False)
        transport.set_available(nodes[1].peer, False)
        nodes[0].stabilize()
        self.assertEqual(nodes[0].state.successor, nodes[2].peer)
        converge([n for n in nodes if n != nodes[1]], lists=False)

    def test_seeded_failure_subsets_and_recovery(self):
        rng = random.Random(729)
        for _ in range(12):
            nodes, transport = make_ring(rng.sample(range(32), 9), m=5)
            converge(nodes)
            failed = rng.sample(nodes, rng.randint(1, 3))
            for node in failed:
                transport.set_available(node.peer, False)
            live = [n for n in nodes if n not in failed]
            converge(live)
            self.assert_ownership(live)
            for node in failed:
                transport.set_available(node.peer, True)
            converge(nodes)
            self.assert_ownership(nodes)
