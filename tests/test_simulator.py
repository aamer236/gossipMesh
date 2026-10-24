import itertools
import unittest

from chord import InvalidState, LookupFailed, PeerUnavailable
from simulator import ChordSimulation, VirtualClock


class ClockTests(unittest.TestCase):
    def test_cancelled_event_does_not_exhaust_callback_budget(self):
        clock = VirtualClock()
        seen = []
        clock.call_at(1, lambda: seen.append(1))
        clock.call_at(1, lambda: seen.append(2)).cancel()
        self.assertEqual(clock.run_until(1, max_events=1), 1)
        self.assertEqual(seen, [1])

    def test_fifo_cancellation_and_atomic_service_time(self):
        clock = VirtualClock()
        seen = []
        clock.call_at(1, lambda: (seen.append(("first", clock.now)), clock.consume(3)))
        clock.call_at(1, lambda: seen.append(("second", clock.now)))
        clock.call_at(2, lambda: seen.append(("third", clock.now)))
        clock.call_at(1, lambda: seen.append(("cancelled", clock.now))).cancel()
        self.assertEqual(clock.run_until(1), 2)
        self.assertEqual(seen, [("first", 1), ("second", 4)])
        clock.run_until(4)
        self.assertEqual(seen[-1], ("third", 4))

    def test_budget_and_invalid_times(self):
        clock = VirtualClock()

        def repeat():
            clock.call_later(0, repeat)

        clock.call_later(0, repeat)
        with self.assertRaisesRegex(RuntimeError, "budget"):
            clock.run_until(0, max_events=3)
        for value in (-1, float("nan"), float("inf"), True):
            with self.assertRaises(ValueError):
                clock.consume(value)


class SimulationTests(unittest.TestCase):
    def make_ring(self, ids=(0, 4, 8, 12), **kwargs):
        simulation = ChordSimulation(m=4, **kwargs)
        for identifier in ids:
            simulation.add_node(identifier)
        simulation.converge(max_rounds=100)
        return simulation

    def verify_all(self, simulation):
        self.assertTrue(simulation.diagnostics().converged)
        oracle = simulation.oracle()
        for node in simulation.live_nodes():
            for key in range(simulation.space.size):
                result = node.lookup_id(key)
                self.assertEqual(result.owner.id, oracle.owner(key))
                self.assertEqual(result.predecessor.id, oracle.predecessor(key))

    def test_all_small_membership_subsets_and_join_permutations(self):
        # Exhaustive all 255 nonempty membership subsets of an 8-ID space.
        for count in range(1, 9):
            for ids in itertools.combinations(range(8), count):
                simulation = ChordSimulation(m=3)
                for identifier in ids:
                    simulation.add_node(identifier)
                simulation.converge(max_rounds=40)
                self.verify_all(simulation)
        for ids in itertools.permutations((0, 2, 5, 7)):
            simulation = ChordSimulation(m=3)
            for identifier in ids:
                simulation.add_node(identifier)
                simulation.converge(max_rounds=40)
            self.verify_all(simulation)

    def test_virtual_delay_scales_latency_without_changing_routes(self):
        routes = []
        elapsed = []
        for latency in (1, 3):
            simulation = self.make_ring(latency=latency)
            start = simulation.clock.now
            result = simulation.nodes[0].lookup_id(11)
            elapsed.append(simulation.clock.now - start)
            routes.append(result.path)
        self.assertEqual(routes[0], routes[1])
        self.assertGreater(elapsed[0], 0)
        self.assertEqual(elapsed[1], 3 * elapsed[0])

    def test_dropped_notification_has_no_side_effect_and_can_retry(self):
        simulation = ChordSimulation(m=4, latency=1, timeout=7)
        a = simulation.add_node(0)
        b = simulation.add_node(8)
        before = a.state
        start = simulation.clock.now
        simulation.transport.drop_next(b.peer, a.peer)
        with self.assertRaises(PeerUnavailable):
            simulation.transport.notify(b.peer, a.peer)
        self.assertEqual(a.state, before)
        self.assertEqual(simulation.clock.now - start, 7)
        self.assertEqual(simulation.transport.dropped, 1)
        simulation.converge()
        self.verify_all(simulation)

    def test_seeded_drops_are_reproducible_and_bound_failed_maintenance(self):
        outcomes = []
        for _ in range(2):
            simulation = self.make_ring(seed=17, latency=1)
            simulation.transport.set_drop_rate(0.35)
            simulation.run_rounds(8)
            outcomes.append(
                (
                    simulation.trace(),
                    dict(simulation.transport.calls),
                    simulation.transport.dropped,
                )
            )
            simulation.transport.set_drop_rate(1)
            with self.assertRaises(LookupFailed):
                simulation.nodes[0].lookup_id(3)
            with self.assertRaises(RuntimeError):
                simulation.converge(max_rounds=3)
            simulation.transport.set_drop_rate(0)
            simulation.converge()
            self.verify_all(simulation)
        self.assertEqual(outcomes[0], outcomes[1])

    def test_scheduled_crash_and_recovery_are_serialized(self):
        simulation = self.make_ring(latency=0)
        failed = simulation.nodes[4]
        simulation.clock.call_later(0.25, lambda: simulation.crash(4))
        simulation.run_rounds(1)
        self.assertNotIn(failed, simulation.live_nodes())
        simulation.converge()
        self.verify_all(simulation)
        simulation.clock.call_later(0.25, lambda: simulation.recover(4))
        simulation.run_rounds(1)
        simulation.converge()
        self.verify_all(simulation)

    def test_directed_links_and_local_delays(self):
        simulation = self.make_ring(latency=1)
        a, b = simulation.nodes[0], simulation.nodes[4]
        simulation.transport.set_latency(a.peer, b.peer, 9)
        start = simulation.clock.now
        self.assertTrue(simulation.transport.ping(a.peer, b.peer))
        self.assertEqual(simulation.clock.now - start, 9)
        simulation.transport.set_link(a.peer, b.peer, False)
        self.assertTrue(simulation.transport.ping(b.peer, a.peer))
        with self.assertRaises(LookupFailed):
            a.lookup_id(3)
        simulation.transport.set_link(a.peer, b.peer, True)
        self.verify_all(simulation)

    def test_withdraw_rejoin_duplicates_and_no_route(self):
        simulation = self.make_ring()
        simulation.leave(4)
        simulation.converge()
        simulation.rejoin(4, 0)
        simulation.converge()
        self.verify_all(simulation)
        with self.assertRaises(InvalidState):
            simulation.add_node(4)
        with self.assertRaises(ValueError):
            simulation.add_node(16)
        for identifier in (4, 8, 12):
            simulation.crash(identifier)
        with self.assertRaises(RuntimeError):
            simulation.converge(max_rounds=6)
        with self.assertRaises(LookupFailed):
            simulation.nodes[0].lookup_id(0)
        for identifier in (4, 8, 12):
            simulation.recover(identifier)
        simulation.converge()
        self.verify_all(simulation)

    def test_diagnostics_do_not_deliver_or_modify_anything(self):
        simulation = self.make_ring()
        states = [node.state for node in simulation.nodes.values()]
        before = dict(simulation.transport.calls), simulation.clock.now
        for _ in range(4):
            simulation.diagnostics()
            simulation.oracle()
        self.assertEqual(
            before, (dict(simulation.transport.calls), simulation.clock.now)
        )
        self.assertEqual(states, [node.state for node in simulation.nodes.values()])

    def test_withdrawn_offline_node_can_explicitly_rejoin(self):
        simulation = self.make_ring()
        simulation.crash(4)
        with self.assertRaises(InvalidState):
            simulation.rejoin(4, 0)
        simulation.leave(4)
        simulation.converge()
        simulation.rejoin(4, 0)
        simulation.converge()
        self.verify_all(simulation)
