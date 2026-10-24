import unittest
from unittest.mock import patch

from chord import LookupFailed
from visualizer.simulation import Simulation


class VisualizerTests(unittest.TestCase):
    def setUp(self):
        self.simulation = Simulation()

    def test_initial_32_node_ring_uses_actual_routing(self):
        simulation = self.simulation
        snapshot = simulation.snapshot()
        self.assertEqual(snapshot["node_count"], 32)
        self.assertEqual(len(snapshot["nodes"]), 32)
        self.assertTrue(snapshot["converged"])
        self.assertEqual(snapshot["tick"], 0)
        for node in simulation.nodes.values():
            for key in range(256):
                expected = simulation._expected_owner(key)
                self.assertEqual(node.find_successor(key), expected)

    def test_crash_does_not_inject_routing_updates(self):
        simulation = self.simulation
        before = {key: node.state for key, node in simulation.nodes.items()}
        simulation.crash(8)
        simulation.crash(8)
        self.assertEqual(simulation.tick, 0)
        self.assertEqual(
            before, {key: node.state for key, node in simulation.nodes.items()}
        )
        self.assertEqual(simulation.snapshot()["online"], 31)
        self.assertGreater(simulation.snapshot()["stale_fingers"], 0)
        self.assertFalse(simulation.snapshot()["converged"])
        with self.assertRaises(LookupFailed):
            simulation.nodes[0].find_successor(8)
        self.assertEqual(sum(e["kind"] == "crash" for e in simulation.events), 1)

    def test_periodic_checks_repair_successor_and_all_fingers(self):
        simulation = self.simulation
        simulation.crash(8)
        for _ in range(16):
            simulation.advance()
            if simulation.snapshot()["converged"]:
                break
        snapshot = simulation.snapshot()
        self.assertTrue(snapshot["converged"])
        self.assertEqual(snapshot["stale_fingers"], 0)
        self.assertGreater(snapshot["repairs"], 0)
        self.assertEqual(simulation.nodes[0].state.successor.id, 16)
        self.assertNotIn(8, simulation.last_checks)
        self.assertEqual(snapshot["checks"], simulation.tick * 31 * 3)
        for key, node in simulation.nodes.items():
            if key == 8:
                continue
            for identifier in range(256):
                self.assertEqual(
                    node.find_successor(identifier),
                    simulation._expected_owner(identifier),
                )

    def test_restore_and_reset(self):
        simulation = self.simulation
        simulation.crash(8)
        for _ in range(10):
            simulation.advance()
        simulation.restore(8)
        simulation.restore(8)
        for _ in range(24):
            simulation.advance()
            if simulation.snapshot()["converged"]:
                break
        self.assertTrue(simulation.snapshot()["converged"])
        simulation.configure(paused=True, interval=2)
        simulation.reset()
        snapshot = simulation.snapshot()
        self.assertEqual(snapshot["online"], 32)
        self.assertEqual(snapshot["tick"], 0)
        self.assertEqual(snapshot["repairs"], 0)
        self.assertEqual(snapshot["checks"], 0)
        self.assertFalse(snapshot["paused"])
        self.assertEqual(snapshot["rpc_calls"], {})

    def test_snapshots_are_observations_and_do_not_repair(self):
        simulation = self.simulation
        simulation.crash(8)
        before = {key: node.state for key, node in simulation.nodes.items()}
        calls = dict(simulation.transport.calls)
        for _ in range(5):
            simulation.snapshot()
        self.assertEqual(
            before, {key: node.state for key, node in simulation.nodes.items()}
        )
        self.assertEqual(dict(simulation.transport.calls), calls)

    def test_all_nodes_offline_and_invalid_inputs(self):
        simulation = self.simulation
        for identifier in simulation.nodes:
            simulation.crash(identifier)
        simulation.advance()
        snapshot = simulation.snapshot()
        self.assertEqual(snapshot["online"], 0)
        self.assertFalse(snapshot["converged"])
        self.assertEqual(snapshot["total_fingers"], 0)
        self.assertEqual(snapshot["checks"], 0)
        for bad in (True, None, -1, 3, "8"):
            with self.assertRaises(ValueError):
                simulation.crash(bad)
        with self.assertRaises(ValueError):
            simulation.configure(paused="false")
        with self.assertRaises(ValueError):
            simulation.configure(interval=-1)

    def test_scheduler_pause_and_tick_without_sleeps(self):
        from visualizer.__main__ import VisualizerServer

        # Exercise the actual scheduler method without binding a socket.
        host = type("Host", (), {})()
        host.simulation = self.simulation
        host.next_tick = 0
        with patch("visualizer.__main__.time.monotonic", return_value=5):
            VisualizerServer.service_actions(host)
            self.assertEqual(self.simulation.tick, 1)
            self.assertEqual(host.next_tick, 6)
        self.simulation.configure(paused=True)
        with patch("visualizer.__main__.time.monotonic", return_value=9):
            VisualizerServer.service_actions(host)
        self.assertEqual(self.simulation.tick, 1)
        self.simulation.advance()
        self.assertEqual(self.simulation.tick, 2)
