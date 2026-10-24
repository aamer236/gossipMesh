import contextlib
import io
import json
import sys
import tempfile
import unittest
from unittest.mock import patch

from orchestrator import runner


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.live = [
            {"port": 8000, "id": 0, "addr": "a:8000", "m": 3},
            {"port": 8001, "id": 4, "addr": "b:8000", "m": 3},
        ]
        self.states = []
        for index, node in enumerate(self.live):
            other = self.live[1 - index]
            peer = lambda n: {"id": str(n["id"]), "addr": n["addr"]}
            self.states.append(
                {
                    "id": str(node["id"]),
                    "addr": node["addr"],
                    "m": 3,
                    "r": 4,
                    "successor": peer(other),
                    "predecessor": peer(other),
                    "successor_list": [peer(other)],
                    "fingers": [
                        dict(
                            index=i,
                            **peer(
                                next(
                                    (
                                        v
                                        for v in self.live
                                        if v["id"] >= (node["id"] + (1 << i)) % 8
                                    ),
                                    self.live[0],
                                )
                            ),
                        )
                        for i in range(3)
                    ],
                }
            )

    def test_convergence_requires_every_invariant(self):
        self.assertTrue(runner.ring_converged(self.states))
        for field, wrong in [
            ("fingers", []),
            ("successor_list", []),
            ("predecessor", None),
            ("successor", None),
        ]:
            with self.subTest(field=field):
                states = [dict(state) for state in self.states]
                states[0][field] = wrong
                self.assertFalse(runner.ring_converged(states))

    def test_wait_observes_convergence(self):
        with (
            patch.object(
                runner,
                "get_json",
                side_effect=lambda url, **kw: self.states[0 if ":8000/" in url else 1],
            ),
            patch.object(runner.time, "sleep"),
        ):
            self.assertTrue(
                runner.wait_for_ring_stabilization([8000, 8001], max_seconds=1)
            )

    def test_wait_times_out_on_incomplete_tables(self):
        self.states[0]["fingers"] = []
        with (
            patch.object(
                runner,
                "get_json",
                side_effect=lambda url, **kw: self.states[0 if ":8000/" in url else 1],
            ),
            patch.object(runner.time, "sleep"),
            self.assertRaises(TimeoutError),
        ):
            runner.wait_for_ring_stabilization([8000, 8001], max_seconds=0.01)

    def measure(self, reply, max_seconds=0.03):
        with (
            patch.object(runner, "inject_hard_crash"),
            patch.object(runner, "get_live_nodes", return_value=self.live),
            patch.object(runner, "get_json", side_effect=reply),
            patch.object(runner.time, "sleep"),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            return runner.measure_recovery_time(
                "crashed", 8002, [8000, 8001], max_seconds=max_seconds
            )

    def test_wrong_owners_never_count_as_recovery(self):
        result = self.measure(
            lambda *a, **kw: {
                "success": True,
                "successor": {"id": 999, "addr": "wrong"},
            }
        )
        self.assertFalse(result["recovered"])
        self.assertIsNone(result["time_sec"])
        self.assertEqual(result["correctness_pct"], 0)

    def test_correct_id_wrong_address_rejected(self):
        def reply(url, **kwargs):
            key = int(url.rsplit("=", 1)[1])
            return {
                "success": True,
                "successor": {
                    "id": runner.oracle_successor(self.live, key, 3),
                    "addr": "wrong",
                },
            }

        self.assertFalse(self.measure(reply)["recovered"])

    def test_recovery_samples_every_survivor_and_checks_owners(self):
        ports = set()

        def reply(url, **kwargs):
            ports.add(int(url.split(":")[2].split("/")[0]))
            key = int(url.rsplit("=", 1)[1])
            expected_id = runner.oracle_successor(self.live, key, 3)
            owner = next(node for node in self.live if node["id"] == expected_id)
            return {"success": True, "successor": owner}

        result = self.measure(reply, max_seconds=1)
        self.assertTrue(result["recovered"])
        self.assertEqual(result["correctness_pct"], 100)
        self.assertEqual(ports, {8000, 8001})

    def test_missing_survivor_blocks_recovery(self):
        with (
            patch.object(runner, "inject_hard_crash"),
            patch.object(runner, "get_live_nodes", return_value=self.live[:1]),
            patch.object(runner.time, "sleep"),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            result = runner.measure_recovery_time(
                "crashed", 8002, [8000, 8001], max_seconds=0.01
            )
            self.assertFalse(result["recovered"])

    def test_main_exports_timeout_without_claiming_success(self):
        summary = {
            "correctness_pct": 100,
            "latency_p50_ms": 1,
            "latency_p95_ms": 2,
            "latency_p99_ms": 3,
        }
        telemetry = {
            "total_msgs_sent": 1,
            "total_bytes_sent": 10,
            "avg_alloc_bytes": 100,
        }
        timeout = {
            "recovered": False,
            "time_sec": None,
            "correctness_pct": 0,
            "queries": 10,
        }
        live = [dict(node, succ={"addr": "peer"}) for node in self.live]
        with (
            tempfile.TemporaryDirectory() as directory,
            contextlib.ExitStack() as stack,
        ):
            stack.enter_context(patch.object(runner, "METRICS_DIR", directory))
            stack.enter_context(
                patch.object(sys, "argv", ["runner.py", "--num-queries", "3"])
            )
            for name in [
                "run_cmd",
                "wait_for_cluster",
                "wait_for_ring_stabilization",
                "inject_gray_failure",
                "clear_gray_failure",
                "restart_node",
            ]:
                stack.enter_context(patch.object(runner, name))
            stack.enter_context(
                patch.object(runner, "get_live_nodes", return_value=live)
            )
            stack.enter_context(
                patch.object(runner, "test_lookups", return_value=summary)
            )
            stack.enter_context(
                patch.object(runner, "measure_recovery_time", return_value=timeout)
            )
            stack.enter_context(
                patch.object(runner, "collect_cluster_metrics", return_value=telemetry)
            )
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            runner.main()
            with open(directory + "/baseline_benchmark_results.json") as stream:
                result = json.load(stream)
            self.assertIsNone(result["recovery_time_sec"])
            self.assertFalse(result["verification_passed"])
            with open(directory + "/baseline_benchmark_results.csv") as stream:
                self.assertIn("TIMEOUT", stream.read())


if __name__ == "__main__":
    unittest.main()
