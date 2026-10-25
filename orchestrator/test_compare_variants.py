import argparse
import contextlib
import csv
import http.client
import http.server
import io
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator import compare_variants as bench


class ComparisonTests(unittest.TestCase):
    def test_configurable_probe_timeouts(self):
        args = self.parse_args("--swim-interval-ms", "1000", "--swim-direct-ms", "250", "--swim-suspicion-ms", "5000", "--rpc-timeout-ms", "1000")
        config = bench.variant_config("chord_standard_swim", args)
        self.assertEqual(config["SWIM_INTERVAL_MS"], "1000")
        self.assertEqual(config["SWIM_DIRECT_TIMEOUT_MS"], "250")
        self.assertEqual(config["RPC_TIMEOUT_MS"], "1000")
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.parse_args("--swim-interval-ms", "1000", "--swim-suspicion-ms", "800")

    def test_large_ring_configuration(self):
        for count in (16, 32, 64):
            args = self.parse_args("--nodes", str(count), "--m", "8", "--rate", "64")
            self.assertEqual(args.nodes, count)
            self.assertEqual(len({i * (1 << args.m) // count for i in range(count)}), count)
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.parse_args("--nodes", "64", "--m", "4", "--rate", "64")

    def parse_args(self, *extra):
        return bench.parse_args(
            ["--chord-binary", "/tmp/chord", "--swim-binary", "/tmp/swim", *extra]
        )

    def test_adaptive_defaults_preserved_in_swim_run_config_only(self):
        args = self.parse_args()
        plain = bench.variant_config("chord", args)
        swim = bench.variant_config("chord_standard_swim", args)
        self.assertFalse(any(key.startswith("ADAPTIVE_") for key in plain))
        self.assertEqual(swim["ADAPTIVE_MAINTENANCE"], "false")
        self.assertEqual(swim["ADAPTIVE_STABLE_AFTER_MS"], "2000")
        self.assertEqual(swim["ADAPTIVE_MAX_STABILIZE_MS"], "5000")
        self.assertEqual(swim["ADAPTIVE_MAX_FIX_FINGERS_MS"], "2000")
        self.assertEqual(swim["ADAPTIVE_MAX_CHECK_PRED_MS"], "5000")

    def test_old_programmatic_namespace_defaults_to_fixed_maintenance(self):
        args = self.parse_args()
        legacy = argparse.Namespace(
            **{
                key: value
                for key, value in vars(args).items()
                if not key.startswith("adaptive_")
                and key != "swim_adaptive_maintenance"
            }
        )
        self.assertEqual(
            bench.variant_config("chord_standard_swim", legacy),
            bench.variant_config("chord_standard_swim", args),
        )

    def test_adaptive_enabled_configuration_has_independent_interval_caps(self):
        args = self.parse_args(
            "--swim-adaptive-maintenance",
            "--stabilize-ms",
            "700",
            "--fix-ms",
            "900",
            "--check-pred-ms",
            "1100",
            "--adaptive-stable-after-ms",
            "1500",
            "--adaptive-max-stabilize-ms",
            "700",
            "--adaptive-max-fix-ms",
            "900",
            "--adaptive-max-check-pred-ms",
            "1100",
        )
        config = bench.variant_config("chord_standard_swim", args)
        self.assertEqual(config["ADAPTIVE_MAINTENANCE"], "true")
        self.assertEqual(config["ADAPTIVE_STABLE_AFTER_MS"], "1500")
        self.assertEqual(config["ADAPTIVE_MAX_STABILIZE_MS"], "700")
        self.assertEqual(config["ADAPTIVE_MAX_FIX_FINGERS_MS"], "900")
        self.assertEqual(config["ADAPTIVE_MAX_CHECK_PRED_MS"], "1100")

    def test_adaptive_validation_checks_matching_intervals_and_stability_period(self):
        for extra in (
            ("--adaptive-stable-after-ms", "0"),
            ("--swim-adaptive-maintenance", "--adaptive-max-stabilize-ms", "199"),
            ("--swim-adaptive-maintenance", "--adaptive-max-fix-ms", "99"),
            (
                "--swim-adaptive-maintenance",
                "--check-pred-ms",
                "900",
                "--adaptive-max-check-pred-ms",
                "899",
            ),
            (
                "--swim-adaptive-maintenance",
                "--stabilize-ms",
                "700",
                "--adaptive-max-check-pred-ms",
                "699",
            ),
        ):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as failure:
                    self.parse_args(*extra)
                self.assertEqual(failure.exception.code, 2)
        # Unused caps need not constrain the fixed-maintenance experiment.
        self.parse_args("--stabilize-ms", "6000")

    def test_process_environment_excludes_inherited_adaptive_policy(self):
        args = self.parse_args("--swim-adaptive-maintenance")
        inherited = {
            "PATH": "/test/bin",
            "SWIM_INTERVAL_MS": "1",
            "ADAPTIVE_MAINTENANCE": "true",
            "ADAPTIVE_MAX_STABILIZE_MS": "99999",
            "ADAPTIVE_UNRECOGNIZED": "on",
        }
        with patch.dict(os.environ, inherited, clear=True):
            plain = bench.process_environment(bench.variant_config("chord", args))
            swim = bench.process_environment(
                bench.variant_config("chord_standard_swim", args)
            )
        self.assertEqual(plain["PATH"], "/test/bin")
        self.assertFalse(any(key.startswith("ADAPTIVE_") for key in plain))
        self.assertEqual(swim["ADAPTIVE_MAX_STABILIZE_MS"], "5000")
        self.assertNotIn("ADAPTIVE_UNRECOGNIZED", swim)
        self.assertEqual(swim["SWIM_INTERVAL_MS"], "200")

    def test_maintenance_telemetry_counters_survive_generic_phase_delta(self):
        self.assertEqual(
            bench.counter_delta(
                [{"counters": {"maintenance_stabilize_runs": 4}}],
                [
                    {
                        "counters": {
                            "maintenance_stabilize_runs": 7,
                            "maintenance_backoff": 2,
                        }
                    }
                ],
                "counters",
            ),
            {"maintenance_backoff": 2, "maintenance_stabilize_runs": 3},
        )

    def test_oracle_includes_gray_peer_but_excludes_crashed_peer(self):
        peers = [
            {"id": n, "port": 8000 + n, "addr": f"peer-{n}"} for n in (0, 4, 8, 12)
        ]
        self.assertEqual(bench.owner_for(peers, 6)["id"], 8)
        self.assertEqual(
            bench.owner_for([p for p in peers if p["id"] != 8], 6)["id"], 12
        )
        self.assertEqual(bench.owner_for(peers, 15)["id"], 0)

    def test_rpc_success_with_wrong_address_is_not_correct(self):
        peer = {"id": 0, "port": 8000, "addr": "good"}
        with patch.object(
            bench.runner,
            "get_json",
            return_value={
                "success": True,
                "successor": {"id": 0, "addr": "wrong"},
                "hops": 1,
            },
        ):
            sample = bench.query(peer, 0, [peer], time.monotonic(), 0)
        self.assertTrue(sample["rpc_success"])
        self.assertFalse(sample["correct"])

    def samples(self):
        return [
            {
                "initiator": node,
                "correct": True,
                "scheduled_sec": second + 0.1,
                "completed_sec": second + 0.2,
            }
            for second in range(4)
            for node in (0, 4)
        ]

    def test_recovery_requires_two_consecutive_per_survivor_cohorts(self):
        samples = self.samples()
        samples[0]["correct"] = False
        self.assertEqual(bench.recovery_time(samples, [0, 4]), 3)
        # Missing node in the second cohort prevents a claimed 2s recovery.
        samples = [
            s
            for s in self.samples()
            if not (s["initiator"] == 4 and 1 <= s["scheduled_sec"] < 2)
        ]
        self.assertEqual(bench.recovery_time(samples, [0, 4]), 4)

    def test_recovery_does_not_backdate_late_replies(self):
        samples = self.samples()
        samples[0]["completed_sec"] = 5.5
        self.assertEqual(bench.recovery_time(samples, [0, 4]), 5.5)
        self.assertIsNone(bench.recovery_time(samples[:2], [0, 4]))

    def test_counter_delta_missing_scrape_is_null(self):
        self.assertIsNone(bench.counter_delta([None], [{"c": {"a": 4}}], "c"))
        self.assertEqual(
            bench.counter_delta([{"c": {"a": 4}}], [{"c": {"a": 7, "b": 2}}], "c"),
            {"a": 3, "b": 2},
        )

    def test_same_offered_schedule_and_counts_for_both_variants(self):
        peers = [{"id": 0}, {"id": 4}]

        def fake_query(peer, key, alive, epoch, scheduled):
            return {"initiator": peer["id"], "key": key, "scheduled_sec": scheduled}

        with patch.object(bench, "query", side_effect=fake_query):
            first = bench.workload(peers, peers, [1, 3], 0.02, 200)
            second = bench.workload(peers, peers, [1, 3], 0.02, 200)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 4)

    def test_csv_uses_same_fields_and_blanks_for_missing_recovery(self):
        phase = {
            "queries": 2,
            "correct": 2,
            "correctness_pct": 100,
            "peer_traffic": {
                "requests": 7,
                "request_body_bytes": 10,
                "response_body_bytes": 20,
            },
        }
        result = {
            "runs": [
                {"variant": variant, "repetition": 0, "phases": {"healthy": phase}}
                for variant in ("chord", "chord_standard_swim")
            ]
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "results.csv"
            bench.write_csv(path, result)
            with path.open() as stream:
                rows = list(csv.DictReader(stream))
        self.assertEqual(len(rows), 2)
        self.assertEqual(set(rows[0]), set(rows[1]))
        self.assertEqual(rows[0]["recovery_time_sec"], "")
        self.assertEqual(rows[1]["peer_body_bytes"], "30")

    def test_quiet_workload_withholds_and_does_not_burst_queries(self):
        epoch = time.monotonic()

        def fake_query(peer, key, alive, epoch, scheduled):
            return {"scheduled_sec": scheduled, "actual_sec": time.monotonic() - epoch}

        with patch.object(bench, "query", side_effect=fake_query):
            samples = bench.workload([{"id": 0}], [], [1], 0.05, 100, epoch, 0.02)
        self.assertEqual(
            [round(s["scheduled_sec"], 3) for s in samples], [0.02, 0.03, 0.04]
        )
        self.assertTrue(all(s["actual_sec"] >= s["scheduled_sec"] for s in samples))

    def test_finer_recovery_cohorts_still_require_every_survivor(self):
        samples = [
            {
                "initiator": node,
                "correct": True,
                "scheduled_sec": at,
                "completed_sec": at + 0.01,
            }
            for at in (0.1, 0.35)
            for node in (0, 4)
        ]
        self.assertEqual(bench.recovery_time(samples, [0, 4], 0.25), 0.5)
        self.assertIsNone(bench.recovery_time(samples[:-1], [0, 4], 0.25))

    def test_routing_observer_reads_state_without_lookup_rpcs(self):
        peer = {"id": 0, "port": 8000, "addr": "a"}
        victim = {"id": 8, "addr": "b"}
        state = {
            "successor": victim,
            "predecessor": None,
            "successor_list": [victim],
            "fingers": [dict(victim, index=2)],
        }
        with (
            patch.object(bench.runner, "get_json", return_value=state) as get,
            patch.object(bench.runner, "ring_converged", return_value=False),
        ):
            observation = bench.routing_observation([peer], victim, time.monotonic())
        self.assertEqual(get.call_args.args[0], "http://127.0.0.1:8000/state")
        self.assertFalse(observation["routing_converged"])
        self.assertEqual(observation["references_to_victim"]["0"]["fingers"], [2])

    def test_routing_convergence_requires_consecutive_snapshots(self):
        observations = [
            {"observed_sec": i / 10, "routing_converged": value}
            for i, value in enumerate([True, False, True, True])
        ]
        self.assertEqual(bench.first_routing_convergence(observations), 0.3)
        self.assertIsNone(bench.first_routing_convergence(observations[:-1]))

    def test_comparison_keeps_missing_phase_and_recovery_null(self):
        rows = bench.comparison_rows(
            [{"variant": "chord", "phases": {"healthy": {"correctness_pct": 100}}}]
        )
        self.assertEqual(len(rows), 12)
        recovery = next(
            r
            for r in rows
            if r["variant"] == "chord" and r["phase"] == "crash_recovery"
        )
        self.assertEqual(recovery["completed_runs"], 0)
        self.assertIsNone(recovery["recovery_time_sec"])


class ProxyTests(unittest.TestCase):
    def test_live_backend_is_delayed_then_recovers_and_payloads_counted(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(body)

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        proxy = bench.PeerProxy(server.server_port)
        try:

            def request():
                connection = http.client.HTTPConnection(
                    "127.0.0.1", proxy.port, timeout=1
                )
                try:
                    connection.request("POST", "/swim/rpc", body=b"{}")
                    reply = connection.getresponse()
                    self.assertEqual(reply.status, 200)
                    self.assertEqual(reply.read(), b"{}")
                finally:
                    connection.close()

            proxy.set_delay(0.04)
            started = time.monotonic()
            request()
            self.assertGreaterEqual(time.monotonic() - started, 0.04)
            proxy.set_delay(0)
            request()
            counts = proxy.snapshot()
            self.assertEqual(counts["requests"], 2)
            self.assertEqual(counts["delayed_requests"], 1)
            self.assertEqual(counts["request_body_bytes"], 4)
            self.assertEqual(counts["response_body_bytes"], 4)
        finally:
            proxy.close()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
        self.assertFalse(proxy.thread.is_alive())


if __name__ == "__main__":
    unittest.main()
