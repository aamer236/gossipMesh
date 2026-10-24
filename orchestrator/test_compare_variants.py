import csv
import http.client
import http.server
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator import compare_variants as bench


class ComparisonTests(unittest.TestCase):
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
