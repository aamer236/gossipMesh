import tracemalloc
import unittest
from unittest.mock import patch

from experiments.standalone import distribution, main, run_case


class StandaloneBenchmarkTests(unittest.TestCase):
    def test_memory_tracer_is_cleaned_up_after_invalid_workload(self):
        with patch(
            "sys.argv", ["standalone", "--sizes", "0", "--seeds", "1", "--trace-memory"]
        ):
            with self.assertRaises(ValueError):
                main()
        self.assertFalse(tracemalloc.is_tracing())

    def test_percentiles_and_empty_distributions(self):
        result = distribution(list(range(1, 101)))
        self.assertEqual(result["mean"], 50.5)
        self.assertEqual(result["median"], 50.5)
        self.assertEqual(result["p95"], 95)
        self.assertEqual(result["p99"], 99)
        self.assertTrue(all(value is None for value in distribution([]).values()))

    def test_raw_samples_overhead_latency_and_recovery(self):
        result = run_case(12, 42, m=8, queries=40, latency=2, failures=2)
        healthy = result["healthy"]
        self.assertEqual(healthy["success_rate"], 1)
        self.assertEqual(healthy["incorrect_owners"], 0)
        self.assertEqual(healthy["failed_lookups"], 0)
        self.assertEqual(len(healthy["samples"]), 40)
        for sample in healthy["samples"]:
            self.assertEqual(sample["owner"], sample["expected_owner"])
            self.assertEqual(sample["latency"], 2 * sample["rpc_count"])
        initial = result["initial_convergence"]
        self.assertTrue(initial["converged"])
        self.assertGreater(initial["trace"][0]["incorrect_fingers"], 0)
        self.assertEqual(initial["trace"][-1]["incorrect_fingers"], 0)
        self.assertEqual(
            initial["overhead"]["stabilization_attempts"], initial["rounds"] * 12
        )
        self.assertGreater(sum(result["join"]["overhead"]["rpc_calls"].values()), 0)
        failure = result["adjacent_failure"]
        self.assertTrue(failure["converged"])
        self.assertGreater(failure["trace"][0]["diagnostics"]["incorrect_fingers"], 0)
        self.assertEqual(failure["trace"][-1]["diagnostics"]["incorrect_fingers"], 0)
        for observed in failure["trace"]:
            sample = observed["lookups"]
            self.assertEqual(
                sample["correct_lookups"]
                + sample["failed_lookups"]
                + sample["incorrect_owners"],
                sample["queries"],
            )
        self.assertGreaterEqual(failure["recovery_time_excluding_probes"], 0)
        self.assertTrue(result["restoration"]["converged"])
        self.assertEqual(result["restoration"]["lookups"]["success_rate"], 1)
        self.assertGreater(result["memory"]["snapshot_json_bytes_total"], 0)
        self.assertEqual(result["memory"]["routing_pointer_slots_per_node"]["mean"], 13)

    def test_repeated_seed_matches_protocol_metrics_and_raw_samples(self):
        a = run_case(8, 7, m=8, queries=20)
        b = run_case(8, 7, m=8, queries=20)
        # Interpreter allocation sizes are observations, not protocol output.
        a.pop("memory")
        b.pop("memory")
        self.assertEqual(a, b)

    def test_two_node_survivor_is_reported_without_invented_recovery(self):
        result = run_case(2, 7, m=4, queries=5, failures=1)
        failure = result["adjacent_failure"]
        self.assertFalse(failure["converged"])
        self.assertIsNone(failure["recovery_rounds"])
        self.assertIsNone(failure["recovery_time_excluding_probes"])
        self.assertGreater(failure["trace"][-1]["lookups"]["failed_lookups"], 0)
        self.assertTrue(result["restoration"]["converged"])

    def test_invalid_workload_parameters(self):
        for arguments in (
            (0, {}),
            (17, {"m": 4}),
            (2, {"queries": 0}),
            (2, {"failures": -1}),
        ):
            with self.assertRaises(ValueError):
                run_case(arguments[0], 1, **arguments[1])
