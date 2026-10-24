import math
import unittest

from experiments.scaling import run_experiment


class ScalingTests(unittest.TestCase):
    def test_fingers_avoid_linear_walk_on_seeded_rings(self):
        for count in (16, 32, 64):
            result = run_experiment(count, m=12, queries=300)
            self.assertEqual(result["success_rate"], 1.0)
            self.assertEqual(result["incorrect_owners"], 0)
            self.assertEqual(result["finger_correctness"], 1.0)
            # Distribution-level checks; not a theorem under adversarial IDs.
            self.assertLess(result["mean_hops"], count / 4)
            self.assertLessEqual(result["p95_hops"], 2 * math.log2(count))
            self.assertLess(result["route_rpcs"], 300 * count / 4)

    def test_large_identifier_space_without_enumeration(self):
        result = run_experiment(5, m=160, queries=30)
        self.assertEqual(result["success_rate"], 1.0)
        self.assertEqual(result["finger_correctness"], 1.0)
