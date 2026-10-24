import unittest
from unittest.mock import patch

from orchestrator import standard_swim_verify as verifier


class StandardVerifierTests(unittest.TestCase):
    def setUp(self):
        self.peers = [
            {"id": 0, "port": 8000, "addr": "a:8000"},
            {"id": 8, "port": 8001, "addr": "b:8000"},
        ]

    def test_true_flag_does_not_make_wrong_owner_correct(self):
        with patch.object(
            verifier.runner,
            "get_json",
            return_value={
                "success": True,
                "successor": {"id": 99, "addr": "wrong"},
                "hops": 1,
            },
        ):
            samples = verifier.lookup_batch(self.peers, [0, 4, 8, 15])
        self.assertEqual(len(samples), 8)
        self.assertTrue(all(sample["rpc_success"] for sample in samples))
        self.assertFalse(any(sample["correct"] for sample in samples))

    def test_identity_address_and_wraparound_oracle(self):
        def reply(url, **kwargs):
            key = int(url.rsplit("=", 1)[1])
            expected = self.peers[1] if 0 < key <= 8 else self.peers[0]
            return {"success": True, "successor": expected, "hops": 1}

        with patch.object(verifier.runner, "get_json", side_effect=reply):
            samples = verifier.lookup_batch(self.peers, [0, 4, 8, 15])
        self.assertTrue(all(sample["correct"] for sample in samples))
        self.assertEqual(verifier.summarize(samples)["correctness_pct"], 100)

    def test_wrong_endpoint_is_incorrect(self):
        with patch.object(
            verifier.runner,
            "get_json",
            return_value={
                "success": True,
                "successor": {"id": 0, "addr": "bad"},
                "hops": 0,
            },
        ):
            self.assertFalse(
                any(
                    sample["correct"]
                    for sample in verifier.lookup_batch(self.peers, [0])
                )
            )

    def test_nearest_rank_and_empty_summary(self):
        self.assertEqual(verifier.percentile(list(range(1, 101)), 0.95), 95)
        self.assertIsNone(verifier.percentile([], 0.95))
        self.assertIsNone(verifier.summarize([])["correctness_pct"])


if __name__ == "__main__":
    unittest.main()
