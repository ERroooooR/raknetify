#!/usr/bin/env python3
#
# This file is a part of the Raknetify project, licensed under MIT.

import json
import tempfile
import unittest
from pathlib import Path

from scripts.verify_causal_integration import (
    load_final_samples,
    scan_logs,
    verify_metrics,
)


def sample(**overrides):
    value = {
        "connection": "cafebabe",
        "causal_fences_started": 100,
        "causal_fences_completed": 100,
        "causal_fences_failed": 0,
        "causal_inbound_fences_completed": 0,
        "causal_outbound_epoch": 100,
        "causal_inbound_epoch": 0,
        "causal_stale_frames_dropped": 0,
        "causal_future_frames_pending": 0,
        "causal_future_bytes_pending": 0,
        "causal_atomic_bundles_outbound": 4,
        "causal_atomic_bundles_inbound": 0,
    }
    value.update(overrides)
    return value


class VerifyCausalIntegrationTest(unittest.TestCase):

    def test_complete_run_passes(self):
        result = verify_metrics(
            {"cafebabe": sample()},
            minimum_transitions=100,
            require_bundles=True,
            initial_errors=[],
        )
        self.assertEqual([], result.errors)
        self.assertEqual(100, result.outbound_transitions)

    def test_incomplete_or_stale_run_fails(self):
        result = verify_metrics(
            {
                "cafebabe": sample(
                    causal_fences_completed=99,
                    causal_outbound_epoch=99,
                    causal_stale_frames_dropped=1,
                )
            },
            minimum_transitions=100,
            require_bundles=False,
            initial_errors=[],
        )
        self.assertTrue(
            any("incomplete fences" in error for error in result.errors)
        )
        self.assertTrue(
            any("stale gameplay frames" in error for error in result.errors)
        )

    def test_loader_uses_final_sample_and_log_scanner_finds_protocol_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metrics = root / "metrics.jsonl"
            metrics.write_text(
                json.dumps(sample(causal_fences_completed=1))
                + "\n"
                + json.dumps(sample())
                + "\n",
                encoding="utf-8",
            )
            loaded, errors = load_final_samples(metrics)
            self.assertEqual([], errors)
            self.assertEqual(100, loaded["cafebabe"]["causal_fences_completed"])

            log = root / "latest.log"
            log.write_text(
                "Internal Exception: Received unknown packet id 895\n",
                encoding="utf-8",
            )
            log_errors = scan_logs([log])
            self.assertEqual(1, len(log_errors))


if __name__ == "__main__":
    unittest.main()
