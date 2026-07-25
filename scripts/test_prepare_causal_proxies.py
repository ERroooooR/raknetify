#!/usr/bin/env python3
#
# This file is a part of the Raknetify project, licensed under MIT.

import json
import tempfile
import unittest
from pathlib import Path

from scripts.prepare_causal_proxies import (
    ProxyTestbedError,
    prepare_proxies,
)


class PrepareCausalProxiesTest(unittest.TestCase):

    def test_prepares_both_proxies_with_pinned_causal_arguments(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifacts = {}
            for name in (
                "velocity.jar",
                "velocity-plugin.jar",
                "bungee.jar",
                "bungee-plugin.jar",
            ):
                path = root / name
                path.write_bytes(("artifact-" + name).encode())
                artifacts[name] = path

            output = root / "proxies"
            manifest = prepare_proxies(
                artifacts["velocity.jar"],
                artifacts["velocity-plugin.jar"],
                artifacts["bungee.jar"],
                artifacts["bungee-plugin.jar"],
                output,
                {
                    "causal_a": "127.0.0.1:25576",
                    "causal_b": "127.0.0.1:25579",
                },
                25577,
                25578,
            )

            self.assertEqual(
                {
                    "causal_a": "127.0.0.1:25576",
                    "causal_b": "127.0.0.1:25579",
                },
                manifest["backends"],
            )
            self.assertTrue((output / "velocity" / "plugins" / "raknetify.jar").is_file())
            self.assertTrue((output / "bungee" / "plugins" / "raknetify.jar").is_file())
            velocity = (output / "velocity" / "velocity.toml").read_text()
            self.assertIn('bind = "127.0.0.1:25577"', velocity)
            self.assertIn('causal_a = "127.0.0.1:25576"', velocity)
            self.assertIn('causal_b = "127.0.0.1:25579"', velocity)
            self.assertIn('try = ["causal_a"]', velocity)
            bungee = (output / "bungee" / "config.yml").read_text()
            self.assertIn("host: 127.0.0.1:25578", bungee)
            self.assertIn("address: 127.0.0.1:25576", bungee)
            self.assertIn("address: 127.0.0.1:25579", bungee)
            for proxy in ("velocity", "bungee"):
                run_file = (output / proxy / "run-causal-proxy.bat").read_text()
                self.assertIn("-Draknetify.metricsJsonl=true", run_file)
                self.assertIn("-Draknetify.protocolVersion=12", run_file)
                self.assertNotIn("adaptiveTransport", run_file)

            recorded = json.loads((output / ".causal-proxies.json").read_text())
            self.assertEqual(manifest, recorded)

    def test_refuses_to_replace_unowned_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "artifact.jar"
            artifact.write_bytes(b"artifact")
            output = root / "proxies"
            output.mkdir()
            with self.assertRaisesRegex(ProxyTestbedError, "refusing to replace"):
                prepare_proxies(
                    artifact,
                    artifact,
                    artifact,
                    artifact,
                    output,
                    {"causal": "127.0.0.1:25576"},
                    25577,
                    25578,
                    replace=True,
                )


if __name__ == "__main__":
    unittest.main()
