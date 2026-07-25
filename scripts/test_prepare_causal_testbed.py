#!/usr/bin/env python3
#
# This file is a part of the Raknetify project, licensed under MIT.

import io
import json
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

from scripts.prepare_causal_testbed import (
    DEFAULT_ROOT_MODS,
    TestbedError,
    inspect_jar,
    prepare_testbed,
    resolve_server_mods,
    validate_paths,
)


def neoforge_metadata(mod_id, dependencies=()):
    lines = [
        'modLoader="javafml"',
        'loaderVersion="[1,)"',
        'license="MIT"',
        "[[mods]]",
        f'modId="{mod_id}"',
        'version="1.0.0"',
    ]
    for dependency_id, side in dependencies:
        lines += [
            f"[[dependencies.{mod_id}]]",
            f'modId="{dependency_id}"',
            'type="required"',
            f'side="{side}"',
        ]
    return "\n".join(lines).encode()


def jar_bytes(mod_id, dependencies=()):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(
            "META-INF/neoforge.mods.toml",
            neoforge_metadata(mod_id, dependencies),
        )
    return output.getvalue()


def write_jar(path, mod_id, dependencies=(), nested=()):
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "META-INF/neoforge.mods.toml",
            neoforge_metadata(mod_id, dependencies),
        )
        for name, nested_bytes in nested:
            archive.writestr(f"META-INF/jarjar/{name}.jar", nested_bytes)


class PrepareCausalTestbedTest(unittest.TestCase):

    def test_direct_cli_entrypoint_imports_outside_repository_cwd(self):
        script = Path(__file__).with_name("prepare_causal_testbed.py").resolve()
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [sys.executable, str(script), "--help"],
                cwd=directory,
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("--server-template", result.stdout)

    def test_resolver_discovers_nested_provider_and_skips_client_dependency(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_jar(
                root / "create.jar",
                "create",
                (("ponder", "BOTH"), ("flywheel", "CLIENT")),
                (("ponder", jar_bytes("ponder")),),
            )
            inspected = inspect_jar(root / "create.jar")
            self.assertEqual({"create", "ponder"}, inspected.mod_ids)
            self.assertEqual(
                [inspected],
                resolve_server_mods([inspected], ["create"]),
            )

    def test_conflicting_providers_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_jar(root / "one.jar", "duplicate")
            with zipfile.ZipFile(root / "two.jar", "w") as archive:
                archive.writestr(
                    "META-INF/neoforge.mods.toml",
                    neoforge_metadata("duplicate") + b"\n# different",
                )
            with self.assertRaisesRegex(TestbedError, "conflicting providers"):
                resolve_server_mods(
                    [inspect_jar(root / "one.jar"), inspect_jar(root / "two.jar")],
                    ["duplicate"],
                )

    def test_output_inside_template_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            template = root / "template"
            template.mkdir()
            raknetify = root / "raknetify.jar"
            raknetify.write_bytes(b"jar")
            with self.assertRaisesRegex(TestbedError, "descendant"):
                validate_paths(
                    template,
                    raknetify,
                    template / "nested-output",
                    replace=False,
                )

    def test_prepares_isolated_server_and_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            template = root / "template"
            mods = root / "client-mods"
            output = root / "testbed"
            template.mkdir()
            mods.mkdir()
            (template / "libraries" / "cpw" / "mods").mkdir(parents=True)
            (template / "libraries" / "keep.txt").write_text("launcher")
            (template / "libraries" / "cpw" / "mods" / "keep.jar").write_text(
                "bootstrap"
            )
            (template / "mods").mkdir()
            (template / "mods" / "old.jar").write_text("old")
            (template / "logs").mkdir()
            (template / "world").mkdir()
            (template / "run.bat").write_text("java @user_jvm_args.txt")
            (template / "server.properties").write_text(
                "server-port=25565\nonline-mode=true\n"
            )

            write_jar(mods / "connector.jar", "connector")
            write_jar(mods / "fabric-api.jar", "fabric_api")
            write_jar(mods / "connector-extras.jar", "connectorextras")
            write_jar(mods / "bandwidth.jar", "bandwidthoptimizer")
            write_jar(mods / "create.jar", "create")
            # Byte-identical duplicate providers are intentionally allowed.
            duplicate = jar_bytes("create_bb")
            (mods / "bogie-a.jar").write_bytes(duplicate)
            (mods / "bogie-b.jar").write_bytes(duplicate)
            write_jar(mods / "maid.jar", "touhou_little_maid")
            write_jar(mods / "maid-extension.jar", "muhc")
            raknetify = root / "raknetify.jar"
            with zipfile.ZipFile(raknetify, "w") as archive:
                archive.writestr(
                    "fabric.mod.json",
                    json.dumps(
                        {
                            "schemaVersion": 1,
                            "id": "raknetify",
                            "version": "test",
                            "environment": "*",
                            "depends": {"fabricloader": ">=0.15.2"},
                        }
                    ),
                )

            manifest = prepare_testbed(
                template,
                mods,
                raknetify,
                output,
                DEFAULT_ROOT_MODS,
                25576,
            )

            self.assertTrue((output / "libraries" / "keep.txt").is_file())
            self.assertTrue(
                (output / "libraries" / "cpw" / "mods" / "keep.jar").is_file()
            )
            self.assertFalse((output / "mods" / "old.jar").exists())
            self.assertFalse((output / "world").exists())
            self.assertEqual(9, len(manifest["mods"]))
            properties = (output / "server.properties").read_text()
            self.assertIn("server-ip=127.0.0.1", properties)
            self.assertIn("server-port=25576", properties)
            arguments = (output / "user_jvm_args.txt").read_text()
            self.assertIn("-Draknetify.metricsJsonl=true", arguments)
            self.assertNotIn("adaptiveTransport", arguments)
            harness = output / manifest["causal_harness"]["datapack"]
            self.assertTrue((harness / "pack.mcmeta").is_file())
            tick = (
                harness
                / "data"
                / "raknetify_causal"
                / "function"
                / "tick.mcfunction"
            ).read_text()
            self.assertIn("rk_step=100..", tick)
            self.assertEqual(
                "/trigger rk_causal set 1",
                manifest["causal_harness"]["start_command"],
            )


if __name__ == "__main__":
    unittest.main()
