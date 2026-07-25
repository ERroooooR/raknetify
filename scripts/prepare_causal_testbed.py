#!/usr/bin/env python3
#
# This file is a part of the Raknetify project, licensed under MIT.

"""Prepare an isolated NeoForge/Connector server for causal integration tests.

The source modpack remains untouched. Mod providers and their required,
server-side dependencies are discovered from NeoForge/Forge/Fabric metadata,
including metadata stored in jar-in-jar dependencies.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Iterable

if __package__:
    from scripts.causal_testbed_mods import (
        DEFAULT_ROOT_MODS,
        ModJar,
        TestbedError,
        inspect_jar,
        resolve_server_mods,
        scan_mod_directory,
    )
else:
    from causal_testbed_mods import (
        DEFAULT_ROOT_MODS,
        ModJar,
        TestbedError,
        inspect_jar,
        resolve_server_mods,
        scan_mod_directory,
    )

IGNORED_TEMPLATE_ENTRIES = frozenset(
    {
        "bandwidthoptimizer-native",
        "crash-reports",
        "logs",
        "mods",
        "world",
        "world-causal",
    }
)

SENTINEL_NAME = ".causal-testbed.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare an isolated NeoForge/Connector causal test server."
    )
    parser.add_argument(
        "--server-template",
        required=True,
        type=Path,
        help="existing NeoForge server installation used only as a launcher template",
    )
    parser.add_argument(
        "--client-mods",
        required=True,
        type=Path,
        help="mods directory of the affected client pack",
    )
    parser.add_argument(
        "--raknetify-jar",
        required=True,
        type=Path,
        help="newly built Raknetify Fabric all jar",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="new isolated server directory",
    )
    parser.add_argument(
        "--root-mod",
        action="append",
        default=[],
        help="root mod id to include; repeat to override the default compatibility set",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=25576,
        help="loopback TCP/UDP server port (default: 25576)",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="replace an existing directory only when it contains our testbed sentinel",
    )
    return parser.parse_args()


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def validate_paths(
    server_template: Path,
    raknetify_jar: Path,
    output: Path,
    *,
    replace: bool,
) -> tuple[Path, Path, Path]:
    template = server_template.resolve()
    raknetify = raknetify_jar.resolve()
    destination = output.resolve()
    if not template.is_dir():
        raise TestbedError(f"server template does not exist: {template}")
    if not raknetify.is_file():
        raise TestbedError(f"Raknetify jar does not exist: {raknetify}")
    if (
        destination == template
        or _is_relative_to(template, destination)
        or _is_relative_to(destination, template)
    ):
        raise TestbedError(
            "output cannot be the template, its ancestor, or its descendant"
        )
    if destination.exists():
        sentinel = destination / SENTINEL_NAME
        if not replace:
            raise TestbedError(
                f"output already exists: {destination}; use --replace for our testbed"
            )
        if not sentinel.is_file():
            raise TestbedError(
                f"refusing to replace directory without {SENTINEL_NAME}: {destination}"
            )
        shutil.rmtree(destination)
    return template, raknetify, destination


def copy_server_template(template: Path, destination: Path) -> None:
    template = template.resolve()

    def ignore(directory: str, names: list[str]) -> set[str]:
        if Path(directory).resolve() != template:
            return set()
        return {name for name in names if name in IGNORED_TEMPLATE_ENTRIES}

    shutil.copytree(template, destination, ignore=ignore)
    (destination / "mods").mkdir()
    (destination / "logs").mkdir()


def rewrite_server_properties(path: Path, port: int) -> None:
    if not 1 <= port <= 65535:
        raise TestbedError(f"invalid server port: {port}")
    properties: dict[str, str] = {}
    comments: list[str] = []
    if path.exists():
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line or line.lstrip().startswith("#") or "=" not in line:
                comments.append(line)
                continue
            key, value = line.split("=", 1)
            properties[key] = value
    properties.update(
        {
            "enable-query": "false",
            "enable-rcon": "false",
            "level-name": "world-causal",
            "motd": "Raknetify causal multichannel testbed",
            "online-mode": "false",
            "server-ip": "127.0.0.1",
            "server-port": str(port),
            "white-list": "false",
        }
    )
    rendered = comments + [
        f"{key}={properties[key]}" for key in sorted(properties)
    ]
    path.write_text("\n".join(rendered).rstrip() + "\n", encoding="utf-8")


def write_launcher_files(destination: Path) -> list[str]:
    jvm_arguments = [
        "-Xms2G",
        "-Xmx6G",
        "-Draknetify.metricsJsonl=true",
        "-Draknetify.protocolVersion=12",
    ]
    (destination / "user_jvm_args.txt").write_text(
        "\n".join(jvm_arguments) + "\n", encoding="utf-8"
    )
    (destination / "run-causal-testbed.bat").write_text(
        "@echo off\r\n"
        "call run.bat nogui\r\n",
        encoding="utf-8",
    )
    return jvm_arguments


def write_causal_datapack(destination: Path) -> dict[str, Any]:
    datapack = (
        destination
        / "world-causal"
        / "datapacks"
        / "raknetify-causal-harness"
    )
    function_directory = datapack / "data" / "raknetify_causal" / "function"
    tag_directory = datapack / "data" / "minecraft" / "tags" / "function"
    function_directory.mkdir(parents=True)
    tag_directory.mkdir(parents=True)
    (datapack / "pack.mcmeta").write_text(
        json.dumps(
            {
                "pack": {
                    "pack_format": 48,
                    "description": "Raknetify 100-transition causal harness",
                }
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (tag_directory / "load.json").write_text(
        '{"values":["raknetify_causal:load"]}\n', encoding="utf-8"
    )
    (tag_directory / "tick.json").write_text(
        '{"values":["raknetify_causal:tick"]}\n', encoding="utf-8"
    )

    functions = {
        "load": """\
scoreboard objectives add rk_causal trigger
scoreboard objectives add rk_step dummy
scoreboard objectives add rk_wait dummy
execute in minecraft:overworld run forceload add 0 0
execute in minecraft:the_nether run forceload add 0 0
execute in minecraft:overworld run fill -2 99 -2 2 99 2 minecraft:obsidian
execute in minecraft:the_nether run fill -2 99 -2 2 99 2 minecraft:obsidian
""",
        "tick": """\
scoreboard players enable @a rk_causal
execute as @a[scores={rk_causal=1}] run function raknetify_causal:start
execute as @a[scores={rk_causal=2}] run function raknetify_causal:stop
execute as @a[scores={rk_causal=3}] run function raknetify_causal:status
scoreboard players set @a[scores={rk_causal=1..}] rk_causal 0
scoreboard players remove @a[tag=raknetify_causal_active,scores={rk_wait=1..}] rk_wait 1
execute as @a[tag=raknetify_causal_active,tag=raknetify_causal_to_nether,scores={rk_wait=..0,rk_step=..99}] run function raknetify_causal:to_nether
execute as @a[tag=raknetify_causal_active,tag=raknetify_causal_to_overworld,scores={rk_wait=..0,rk_step=..99}] run function raknetify_causal:to_overworld
execute as @a[tag=raknetify_causal_active,scores={rk_step=100..}] run function raknetify_causal:complete
""",
        "start": """\
tag @s add raknetify_causal_active
tag @s add raknetify_causal_to_nether
tag @s remove raknetify_causal_to_overworld
scoreboard players set @s rk_step 0
scoreboard players set @s rk_wait 20
execute in minecraft:overworld run tp @s 0.5 100 0.5
tellraw @s {"text":"[Raknetify] 100-transition causal run started (1 transition/second).","color":"aqua"}
""",
        "stop": """\
tag @s remove raknetify_causal_active
tag @s remove raknetify_causal_to_nether
tag @s remove raknetify_causal_to_overworld
tellraw @s [{"text":"[Raknetify] Causal run stopped at transition ","color":"yellow"},{"score":{"name":"@s","objective":"rk_step"}}]
""",
        "status": """\
tellraw @s [{"text":"[Raknetify] Causal transition ","color":"aqua"},{"score":{"name":"@s","objective":"rk_step"}},{"text":"/100"}]
""",
        "to_nether": """\
execute in minecraft:the_nether run tp @s 0.5 100 0.5
tag @s remove raknetify_causal_to_nether
tag @s add raknetify_causal_to_overworld
scoreboard players add @s rk_step 1
scoreboard players set @s rk_wait 20
function raknetify_causal:progress
""",
        "to_overworld": """\
execute in minecraft:overworld run tp @s 0.5 100 0.5
tag @s remove raknetify_causal_to_overworld
tag @s add raknetify_causal_to_nether
scoreboard players add @s rk_step 1
scoreboard players set @s rk_wait 20
function raknetify_causal:progress
""",
        "progress": "\n".join(
            f'execute if score @s rk_step matches {step} run tellraw @s '
            f'{{"text":"[Raknetify] {step}/100 causal transitions complete.",'
            '"color":"aqua"}'
            for step in range(10, 100, 10)
        )
        + "\n",
        "complete": """\
tag @s remove raknetify_causal_active
tag @s remove raknetify_causal_to_nether
tag @s remove raknetify_causal_to_overworld
tellraw @s {"text":"[Raknetify] 100/100 complete. Stay connected for at least 10 seconds before collecting metrics.","color":"green","bold":true}
""",
    }
    for name, content in functions.items():
        (function_directory / f"{name}.mcfunction").write_text(
            content, encoding="utf-8"
        )
    return {
        "datapack": str(datapack.relative_to(destination)),
        "transitions": 100,
        "interval_ticks": 20,
        "start_command": "/trigger rk_causal set 1",
        "stop_command": "/trigger rk_causal set 2",
        "status_command": "/trigger rk_causal set 3",
    }


def copy_mods(
    selected: Iterable[ModJar],
    raknetify: ModJar,
    destination: Path,
) -> list[dict[str, Any]]:
    mods_directory = destination / "mods"
    manifest: list[dict[str, Any]] = []
    all_jars = list(selected) + [raknetify]
    seen_hashes: set[str] = set()
    for jar in all_jars:
        if jar.sha256 in seen_hashes:
            continue
        seen_hashes.add(jar.sha256)
        target = mods_directory / jar.path.name
        if target.exists():
            raise TestbedError(f"two selected jars have the same filename: {target.name}")
        shutil.copy2(jar.path, target)
        manifest.append(
            {
                "file": target.name,
                "sha256": jar.sha256,
                "mod_ids": sorted(jar.mod_ids),
                "source": str(jar.path),
            }
        )
    return manifest


def prepare_testbed(
    server_template: Path,
    client_mods: Path,
    raknetify_jar: Path,
    output: Path,
    roots: Iterable[str],
    port: int,
    *,
    replace: bool = False,
) -> dict[str, Any]:
    roots = tuple(roots)
    template, raknetify_path, destination = validate_paths(
        server_template, raknetify_jar, output, replace=replace
    )
    scanned = scan_mod_directory(client_mods.resolve())
    selected = resolve_server_mods(scanned, roots)
    raknetify = inspect_jar(raknetify_path)
    if "raknetify" not in raknetify.mod_ids:
        raise TestbedError(
            f"provided Raknetify jar does not declare the raknetify mod id: {raknetify_path}"
        )

    copy_server_template(template, destination)
    # Mark the directory immediately so an interrupted run can be replaced safely.
    (destination / SENTINEL_NAME).write_text("{}\n", encoding="utf-8")
    rewrite_server_properties(destination / "server.properties", port)
    jvm_arguments = write_launcher_files(destination)
    harness = write_causal_datapack(destination)
    mods = copy_mods(selected, raknetify, destination)
    manifest = {
        "format": 1,
        "client_mods": str(client_mods.resolve()),
        "jvm_arguments": jvm_arguments,
        "causal_harness": harness,
        "mods": mods,
        "port": port,
        "root_mods": list(roots),
        "server_template": str(template),
    }
    (destination / SENTINEL_NAME).write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    args = parse_args()
    roots = tuple(args.root_mod) if args.root_mod else DEFAULT_ROOT_MODS
    try:
        manifest = prepare_testbed(
            args.server_template,
            args.client_mods,
            args.raknetify_jar,
            args.output,
            roots,
            args.port,
            replace=args.replace,
        )
    except TestbedError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"Prepared causal testbed: {args.output.resolve()}")
    print(f"Selected {len(manifest['mods'])} jars on loopback port {args.port}:")
    for mod in manifest["mods"]:
        print(f"  {mod['file']} [{', '.join(mod['mod_ids'])}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
