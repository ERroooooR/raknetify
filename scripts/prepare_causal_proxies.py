#!/usr/bin/env python3
#
# This file is a part of the Raknetify project, licensed under MIT.

"""Prepare isolated Velocity and Bungee causal integration proxies."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
from pathlib import Path
from typing import Any


SENTINEL_NAME = ".causal-proxies.json"


class ProxyTestbedError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare isolated Velocity and Bungee causal test proxies."
    )
    parser.add_argument("--velocity-jar", required=True, type=Path)
    parser.add_argument("--velocity-plugin", required=True, type=Path)
    parser.add_argument("--bungee-jar", required=True, type=Path)
    parser.add_argument("--bungee-plugin", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--backend",
        action="append",
        default=[],
        metavar="NAME=HOST:PORT",
        help=(
            "named backend; repeat for server-switch tests "
            "(default: causal=127.0.0.1:25576)"
        ),
    )
    parser.add_argument("--velocity-port", type=int, default=25577)
    parser.add_argument("--bungee-port", type=int, default=25578)
    parser.add_argument(
        "--replace",
        action="store_true",
        help="replace an existing directory only when it has our sentinel",
    )
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def checked_file(path: Path, label: str) -> Path:
    resolved = path.resolve()
    if not resolved.is_file():
        raise ProxyTestbedError(f"{label} does not exist: {resolved}")
    return resolved


def checked_port(port: int, label: str) -> int:
    if not 1 <= port <= 65535:
        raise ProxyTestbedError(f"invalid {label}: {port}")
    return port


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def prepare_destination(
    output: Path,
    replace: bool,
    protected_paths: tuple[Path, ...] = (),
) -> Path:
    destination = output.resolve()
    if destination.parent == destination:
        raise ProxyTestbedError("output cannot be a filesystem root")
    for protected_path in protected_paths:
        protected = protected_path.resolve()
        if protected == destination or _is_relative_to(protected, destination):
            raise ProxyTestbedError(
                f"output cannot contain an input artifact: {protected}"
            )
    if destination.exists():
        if not replace:
            raise ProxyTestbedError(
                f"output already exists: {destination}; use --replace for our testbed"
            )
        sentinel = destination / SENTINEL_NAME
        if not sentinel.is_file():
            raise ProxyTestbedError(
                f"refusing to replace directory without {SENTINEL_NAME}: {destination}"
            )
        shutil.rmtree(destination)
    destination.mkdir(parents=True)
    # Write the sentinel before the remaining files so interrupted runs stay replaceable.
    (destination / SENTINEL_NAME).write_text("{}\n", encoding="utf-8")
    return destination


def parse_backends(specifications: list[str]) -> dict[str, str]:
    if not specifications:
        return {"causal": "127.0.0.1:25576"}
    backends: dict[str, str] = {}
    for specification in specifications:
        if "=" not in specification:
            raise ProxyTestbedError(
                f"backend must be NAME=HOST:PORT: {specification}"
            )
        name, address = specification.split("=", 1)
        if not re.fullmatch(r"[a-z0-9_-]+", name):
            raise ProxyTestbedError(
                f"backend name must match [a-z0-9_-]+: {name}"
            )
        if ":" not in address:
            raise ProxyTestbedError(
                f"backend address must be HOST:PORT: {address}"
            )
        try:
            backend_port = int(address.rsplit(":", 1)[1])
        except ValueError as exc:
            raise ProxyTestbedError(
                f"backend address has invalid port: {address}"
            ) from exc
        checked_port(backend_port, f"backend {name} port")
        if name in backends:
            raise ProxyTestbedError(f"duplicate backend name: {name}")
        backends[name] = address
    return backends


def velocity_config(backends: dict[str, str], port: int) -> str:
    server_lines = "\n".join(
        f'{name} = "{address}"' for name, address in backends.items()
    )
    first_backend = next(iter(backends))
    return f"""\
config-version = "2.7"
bind = "127.0.0.1:{port}"
motd = "<#09add3>Raknetify causal Velocity testbed"
show-max-players = 20
online-mode = false
force-key-authentication = false
prevent-client-proxy-connections = false
player-info-forwarding-mode = "NONE"
forwarding-secret-file = "forwarding.secret"
announce-forge = true
kick-existing-players = false
ping-passthrough = "MODS"
sample-players-in-ping = false
enable-player-address-logging = true

[servers]
{server_lines}
try = ["{first_backend}"]

[forced-hosts]

[advanced]
compression-threshold = 256
compression-level = -1
login-ratelimit = 0
connection-timeout = 5000
read-timeout = 30000
haproxy-protocol = false
tcp-fast-open = false
bungee-plugin-message-channel = true
show-ping-requests = false
failover-on-unexpected-server-disconnect = true
announce-proxy-commands = true
log-command-executions = false
log-player-connections = true
accepts-transfers = false
enable-reuse-port = false
command-rate-limit = 50
forward-commands-if-rate-limited = true
kick-after-rate-limited-commands = 0
tab-complete-rate-limit = 10
kick-after-rate-limited-tab-completes = 0

[query]
enabled = false
port = {port}
map = "Velocity"
show-plugins = false
"""


def bungee_config(backends: dict[str, str], port: int) -> str:
    first_backend = next(iter(backends))
    server_lines = "\n".join(
        f"  {name}:\n"
        "    motd: '&bCausal backend'\n"
        f"    address: {address}\n"
        "    restricted: false"
        for name, address in backends.items()
    )
    return f"""\
server_connect_timeout: 5000
listeners:
- query_port: {port}
  motd: '&bRaknetify causal Bungee testbed'
  tab_list: GLOBAL_PING
  query_enabled: false
  proxy_protocol: false
  forced_hosts: {{}}
  ping_passthrough: true
  priorities:
  - {first_backend}
  bind_local_address: true
  host: 127.0.0.1:{port}
  max_players: 20
  tab_size: 60
  force_default_server: true
remote_ping_cache: -1
network_compression_threshold: 256
permissions:
  default: []
log_pings: false
connection_throttle_limit: 3
timeout: 30000
player_limit: 20
ip_forward: false
groups: {{}}
remote_ping_timeout: 5000
connection_throttle: -1
log_commands: false
stats: causal-testbed
online_mode: false
forge_support: true
disabled_commands: []
servers:
{server_lines}
reject_transfers: false
"""


def write_run_file(directory: Path, jar_name: str) -> list[str]:
    arguments = [
        "-Xms512M",
        "-Xmx1G",
        "-Draknetify.metricsJsonl=true",
        "-Draknetify.protocolVersion=12",
    ]
    command = "java " + " ".join(arguments) + f' -jar "{jar_name}"\r\n'
    (directory / "run-causal-proxy.bat").write_text(
        "@echo off\r\n" + command,
        encoding="utf-8",
    )
    return arguments


def copy_artifact(source: Path, destination: Path) -> dict[str, Any]:
    shutil.copy2(source, destination)
    return {
        "file": destination.name,
        "sha256": sha256(source),
        "source": str(source),
    }


def prepare_proxies(
    velocity_jar: Path,
    velocity_plugin: Path,
    bungee_jar: Path,
    bungee_plugin: Path,
    output: Path,
    backends: dict[str, str],
    velocity_port: int,
    bungee_port: int,
    *,
    replace: bool = False,
) -> dict[str, Any]:
    artifacts = {
        "velocity_jar": checked_file(velocity_jar, "Velocity jar"),
        "velocity_plugin": checked_file(velocity_plugin, "Velocity plugin"),
        "bungee_jar": checked_file(bungee_jar, "Bungee jar"),
        "bungee_plugin": checked_file(bungee_plugin, "Bungee plugin"),
    }
    velocity_port = checked_port(velocity_port, "Velocity port")
    bungee_port = checked_port(bungee_port, "Bungee port")
    if velocity_port == bungee_port:
        raise ProxyTestbedError("Velocity and Bungee ports must differ")
    if not backends:
        raise ProxyTestbedError("at least one backend is required")
    # Re-parse the mapping through the CLI validator so API callers receive
    # the same name/address checks as command-line callers.
    backends = parse_backends(
        [f"{name}={address}" for name, address in backends.items()]
    )

    destination = prepare_destination(
        output,
        replace,
        tuple(artifacts.values()),
    )
    velocity = destination / "velocity"
    bungee = destination / "bungee"
    (velocity / "plugins").mkdir(parents=True)
    (bungee / "plugins").mkdir(parents=True)

    copied = {
        "velocity_jar": copy_artifact(
            artifacts["velocity_jar"], velocity / "velocity.jar"
        ),
        "velocity_plugin": copy_artifact(
            artifacts["velocity_plugin"], velocity / "plugins" / "raknetify.jar"
        ),
        "bungee_jar": copy_artifact(
            artifacts["bungee_jar"], bungee / "bungeecord.jar"
        ),
        "bungee_plugin": copy_artifact(
            artifacts["bungee_plugin"], bungee / "plugins" / "raknetify.jar"
        ),
    }
    (velocity / "velocity.toml").write_text(
        velocity_config(backends, velocity_port), encoding="utf-8"
    )
    (velocity / "forwarding.secret").write_text(
        "unused-in-none-mode\n", encoding="utf-8"
    )
    (bungee / "config.yml").write_text(
        bungee_config(backends, bungee_port), encoding="utf-8"
    )
    jvm_arguments = {
        "velocity": write_run_file(velocity, "velocity.jar"),
        "bungee": write_run_file(bungee, "bungeecord.jar"),
    }
    manifest = {
        "format": 1,
        "backends": backends,
        "ports": {
            "velocity": velocity_port,
            "bungee": bungee_port,
        },
        "artifacts": copied,
        "jvm_arguments": jvm_arguments,
    }
    (destination / SENTINEL_NAME).write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    args = parse_args()
    try:
        backends = parse_backends(args.backend)
        manifest = prepare_proxies(
            args.velocity_jar,
            args.velocity_plugin,
            args.bungee_jar,
            args.bungee_plugin,
            args.output,
            backends,
            args.velocity_port,
            args.bungee_port,
            replace=args.replace,
        )
    except ProxyTestbedError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"Prepared causal proxies: {args.output.resolve()}")
    print(
        "Velocity 127.0.0.1:{velocity}, Bungee 127.0.0.1:{bungee}, "
        "backends {backends}".format(
            **manifest["ports"],
            backends=", ".join(
                f"{name}={address}"
                for name, address in manifest["backends"].items()
            ),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
