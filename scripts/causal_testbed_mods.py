#!/usr/bin/env python3
#
# This file is a part of the Raknetify project, licensed under MIT.

"""Discover and resolve the server-side subset of a modpack."""

from __future__ import annotations

import hashlib
import io
import json
import tomllib
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


DEFAULT_ROOT_MODS = (
    "connector",
    "fabric_api",
    "connectorextras",
    "bandwidthoptimizer",
    "create",
    "create_bb",
    "touhou_little_maid",
    "muhc",
)

BUILTIN_MODS = frozenset(
    {
        "fabricloader",
        "forge",
        "java",
        "minecraft",
        "neoforge",
    }
)


class TestbedError(RuntimeError):
    pass


@dataclass(frozen=True)
class RequiredDependency:
    mod_id: str
    source_mod_id: str


@dataclass
class ModJar:
    path: Path
    sha256: str
    mod_ids: set[str] = field(default_factory=set)
    required_dependencies: list[RequiredDependency] = field(default_factory=list)
    client_only_mod_ids: set[str] = field(default_factory=set)

    def display(self) -> str:
        return f"{self.path.name} ({self.sha256[:12]})"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _required_neoforge_dependency(dependency: dict[str, Any]) -> bool:
    side = str(dependency.get("side", "BOTH")).upper()
    if side == "CLIENT":
        return False
    dependency_type = dependency.get("type")
    if dependency_type is not None:
        return str(dependency_type).lower() == "required"
    return dependency.get("mandatory") is True


def _parse_toml_metadata(raw: bytes, result: ModJar) -> None:
    metadata = tomllib.loads(raw.decode("utf-8-sig"))
    local_ids = {
        str(mod["modId"])
        for mod in metadata.get("mods", ())
        if isinstance(mod, dict) and mod.get("modId")
    }
    result.mod_ids.update(local_ids)

    dependency_tables = metadata.get("dependencies", {})
    if not isinstance(dependency_tables, dict):
        return
    for source_mod_id, dependencies in dependency_tables.items():
        if not isinstance(dependencies, list):
            continue
        for dependency in dependencies:
            if not isinstance(dependency, dict):
                continue
            mod_id = dependency.get("modId")
            if mod_id and _required_neoforge_dependency(dependency):
                result.required_dependencies.append(
                    RequiredDependency(str(mod_id), str(source_mod_id))
                )


def _parse_fabric_metadata(raw: bytes, result: ModJar) -> None:
    metadata = json.loads(raw.decode("utf-8-sig"))
    mod_id = metadata.get("id")
    if not mod_id:
        return
    mod_id = str(mod_id)
    result.mod_ids.add(mod_id)
    if str(metadata.get("environment", "*")).lower() == "client":
        result.client_only_mod_ids.add(mod_id)
    dependencies = metadata.get("depends", {})
    if isinstance(dependencies, dict):
        for dependency_id in dependencies:
            result.required_dependencies.append(
                RequiredDependency(str(dependency_id), mod_id)
            )


def _parse_archive(
    archive: zipfile.ZipFile,
    result: ModJar,
    *,
    nested_depth: int = 0,
) -> None:
    names = archive.namelist()
    for name in names:
        normalized = name.replace("\\", "/")
        if normalized in {
            "META-INF/neoforge.mods.toml",
            "META-INF/mods.toml",
        }:
            _parse_toml_metadata(archive.read(name), result)
        elif normalized == "fabric.mod.json":
            _parse_fabric_metadata(archive.read(name), result)

    if nested_depth >= 2:
        return
    for name in names:
        normalized = name.replace("\\", "/")
        if not (
            normalized.startswith("META-INF/jarjar/")
            and normalized.lower().endswith(".jar")
        ):
            continue
        try:
            with zipfile.ZipFile(io.BytesIO(archive.read(name))) as nested:
                _parse_archive(nested, result, nested_depth=nested_depth + 1)
        except (OSError, zipfile.BadZipFile):
            continue


def inspect_jar(path: Path) -> ModJar:
    result = ModJar(path=path.resolve(), sha256=sha256(path))
    try:
        with zipfile.ZipFile(path) as archive:
            _parse_archive(archive, result)
    except (
        json.JSONDecodeError,
        OSError,
        tomllib.TOMLDecodeError,
        UnicodeDecodeError,
        zipfile.BadZipFile,
    ) as exc:
        raise TestbedError(f"cannot inspect mod jar {path}: {exc}") from exc
    return result


def scan_mod_directory(path: Path) -> list[ModJar]:
    if not path.is_dir():
        raise TestbedError(f"client mods directory does not exist: {path}")
    jars = []
    for jar_path in sorted(path.rglob("*.jar"), key=lambda item: str(item).casefold()):
        inspected = inspect_jar(jar_path)
        if inspected.mod_ids:
            jars.append(inspected)
    return jars


def provider_index(jars: Iterable[ModJar]) -> dict[str, list[ModJar]]:
    providers: dict[str, list[ModJar]] = {}
    for jar in jars:
        for mod_id in jar.mod_ids:
            providers.setdefault(mod_id, []).append(jar)
    return providers


def choose_provider(mod_id: str, providers: dict[str, list[ModJar]]) -> ModJar:
    candidates = providers.get(mod_id, [])
    if not candidates:
        raise TestbedError(f"required server mod has no provider in client pack: {mod_id}")

    hashes = {candidate.sha256 for candidate in candidates}
    if len(hashes) != 1:
        formatted = "\n  ".join(candidate.display() for candidate in candidates)
        raise TestbedError(
            f"conflicting providers for mod id {mod_id}:\n  {formatted}"
        )
    return sorted(candidates, key=lambda candidate: str(candidate.path).casefold())[0]


def resolve_server_mods(jars: Iterable[ModJar], roots: Iterable[str]) -> list[ModJar]:
    index = provider_index(jars)
    selected_by_hash: dict[str, ModJar] = {}
    pending = list(dict.fromkeys(roots))
    visited: set[str] = set()

    while pending:
        mod_id = pending.pop(0)
        if mod_id in visited or mod_id in BUILTIN_MODS:
            continue
        visited.add(mod_id)
        provider = choose_provider(mod_id, index)
        if mod_id in provider.client_only_mod_ids:
            raise TestbedError(f"root/required mod is client-only: {mod_id}")
        selected_by_hash.setdefault(provider.sha256, provider)
        for dependency in provider.required_dependencies:
            if dependency.mod_id not in visited:
                pending.append(dependency.mod_id)

    return sorted(selected_by_hash.values(), key=lambda jar: jar.path.name.casefold())
