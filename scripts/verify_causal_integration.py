#!/usr/bin/env python3
#
# This file is a part of the Raknetify project, licensed under MIT.

"""Validate a real causal-multichannel integration run.

The verifier intentionally consumes only durable evidence: the final JSONL
sample for each connection and fatal compatibility signatures in latest.log.
It exits non-zero when the run cannot prove the requested transition count.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


FATAL_LOG_PATTERNS = (
    re.compile(r"received unknown packet id", re.IGNORECASE),
    re.compile(r"recvAddress\(.*message too long", re.IGNORECASE),
    re.compile(r"lost connection: internal exception", re.IGNORECASE),
    re.compile(r"exception caught in connection", re.IGNORECASE),
    re.compile(r"ticking entity", re.IGNORECASE),
    re.compile(r"nullpointerexception.*contraption", re.IGNORECASE),
)

REQUIRED_CAUSAL_FIELDS = (
    "causal_fences_started",
    "causal_fences_completed",
    "causal_fences_failed",
    "causal_inbound_fences_completed",
    "causal_outbound_epoch",
    "causal_inbound_epoch",
    "causal_stale_frames_dropped",
    "causal_outbound_frames_pending",
    "causal_outbound_bytes_pending",
    "causal_outbound_queue_overflows",
    "causal_outbound_queue_names",
    "causal_outbound_frames_queued_by_queue",
    "causal_outbound_frames_pending_by_queue",
    "causal_outbound_bytes_pending_by_queue",
    "causal_outbound_queue_overflows_by_queue",
    "causal_future_frames_pending",
    "causal_future_bytes_pending",
)

CAUSAL_OUTBOUND_QUEUE_NAMES = [
    "APPLICATION",
    "BUNDLE_CONTROL",
    "FENCE",
    "DOMAIN_SCHEDULER",
]


@dataclass
class Verification:
    errors: list[str]
    warnings: list[str]
    connections: int
    outbound_transitions: int
    inbound_transitions: int
    bundles_outbound: int
    bundles_inbound: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Raknetify causal fence/epoch integration evidence."
    )
    parser.add_argument("metrics", type=Path, help="raknetify-metrics.jsonl")
    parser.add_argument(
        "--log",
        type=Path,
        action="append",
        default=[],
        help="latest.log to scan; may be specified more than once",
    )
    parser.add_argument(
        "--minimum-transitions",
        type=int,
        default=100,
        help="minimum completed transitions in either direction (default: 100)",
    )
    parser.add_argument(
        "--require-bundles",
        action="store_true",
        help="fail if no negotiated atomic bundle was observed",
    )
    return parser.parse_args()


def load_final_samples(path: Path) -> tuple[dict[str, dict[str, Any]], list[str]]:
    final_samples: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        return {}, [f"cannot read metrics file {path}: {exc}"]

    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            sample = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"{path}:{line_number}: invalid JSON: {exc}")
            continue
        connection = sample.get("connection")
        if not isinstance(connection, str) or not connection:
            errors.append(f"{path}:{line_number}: missing connection id")
            continue
        final_samples[connection] = sample

    if not final_samples:
        errors.append(f"{path}: no usable metrics samples")
    return final_samples, errors


def integer(sample: dict[str, Any], field: str, connection: str, errors: list[str]) -> int:
    value = sample.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        errors.append(f"connection {connection}: missing numeric field {field}")
        return 0
    return int(value)


def integer_array(
    sample: dict[str, Any],
    field: str,
    connection: str,
    errors: list[str],
) -> list[int]:
    value = sample.get(field)
    if not isinstance(value, list) or len(value) != len(CAUSAL_OUTBOUND_QUEUE_NAMES):
        errors.append(
            f"connection {connection}: field {field} must contain "
            f"{len(CAUSAL_OUTBOUND_QUEUE_NAMES)} numeric values"
        )
        return [0] * len(CAUSAL_OUTBOUND_QUEUE_NAMES)
    result: list[int] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, int):
            errors.append(
                f"connection {connection}: field {field} contains a non-numeric value"
            )
            return [0] * len(CAUSAL_OUTBOUND_QUEUE_NAMES)
        result.append(int(item))
    return result


def verify_metrics(
    samples: dict[str, dict[str, Any]],
    minimum_transitions: int,
    require_bundles: bool,
    initial_errors: list[str],
) -> Verification:
    errors = list(initial_errors)
    warnings: list[str] = []
    outbound_transitions = 0
    inbound_transitions = 0
    bundles_outbound = 0
    bundles_inbound = 0
    active_connections = 0

    for connection, sample in samples.items():
        missing = [field for field in REQUIRED_CAUSAL_FIELDS if field not in sample]
        if missing:
            errors.append(
                f"connection {connection}: build lacks causal metrics: {', '.join(missing)}"
            )
            continue

        started = integer(sample, "causal_fences_started", connection, errors)
        completed = integer(sample, "causal_fences_completed", connection, errors)
        failed = integer(sample, "causal_fences_failed", connection, errors)
        inbound = integer(
            sample, "causal_inbound_fences_completed", connection, errors
        )
        outbound_epoch = integer(sample, "causal_outbound_epoch", connection, errors)
        inbound_epoch = integer(sample, "causal_inbound_epoch", connection, errors)
        stale = integer(sample, "causal_stale_frames_dropped", connection, errors)
        outbound_frames = integer(
            sample, "causal_outbound_frames_pending", connection, errors
        )
        outbound_bytes = integer(
            sample, "causal_outbound_bytes_pending", connection, errors
        )
        outbound_overflows = integer(
            sample, "causal_outbound_queue_overflows", connection, errors
        )
        queue_names = sample.get("causal_outbound_queue_names")
        queued_frames_by_queue = integer_array(
            sample,
            "causal_outbound_frames_queued_by_queue",
            connection,
            errors,
        )
        pending_frames_by_queue = integer_array(
            sample,
            "causal_outbound_frames_pending_by_queue",
            connection,
            errors,
        )
        pending_bytes_by_queue = integer_array(
            sample,
            "causal_outbound_bytes_pending_by_queue",
            connection,
            errors,
        )
        overflows_by_queue = integer_array(
            sample,
            "causal_outbound_queue_overflows_by_queue",
            connection,
            errors,
        )
        future_frames = integer(
            sample, "causal_future_frames_pending", connection, errors
        )
        future_bytes = integer(
            sample, "causal_future_bytes_pending", connection, errors
        )
        bundles_out = integer(
            sample, "causal_atomic_bundles_outbound", connection, errors
        )
        bundles_in = integer(
            sample, "causal_atomic_bundles_inbound", connection, errors
        )

        if started or completed or inbound:
            active_connections += 1
        outbound_transitions += completed
        inbound_transitions += inbound
        bundles_outbound += bundles_out
        bundles_inbound += bundles_in

        if failed:
            errors.append(f"connection {connection}: {failed} causal fences failed")
        if started != completed:
            errors.append(
                f"connection {connection}: incomplete fences "
                f"(started={started}, completed={completed})"
            )
        if outbound_epoch != completed:
            errors.append(
                f"connection {connection}: outbound epoch {outbound_epoch} "
                f"does not match completed fences {completed}"
            )
        if inbound_epoch != inbound:
            errors.append(
                f"connection {connection}: inbound epoch {inbound_epoch} "
                f"does not match completed inbound fences {inbound}"
            )
        if stale:
            errors.append(
                f"connection {connection}: {stale} stale gameplay frames reached the codec"
            )
        if queue_names != CAUSAL_OUTBOUND_QUEUE_NAMES:
            errors.append(
                f"connection {connection}: unexpected causal outbound queue schema "
                f"{queue_names!r}"
            )
        if sum(pending_frames_by_queue) != outbound_frames:
            errors.append(
                f"connection {connection}: per-queue pending frame total "
                f"{sum(pending_frames_by_queue)} does not match aggregate {outbound_frames}"
            )
        if sum(pending_bytes_by_queue) != outbound_bytes:
            errors.append(
                f"connection {connection}: per-queue pending byte total "
                f"{sum(pending_bytes_by_queue)} does not match aggregate {outbound_bytes}"
            )
        if sum(overflows_by_queue) != outbound_overflows:
            errors.append(
                f"connection {connection}: per-queue overflow total "
                f"{sum(overflows_by_queue)} does not match aggregate {outbound_overflows}"
            )
        if any(
            value < 0
            for values in (
                queued_frames_by_queue,
                pending_frames_by_queue,
                pending_bytes_by_queue,
                overflows_by_queue,
            )
            for value in values
        ):
            errors.append(
                f"connection {connection}: negative per-queue causal counter"
            )
        if outbound_overflows:
            errors.append(
                f"connection {connection}: causal outbound queue overflowed "
                f"{outbound_overflows} time(s)"
            )
        if outbound_frames or outbound_bytes:
            errors.append(
                f"connection {connection}: causal outbound queue did not drain "
                f"(frames={outbound_frames}, bytes={outbound_bytes})"
            )
        if future_frames or future_bytes:
            errors.append(
                f"connection {connection}: future-epoch queue did not drain "
                f"(frames={future_frames}, bytes={future_bytes})"
            )

    observed = max(outbound_transitions, inbound_transitions)
    if active_connections == 0:
        errors.append("no connection recorded a causal transition")
    if observed < minimum_transitions:
        errors.append(
            f"only {observed} completed transitions observed; "
            f"{minimum_transitions} required"
        )
    if require_bundles and bundles_outbound + bundles_inbound == 0:
        errors.append("no negotiated atomic bundle was observed")
    elif bundles_outbound + bundles_inbound == 0:
        warnings.append("no negotiated atomic bundle was observed")

    return Verification(
        errors=errors,
        warnings=warnings,
        connections=len(samples),
        outbound_transitions=outbound_transitions,
        inbound_transitions=inbound_transitions,
        bundles_outbound=bundles_outbound,
        bundles_inbound=bundles_inbound,
    )


def scan_logs(paths: list[Path]) -> list[str]:
    errors: list[str] = []
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError as exc:
            errors.append(f"cannot read log file {path}: {exc}")
            continue
        for line_number, line in enumerate(lines, 1):
            if any(pattern.search(line) for pattern in FATAL_LOG_PATTERNS):
                errors.append(f"{path}:{line_number}: {line.strip()[:300]}")
    return errors


def main() -> int:
    args = parse_args()
    if args.minimum_transitions < 1:
        print("--minimum-transitions must be positive", file=sys.stderr)
        return 2

    samples, parse_errors = load_final_samples(args.metrics)
    result = verify_metrics(
        samples,
        args.minimum_transitions,
        args.require_bundles,
        parse_errors,
    )
    result.errors.extend(scan_logs(args.log))

    summary = {
        "status": "PASS" if not result.errors else "FAIL",
        "connections": result.connections,
        "outbound_transitions": result.outbound_transitions,
        "inbound_transitions": result.inbound_transitions,
        "atomic_bundles_outbound": result.bundles_outbound,
        "atomic_bundles_inbound": result.bundles_inbound,
        "warnings": result.warnings,
        "errors": result.errors,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if not result.errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
