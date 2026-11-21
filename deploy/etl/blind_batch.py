#!/usr/bin/env python3
"""Prepare, execute, and reveal a procedurally blinded ETL experiment batch."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import experiment


ETL_DIR = Path(__file__).resolve().parent
DEFAULT_PROTOCOL = ETL_DIR / "blind_protocol_v1.json"
DEFAULT_BATCH_ROOT = ETL_DIR / "reports" / "blind-batches"
DEFAULT_CONFIRMATORY_MANIFEST = (
    ETL_DIR.parents[2]
    / "semillero-monitoreo-distribuido"
    / "experiments"
    / "algorithm_comparison"
    / "config"
    / "confirmatory_manifest_v1.json"
)
SEALED_NAME = "allocation.sealed.json"
PUBLIC_NAME = "public-cohort.json"
COMMITMENT_NAME = "commitment.json"
PROTOCOL_NAME = "protocol.snapshot.json"
CONFIRMATORY_NAME = "confirmatory-manifest.snapshot.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def sha256_value(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def atomic_json(path: Path, value: object, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as target:
            json.dump(value, target, indent=2, ensure_ascii=False)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_copy(source: Path, path: Path, mode: int = 0o444) -> None:
    """Copy an already frozen artifact without changing its committed bytes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with source.open("rb") as origin, os.fdopen(descriptor, "wb") as target:
            for chunk in iter(lambda: origin.read(1024 * 1024), b""):
                target.write(chunk)
            target.flush()
            os.fsync(target.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def validate_protocol(protocol: dict[str, Any]) -> list[dict[str, Any]]:
    required = {
        "protocol_version", "scenario_id", "application_version",
        "configuration", "load_profile", "fault", "affected_component",
        "repetitions_per_cell", "warmup_seconds",
        "observation_horizon_seconds", "drain_seconds",
        "maximum_consumer_lag", "cells", "acceptance",
    }
    missing = sorted(required - protocol.keys())
    if missing:
        raise ValueError(f"Protocol is missing required fields: {', '.join(missing)}")

    repetitions = int(protocol["repetitions_per_cell"])
    horizon = float(protocol["observation_horizon_seconds"])
    acceptance = protocol["acceptance"]
    cells = protocol["cells"]
    if repetitions != 3 or len(cells) != 4:
        raise ValueError("The blind protocol requires four cells and three repetitions")
    if float(protocol["warmup_seconds"]) <= 0 or horizon <= 0:
        raise ValueError("Warm-up and observation horizon must be positive")

    identifiers: set[str] = set()
    controls = 0
    expanded: list[dict[str, Any]] = []
    for cell in cells:
        cell_id = str(cell["cell_id"])
        if cell_id in identifiers:
            raise ValueError(f"Duplicate cell_id: {cell_id}")
        identifiers.add(cell_id)
        before = float(cell["normal_before_fault_seconds"])
        fault = float(cell["fault_seconds"])
        recovery = float(cell["recovery_seconds"])
        if min(before, fault, recovery) < 0:
            raise ValueError(f"Cell {cell_id} has a negative duration")
        if abs(before + fault + recovery - horizon) > 1e-9:
            raise ValueError(f"Cell {cell_id} does not match the common horizon")
        if bool(cell["control"]):
            controls += 1
            if fault != 0 or recovery != 0 or before != horizon:
                raise ValueError("The control cell must remain normal for the full horizon")
        else:
            if fault != float(protocol["fault_duration_seconds"]):
                raise ValueError(f"Cell {cell_id} has an unexpected fault duration")
            if before < float(acceptance["minimum_normal_reference_seconds"]):
                raise ValueError(f"Cell {cell_id} has insufficient normal reference")
            if recovery < float(acceptance["minimum_post_fault_seconds"]):
                raise ValueError(f"Cell {cell_id} has insufficient recovery")
        for repetition in range(1, repetitions + 1):
            expanded.append({
                "cell_id": cell_id,
                "cell_repetition": repetition,
                "control": bool(cell["control"]),
                "normal_before_fault_seconds": before,
                "fault_seconds": fault,
                "recovery_seconds": recovery,
            })

    expected_runs = int(acceptance["complete_runs_required"])
    if controls != 1 or len(expanded) != expected_runs or expected_runs != 12:
        raise ValueError("The protocol must expand to one control cell and twelve runs")
    if not acceptance.get("equal_duration"):
        raise ValueError("Equal observation duration is required")
    if acceptance.get("no_backlog_override") is not True:
        raise ValueError("Blind runs may not override the backlog safeguard")
    if float(acceptance.get("duration_scale", 0)) != 1.0:
        raise ValueError("Blind runs require duration_scale 1.0")
    return expanded


def new_batch_id(protocol_version: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{protocol_version}-{timestamp}-{secrets.token_hex(3)}"


def prepare_batch(
    protocol_path: Path,
    output_root: Path,
    batch_id: str | None = None,
    rng: Any | None = None,
    confirmatory_manifest_path: Path = DEFAULT_CONFIRMATORY_MANIFEST,
) -> Path:
    protocol = read_json(protocol_path)
    allocations = validate_protocol(protocol)
    confirmatory = read_json(confirmatory_manifest_path)
    if confirmatory.get("status") != "frozen_before_blind_batch_preparation":
        raise ValueError("Confirmatory manifest is not frozen")
    if confirmatory.get("blind_protocol", {}).get("protocol_version") != protocol[
        "protocol_version"
    ]:
        raise ValueError("Confirmatory manifest targets a different blind protocol")
    confirmatory_hash = sha256_file(confirmatory_manifest_path)
    if confirmatory.get("blind_protocol", {}).get("protocol_sha256") != sha256_file(
        protocol_path
    ):
        raise ValueError("Confirmatory manifest targets different protocol contents")
    batch_id = batch_id or new_batch_id(str(protocol["protocol_version"]))
    batch_dir = output_root / batch_id
    if batch_dir.exists():
        raise FileExistsError(f"Batch directory already exists: {batch_dir}")
    batch_dir.mkdir(parents=True)

    (rng or secrets.SystemRandom()).shuffle(allocations)
    used_codes: set[str] = set()
    for order, allocation in enumerate(allocations, start=1):
        code = ""
        while not code or code in used_codes:
            code = f"BR-{secrets.token_hex(6).upper()}"
        used_codes.add(code)
        allocation["order"] = order
        allocation["run_code"] = code

    sealed = {
        "batch_id": batch_id,
        "protocol_version": protocol["protocol_version"],
        "created_at": utc_now(),
        "allocations": allocations,
    }
    commitment_hash = sha256_value(sealed)
    commitment = {
        "batch_id": batch_id,
        "protocol_version": protocol["protocol_version"],
        "created_at": sealed["created_at"],
        "protocol_sha256": sha256_value(protocol),
        "confirmatory_manifest_version": confirmatory["manifest_version"],
        "confirmatory_manifest_sha256": confirmatory_hash,
        "allocation_commitment_sha256": commitment_hash,
        "run_count": len(allocations),
        "status": "prepared",
        "blinding": "procedural-single-blind",
    }
    public = {
        "batch_id": batch_id,
        "protocol_version": protocol["protocol_version"],
        "confirmatory_manifest_version": confirmatory["manifest_version"],
        "confirmatory_manifest_sha256": confirmatory_hash,
        "allocation_commitment_sha256": commitment_hash,
        "status": "prepared",
        "runs": [
            {
                "order": item["order"],
                "run_code": item["run_code"],
                "run_id": None,
                "status": "pending",
            }
            for item in allocations
        ],
    }
    atomic_json(batch_dir / PROTOCOL_NAME, protocol, mode=0o444)
    atomic_copy(confirmatory_manifest_path, batch_dir / CONFIRMATORY_NAME)
    atomic_json(batch_dir / SEALED_NAME, sealed, mode=0o600)
    atomic_json(batch_dir / COMMITMENT_NAME, commitment, mode=0o444)
    atomic_json(batch_dir / PUBLIC_NAME, public)
    return batch_dir


def verify_batch(batch_dir: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    protocol = read_json(batch_dir / PROTOCOL_NAME)
    validate_protocol(protocol)
    confirmatory = read_json(batch_dir / CONFIRMATORY_NAME)
    sealed = read_json(batch_dir / SEALED_NAME)
    commitment = read_json(batch_dir / COMMITMENT_NAME)
    if sha256_value(protocol) != commitment["protocol_sha256"]:
        raise RuntimeError("Protocol snapshot does not match its SHA-256 commitment")
    if sha256_file(batch_dir / CONFIRMATORY_NAME) != commitment[
        "confirmatory_manifest_sha256"
    ]:
        raise RuntimeError(
            "Confirmatory manifest snapshot does not match its SHA-256 commitment"
        )
    if confirmatory.get("manifest_version") != commitment[
        "confirmatory_manifest_version"
    ]:
        raise RuntimeError("Confirmatory manifest version does not match")
    if sha256_value(sealed) != commitment["allocation_commitment_sha256"]:
        raise RuntimeError("Sealed allocation does not match its SHA-256 commitment")
    if sealed["batch_id"] != commitment["batch_id"]:
        raise RuntimeError("Batch identifiers do not match")
    return protocol, sealed, commitment


def require_valid_public_cohort(
    public: dict[str, Any], sealed: dict[str, Any], commitment: dict[str, Any],
) -> None:
    if public.get("batch_id") != commitment["batch_id"]:
        raise RuntimeError("Public cohort has a different batch identifier")
    if public.get("allocation_commitment_sha256") != commitment[
        "allocation_commitment_sha256"
    ]:
        raise RuntimeError("Public cohort has a different allocation commitment")
    if public.get("confirmatory_manifest_sha256") != commitment[
        "confirmatory_manifest_sha256"
    ]:
        raise RuntimeError("Public cohort has a different confirmatory manifest")
    expected = {
        (item["order"], item["run_code"]) for item in sealed["allocations"]
    }
    observed = {
        (item.get("order"), item.get("run_code")) for item in public.get("runs", [])
    }
    if observed != expected or len(public.get("runs", [])) != len(expected):
        raise RuntimeError("Public cohort does not match the committed run codes")


def execution_args(
    protocol: dict[str, Any], allocation: dict[str, Any], commitment: dict[str, Any],
) -> SimpleNamespace:
    return SimpleNamespace(
        scenario=protocol["scenario_id"],
        fault=protocol["fault"],
        application_version=protocol["application_version"],
        configuration=protocol["configuration"],
        load_profile=protocol["load_profile"],
        repetitions=1,
        warmup=float(protocol["warmup_seconds"]),
        normal=float(allocation["normal_before_fault_seconds"]),
        fault_duration=float(allocation["fault_seconds"]),
        recovery=float(allocation["recovery_seconds"]),
        stationary_seconds=None,
        control_cycle=bool(allocation["control"]),
        duration_scale=1.0,
        drain_seconds=float(protocol["drain_seconds"]),
        allow_backlog=False,
        max_lag=int(protocol["maximum_consumer_lag"]),
        blind_batch_id=commitment["batch_id"],
        blind_run_code=allocation["run_code"],
        allocation_commitment=commitment["allocation_commitment_sha256"],
    )


def run_batch(batch_dir: Path) -> None:
    protocol, sealed, commitment = verify_batch(batch_dir)
    public_path = batch_dir / PUBLIC_NAME
    public = read_json(public_path)
    require_valid_public_cohort(public, sealed, commitment)
    allocations = {item["run_code"]: item for item in sealed["allocations"]}
    for public_run in public["runs"]:
        if public_run["status"] == "complete":
            continue
        allocation = allocations[public_run["run_code"]]
        public_run["status"] = "running"
        atomic_json(public_path, public)
        try:
            report = experiment.execute(execution_args(protocol, allocation, commitment))
            completed = report["runs"][-1]
            public_run["run_id"] = completed["run_id"]
            public_run["status"] = completed["status"]
            public_run.pop("error_type", None)
            if completed["status"] != "complete":
                raise RuntimeError(
                    f"Blind run {public_run['run_code']} ended as {completed['status']}"
                )
        except BaseException as error:
            public_run["status"] = "failed"
            public_run["error_type"] = type(error).__name__
            public["status"] = "failed"
            atomic_json(public_path, public)
            raise
        atomic_json(public_path, public)
    public["status"] = "execution_complete"
    public["execution_completed_at"] = utc_now()
    atomic_json(public_path, public)


def reveal_batch(batch_dir: Path, predictions: Path) -> Path:
    _, sealed, commitment = verify_batch(batch_dir)
    public_path = batch_dir / PUBLIC_NAME
    public = read_json(public_path)
    require_valid_public_cohort(public, sealed, commitment)
    if public.get("status") != "execution_complete":
        raise RuntimeError("The allocation cannot be revealed before all runs complete")
    if any(
        run.get("status") != "complete" or not run.get("run_id")
        for run in public["runs"]
    ):
        raise RuntimeError("Every public run must be complete and have a run_id")
    if not predictions.is_file():
        raise FileNotFoundError(f"Predictions file does not exist: {predictions}")
    if predictions.stat().st_size == 0:
        raise ValueError("Predictions file must not be empty")
    prediction_commitment_path = batch_dir / "predictions-commitment.json"
    revealed_path = batch_dir / "allocation-revealed.json"
    if prediction_commitment_path.exists() or revealed_path.exists():
        raise FileExistsError("This blind allocation has already been revealed")
    frozen_at = utc_now()
    prediction_commitment = {
        "batch_id": commitment["batch_id"],
        "predictions_file": predictions.name,
        "predictions_sha256": sha256_file(predictions),
        "frozen_at": frozen_at,
    }
    atomic_json(prediction_commitment_path, prediction_commitment, mode=0o444)
    revealed = {
        "batch_id": commitment["batch_id"],
        "allocation_commitment_sha256": commitment[
            "allocation_commitment_sha256"
        ],
        "predictions_commitment": prediction_commitment,
        "revealed_at": utc_now(),
        "allocations": sealed["allocations"],
    }
    atomic_json(revealed_path, revealed, mode=0o444)
    public["status"] = "revealed"
    public["revealed_at"] = revealed["revealed_at"]
    public["predictions_sha256"] = prediction_commitment["predictions_sha256"]
    atomic_json(public_path, public)
    return revealed_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare = subparsers.add_parser("prepare", help="Freeze and commit a blind allocation")
    prepare.add_argument("--protocol", type=Path, default=DEFAULT_PROTOCOL)
    prepare.add_argument("--output-root", type=Path, default=DEFAULT_BATCH_ROOT)
    prepare.add_argument("--batch-id")
    prepare.add_argument(
        "--confirmatory-manifest",
        type=Path,
        default=DEFAULT_CONFIRMATORY_MANIFEST,
    )
    run = subparsers.add_parser("run", help="Execute pending runs in a prepared batch")
    run.add_argument("--batch-dir", type=Path, required=True)
    verify = subparsers.add_parser("verify", help="Verify protocol and allocation commitments")
    verify.add_argument("--batch-dir", type=Path, required=True)
    reveal = subparsers.add_parser(
        "reveal", help="Freeze predictions and reveal the allocation"
    )
    reveal.add_argument("--batch-dir", type=Path, required=True)
    reveal.add_argument("--predictions", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "prepare":
        batch_dir = prepare_batch(
            args.protocol,
            args.output_root,
            args.batch_id,
            confirmatory_manifest_path=args.confirmatory_manifest,
        )
        print(batch_dir)
    elif args.command == "run":
        run_batch(args.batch_dir)
        print(f"Completed blind batch: {args.batch_dir}")
    elif args.command == "verify":
        _, _, commitment = verify_batch(args.batch_dir)
        print(json.dumps(commitment, indent=2))
    elif args.command == "reveal":
        print(reveal_batch(args.batch_dir, args.predictions))
    return 0


if __name__ == "__main__":
    sys.exit(main())
