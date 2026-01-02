"""
Run verification for WCBT backup archives.

This module verifies an already-materialized run by hashing archived files and
recording deterministic results into the run manifest.

Safety posture
--------------
- Verification never mutates source data.
- Verification never modifies archived file bytes.
- Verification never deletes anything.
- The only side effect is an atomic update to the run manifest.json.

Design notes
------------
- Full scan: verification records results for all verifiable operations.
- Locking: verification participates in the per-profile lock.
- Hashing: SHA-256 only in v1, with a minimal abstraction seam.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Protocol

from backup_engine.errors import WcbtError
from backup_engine.manifest_store import (
    read_manifest_json,
    write_json_atomic,
    write_manifest_json_atomic,
)
from backup_engine.paths_and_safety import resolve_profile_paths
from backup_engine.profile_lock import acquire_profile_lock, build_profile_lock_path


class VerifyError(WcbtError):
    """Raised when verification cannot be completed due to domain-level failures."""


class HashAlgorithm(str, Enum):
    """Supported hashing algorithms for verification."""

    SHA256 = "sha256"


class VerificationOutcome(str, Enum):
    """Per-operation verification outcome."""

    VERIFIED = "verified"
    FAILED = "failed"
    NOT_APPLICABLE = "not_applicable"


class _VerifyCounts(Protocol):
    @property
    def verified(self) -> int: ...

    @property
    def failed(self) -> int: ...

    @property
    def not_applicable(self) -> int: ...


@dataclass(frozen=True, slots=True)
class VerificationCounts:
    """Deterministic verification counts."""

    verified: int
    failed: int
    not_applicable: int

    @property
    def total_verifiable(self) -> int:
        return self.verified + self.failed


@dataclass(frozen=True, slots=True)
class VerificationStatusCounts:
    """Deterministic counts by emitted verify record status."""

    ok: int
    missing: int
    unreadable: int
    hash_mismatch: int

    @property
    def total(self) -> int:
        return self.ok + self.missing + self.unreadable + self.hash_mismatch


def compute_digest(file_path: Path, algorithm: HashAlgorithm) -> str:
    """
    Compute a file digest.

    Parameters
    ----------
    file_path:
        Path to the file to hash.
    algorithm:
        Hash algorithm to use.

    Returns
    -------
    str
        Lowercase hex digest.

    Raises
    ------
    OSError
        If the file cannot be read.
    ValueError
        If the algorithm is not supported.

    Notes
    -----
    Files are streamed in 1 MiB chunks to avoid unbounded memory usage.
    """
    if algorithm is not HashAlgorithm.SHA256:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    hasher = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def verify_run(
    *,
    profile_name: str,
    run_id: str,
    data_root: Path | None = None,
    force: bool = False,
    break_lock: bool = False,
) -> None:
    """
    Verify an existing backup run by hashing archived files.

    Parameters
    ----------
    profile_name:
        Profile name that owns the run.
    run_id:
        Run ID (directory name under archives_root).
    data_root:
        Override WCBT data root (primarily for testing).
    force:
        Break a provably stale lock automatically.
    break_lock:
        Break an existing lock even when not provably stale.

    Raises
    ------
    VerifyError
        If verification fails (one or more files missing/unreadable).
    WcbtError
        For domain and safety errors.
    ValueError
        If required manifest fields are missing or invalid.

    Artifacts
    ---------
    The following files are written under the run directory:

    - manifest.json (updated atomically)
    - verify_report.json
    - verify_report.jsonl
    - verify_summary.txt
    """
    paths = resolve_profile_paths(profile_name, data_root=data_root)

    lock_path = build_profile_lock_path(work_root=paths.work_root)
    with acquire_profile_lock(
        lock_path=lock_path,
        profile_name=profile_name,
        command="verify",
        run_id=run_id,
        force=force,
        break_lock=break_lock,
    ):
        run_root = paths.archives_root / run_id
        manifest_path = run_root / "manifest.json"
        manifest = read_manifest_json(manifest_path)

        updated_manifest, counts, status_counts, records = _verify_manifest(
            run_root=run_root,
            manifest=manifest,
            hash_algorithm=HashAlgorithm.SHA256,
        )

        write_manifest_json_atomic(manifest_path, updated_manifest)

        # Always write artifacts so runs are inspectable even when verification fails.
        _write_verify_report(
            run_root,
            run_id=run_id,
            algorithm=HashAlgorithm.SHA256.value,
            counts=counts,
            status_counts=status_counts,
            records=records,
        )

        if counts.failed > 0:
            raise VerifyError(
                f"Verification failed for {counts.failed} file(s). "
                f"See manifest.json for per-operation verification results."
            )

        print()
        print("Verification complete:")
        print(f"  Run ID        : {run_id}")
        print(f"  Algorithm     : {HashAlgorithm.SHA256.value}")
        print(f"  Verified      : {counts.verified}")
        print(f"  Failed        : {counts.failed}")
        print(f"  Not applicable: {counts.not_applicable}")
        print(f"  Results written: {manifest_path}")


def _verify_manifest(
    *,
    run_root: Path,
    manifest: Mapping[str, Any],
    hash_algorithm: HashAlgorithm,
) -> tuple[dict[str, Any], VerificationCounts, VerificationStatusCounts, list[dict[str, Any]]]:
    """
    Verify a manifest's copied file outcomes and return an updated manifest.

    Parameters
    ----------
    manifest:
        Parsed run manifest mapping.
    hash_algorithm:
        Hash algorithm to compute for archived files.

    Returns
    -------
    tuple[dict[str, Any], VerificationCounts, VerificationStatusCounts, list[dict[str, Any]]]
        Updated manifest dictionary, aggregate counts, deterministic status counts,
        and JSONL record payloads.

    Raises
    ------
    ValueError
        If required keys are missing.
    """
    payload = dict(manifest)

    operations = payload.get("operations")
    if not isinstance(operations, list):
        raise ValueError("manifest.operations must be a list")

    execution = payload.get("execution")
    results_by_index = _index_execution_results(execution)

    verified = 0
    failed = 0
    not_applicable = 0

    records: list[dict[str, Any]] = []

    status_ok = 0
    status_missing = 0
    status_unreadable = 0
    status_hash_mismatch = 0

    for i, op in enumerate(operations):
        if not isinstance(op, dict):
            continue

        op_type = str(op.get("operation_type", ""))
        if op_type != "copy_file_to_archive":
            not_applicable += 1
            continue

        exec_result = results_by_index.get(i)
        if exec_result is None:
            # Not executed yet; do not verify.
            not_applicable += 1
            continue

        outcome = str(exec_result.get("outcome", ""))
        if outcome != "copied":
            not_applicable += 1
            continue

        destination_path = _extract_destination_path(op, exec_result)
        if destination_path is None:
            failed += 1
            _write_verification_fields(
                exec_result,
                outcome=VerificationOutcome.FAILED,
                algorithm=hash_algorithm,
                digest_hex=None,
                size_bytes=None,
                error="Missing destination path for verification.",
            )
            continue

        dest_path = Path(destination_path)
        if not dest_path.is_absolute():
            dest_path = run_root / dest_path

        rel_path: str
        try:
            rel_path = str(dest_path.relative_to(run_root))
        except ValueError:
            # Fall back to the raw string if it isn't under run_root
            rel_path = str(Path(destination_path))

        if not dest_path.exists():
            failed += 1
            status_missing += 1
            records.append(
                {
                    "schema": "wcbt_verify_record_v1",
                    "run_id": str(payload.get("run_id", "")),
                    "status": "missing",
                    "path": rel_path,
                }
            )
            _write_verification_fields(
                exec_result,
                outcome=VerificationOutcome.FAILED,
                algorithm=hash_algorithm,
                digest_hex=None,
                size_bytes=None,
                error="File not found for verification.",
            )
            continue

        try:
            size_bytes = dest_path.stat().st_size
            digest_hex = compute_digest(dest_path, hash_algorithm)
        except OSError as exc:
            failed += 1
            status_unreadable += 1
            records.append(
                {
                    "schema": "wcbt_verify_record_v1",
                    "run_id": str(payload.get("run_id", "")),
                    "status": "unreadable",
                    "path": rel_path,
                }
            )
            _write_verification_fields(
                exec_result,
                outcome=VerificationOutcome.FAILED,
                algorithm=hash_algorithm,
                digest_hex=None,
                size_bytes=None,
                error=f"{type(exc).__name__}: {exc}",
            )
            continue

        expected_digest = _extract_expected_digest_hex(op, exec_result)

        if expected_digest is not None and digest_hex != expected_digest:
            failed += 1
            status_hash_mismatch += 1
            records.append(
                {
                    "schema": "wcbt_verify_record_v1",
                    "run_id": str(payload.get("run_id", "")),
                    "status": "hash_mismatch",
                    "path": rel_path,
                }
            )
            _write_verification_fields(
                exec_result,
                outcome=VerificationOutcome.FAILED,
                algorithm=hash_algorithm,
                digest_hex=digest_hex,
                size_bytes=size_bytes,
                error="Digest mismatch.",
            )
            continue

        verified += 1
        status_ok += 1
        records.append(
            {
                "schema": "wcbt_verify_record_v1",
                "run_id": str(payload.get("run_id", "")),
                "status": "ok",
                "path": rel_path,
            }
        )
        _write_verification_fields(
            exec_result,
            outcome=VerificationOutcome.VERIFIED,
            algorithm=hash_algorithm,
            digest_hex=digest_hex,
            size_bytes=size_bytes,
            error=None,
        )

    status = "success" if failed == 0 else "failed"
    payload["verification"] = {
        "status": status,
        "hash_algorithm": hash_algorithm.value,
        "verified_count": verified,
        "failed_count": failed,
        "not_applicable_count": not_applicable,
        "total_verifiable_count": verified + failed,
    }

    counts = VerificationCounts(
        verified=verified,
        failed=failed,
        not_applicable=not_applicable,
    )

    status_counts = VerificationStatusCounts(
        ok=status_ok,
        missing=status_missing,
        unreadable=status_unreadable,
        hash_mismatch=status_hash_mismatch,
    )

    return payload, counts, status_counts, records


def _index_execution_results(execution: Any) -> dict[int, dict[str, Any]]:
    """
    Index execution results by operation_index.

    Parameters
    ----------
    execution:
        manifest.execution payload

    Returns
    -------
    dict[int, dict[str, Any]]
        Mapping of operation index to the mutable result dict.
    """
    if not isinstance(execution, dict):
        return {}

    results = execution.get("results")
    if not isinstance(results, list):
        return {}

    indexed: dict[int, dict[str, Any]] = {}
    for item in results:
        if not isinstance(item, dict):
            continue
        idx = item.get("operation_index")
        if isinstance(idx, int):
            indexed[idx] = item
    return indexed


def _extract_expected_digest_hex(
    op: Mapping[str, Any], exec_result: Mapping[str, Any]
) -> str | None:
    # Prefer explicit "expected" keys
    for key in (
        "expected_digest_hex",
        "expected_hash_hex",
        "expected_sha256",
        "expected_sha256_hex",
    ):
        value = exec_result.get(key)
        if isinstance(value, str) and value:
            return value
    for key in (
        "expected_digest_hex",
        "expected_hash_hex",
        "expected_sha256",
        "expected_sha256_hex",
    ):
        value = op.get(key)
        if isinstance(value, str) and value:
            return value

    # Optional compatibility: if the planned op carries a digest under a common name
    for key in ("sha256", "sha256_hex", "digest_hex"):
        value = op.get(key)
        if isinstance(value, str) and value:
            return value

    return None


def _extract_destination_path(op: Mapping[str, Any], exec_result: Mapping[str, Any]) -> str | None:
    """
    Extract destination path for a copy operation from manifest payloads.

    Parameters
    ----------
    op:
        Planned operation payload.
    exec_result:
        Execution result payload.

    Returns
    -------
    str | None
        Destination path string if present.
    """
    # Prefer execution result naming first (it reflects actual target).
    for key in ("destination_path", "dest_path", "destination"):
        value = exec_result.get(key)
        if isinstance(value, str) and value:
            return value

    # Fall back to planned operation payload.
    for key in ("destination_path", "dest_path", "destination"):
        value = op.get(key)
        if isinstance(value, str) and value:
            return value

    return None


def _write_verification_fields(
    exec_result: dict[str, Any],
    *,
    outcome: VerificationOutcome,
    algorithm: HashAlgorithm,
    digest_hex: str | None,
    size_bytes: int | None,
    error: str | None,
) -> None:
    """
    Mutate a single execution result dict to include verification fields.

    Parameters
    ----------
    exec_result:
        Mutable execution result dictionary.
    outcome:
        Verification outcome.
    algorithm:
        Hash algorithm.
    digest_hex:
        Digest hex string if computed.
    size_bytes:
        File size in bytes if read.
    error:
        Error string if failed.

    Notes
    -----
    This writes only additive fields and does not modify existing execution
    outcome fields.
    """
    exec_result["verification_outcome"] = outcome.value
    exec_result["verification"] = {
        "hash_algorithm": algorithm.value,
        "digest_hex": digest_hex,
        "size_bytes": size_bytes,
        "error": error,
    }


def _write_verify_report(
    run_root: Path,
    *,
    run_id: str,
    algorithm: str,
    counts: _VerifyCounts,
    status_counts: VerificationStatusCounts,
    records: list[dict[str, Any]],
) -> None:
    """
    Write verify artifacts for an archive run.

    Parameters
    ----------
    run_root:
        Root directory of the run under archives_root.
    run_id:
        Run identifier (directory name).
    algorithm:
        Hash algorithm name.
    counts:
        Aggregate verification counts.
    status_counts:
        Deterministic counts by verify record status.
    records:
        JSONL record payloads to write (one per verified or failed file).

    Artifacts
    ---------
    - verify_report.json
    - verify_report.jsonl
    - verify_summary.txt
    """
    verify_report = {
        "schema": "wcbt_verify_report_v1",
        "run_id": run_id,
        "algorithm": algorithm,
        "verified": counts.verified,
        "failed": counts.failed,
        "not_applicable": counts.not_applicable,
        "status_counts": {
            "ok": status_counts.ok,
            "missing": status_counts.missing,
            "unreadable": status_counts.unreadable,
            "hash_mismatch": status_counts.hash_mismatch,
        },
    }

    write_json_atomic(run_root / "verify_report.json", verify_report)

    jsonl_path = run_root / "verify_report.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")

    summary_lines = [
        "WCBT Verify Report",
        f"Run ID        : {run_id}",
        f"Algorithm     : {algorithm}",
        f"Verified      : {counts.verified}",
        f"Failed        : {counts.failed}",
        f"Not applicable: {counts.not_applicable}",
        "",
        "Status counts:",
        f"  ok           : {status_counts.ok}",
        f"  missing      : {status_counts.missing}",
        f"  unreadable   : {status_counts.unreadable}",
        f"  hash_mismatch: {status_counts.hash_mismatch}",
    ]
    (run_root / "verify_summary.txt").write_text(
        "\n".join(summary_lines),
        encoding="utf-8",
    )
