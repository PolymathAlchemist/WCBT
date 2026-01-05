"""
Mock data for GUI prototypes.

No engine wiring. Pure in-memory models so the UI can be iterated safely.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


@dataclass(frozen=True, slots=True)
class MockJob:
    job_id: str
    name: str
    root_path: Path


@dataclass(frozen=True, slots=True)
class MockRun:
    run_id: str
    job_id: str
    started_at: datetime
    finished_at: datetime
    status: str  # "success" | "failed"
    dry_run: bool
    artifact_root: Path
    manifest_summary: dict[str, str]
    counts_summary: dict[str, int]


def seed_jobs() -> list[MockJob]:
    return [
        MockJob("job-001", "Minecraft Server Backup", Path(r"C:\dev\minecraft_server")),
        MockJob("job-002", "Photos Archive", Path(r"D:\photos")),
        MockJob("job-003", "WCBT Repo Snapshot", Path(r"C:\dev\wcbt")),
    ]


def seed_runs() -> list[MockRun]:
    now = datetime.now()
    runs: list[MockRun] = []

    def add_run(i: int, job_id: str, status: str, dry_run: bool) -> None:
        started = now - timedelta(days=i, hours=1)
        finished = started + timedelta(minutes=3, seconds=12)
        runs.append(
            MockRun(
                run_id=f"run-{job_id[-3:]}-{1000 + i}",
                job_id=job_id,
                started_at=started,
                finished_at=finished,
                status=status,
                dry_run=dry_run,
                artifact_root=Path(r"C:\wcbt_data\artifacts") / job_id / f"run_{1000 + i}",
                manifest_summary={
                    "schema_version": "1",
                    "source_root": str(
                        Path(r"C:\dev\minecraft_server")
                        if job_id == "job-001"
                        else Path(r"C:\dev\wcbt")
                    ),
                    "hash_alg": "sha256",
                    "candidate_rule": "include-all; excludes=['logs/**']",
                },
                counts_summary={
                    "candidates": 18234 + i * 3,
                    "selected": 18234 + i * 3,
                    "excluded": 0,
                    "copied": 120 + i,
                    "skipped": 18114,
                    "conflicts": 0,
                    "unreadable": 0,
                    "hash_mismatch": 0 if status == "success" else 2,
                },
            )
        )

    add_run(1, "job-001", "success", False)
    add_run(2, "job-001", "success", False)
    add_run(3, "job-001", "failed", False)
    add_run(4, "job-001", "success", True)
    add_run(1, "job-003", "success", False)
    add_run(2, "job-003", "success", False)

    return runs
