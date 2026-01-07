from __future__ import annotations

import json
import os
from pathlib import Path

from backup_engine.manifest_store import list_backup_runs


def _write_manifest(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_list_backup_runs_filters_schema_and_sorts_by_mtime_desc(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"

    # Valid v2 manifest A (older)
    m1 = archive_root / "runA" / "manifest.json"
    _write_manifest(
        m1,
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "runA",
            "created_at_utc": "2026-01-01T00:00:00+00:00",
            "archive_root": str(archive_root),
            "profile_name": "default",
            "source_root": "C:/source",
        },
    )

    # Invalid schema manifest (should be skipped)
    m2 = archive_root / "bad" / "manifest.json"
    _write_manifest(
        m2,
        {
            "schema_version": "not_v2",
            "run_id": "bad",
        },
    )

    # Valid v2 manifest B (newer)
    m3 = archive_root / "runB" / "manifest.json"
    _write_manifest(
        m3,
        {
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "runB",
            "created_at_utc": "2026-01-02T00:00:00+00:00",
            "archive_root": str(archive_root),
            "profile_name": "default",
            "source_root": "C:/source",
        },
    )

    # Force mtimes so ordering is deterministic
    os.utime(m1, (100.0, 100.0))
    os.utime(m2, (150.0, 150.0))
    os.utime(m3, (200.0, 200.0))

    runs = list_backup_runs(archive_root, limit=500)

    assert [r.run_id for r in runs] == ["runB", "runA"]
    assert runs[0].manifest_path.name == "manifest.json"
    assert runs[0].created_at_utc == "2026-01-02T00:00:00+00:00"

    runs_default = list_backup_runs(archive_root, profile_name="default", limit=500)
    assert [r.run_id for r in runs_default] == ["runB", "runA"]

    runs_other = list_backup_runs(archive_root, profile_name="other", limit=500)
    assert runs_other == []


def test_list_backup_runs_respects_limit(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"

    for i in range(3):
        mp = archive_root / f"run{i}" / "manifest.json"
        _write_manifest(
            mp,
            {
                "schema_version": "wcbt_run_manifest_v2",
                "run_id": f"run{i}",
            },
        )
        os.utime(mp, (float(i), float(i)))

    runs = list_backup_runs(archive_root, limit=2)
    assert len(runs) == 2
