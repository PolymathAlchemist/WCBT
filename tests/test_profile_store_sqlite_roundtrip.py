from __future__ import annotations

from pathlib import Path

from backup_engine.profile_store.api import RuleSet
from backup_engine.profile_store.sqlite_store import SqliteProfileStore


def test_sqlite_profile_store_roundtrip(tmp_path: Path) -> None:
    """Rules saved for a job_id should round-trip exactly."""
    store = SqliteProfileStore(db_path=tmp_path / "profiles.sqlite")

    store.save_rules(
        job_id="job-1", name="Example Job", rules=RuleSet(include=("a/**",), exclude=("b/**",))
    )
    loaded = store.load_rules("job-1")

    assert loaded == RuleSet(include=("a/**",), exclude=("b/**",))
