from __future__ import annotations

import pytest

from backup_engine.profile_store.api import RuleSet
from backup_engine.profile_store.errors import InvalidRuleError
from backup_engine.profile_store.rules import normalize_patterns, normalize_rules


def test_normalize_patterns_drops_empty_and_strips() -> None:
    assert normalize_patterns(["", "  ", "a/**  "]) == ("a/**",)


def test_normalize_patterns_normalizes_backslashes() -> None:
    assert normalize_patterns([r"a\**\b.txt"]) == ("a/**/b.txt",)


def test_normalize_patterns_rejects_leading_slash() -> None:
    with pytest.raises(InvalidRuleError):
        normalize_patterns(["/abs/path"])


def test_normalize_patterns_rejects_drive_hint() -> None:
    with pytest.raises(InvalidRuleError):
        normalize_patterns(["C:/windows"])


def test_normalize_rules_applies_to_both_lists() -> None:
    rules = RuleSet(include=(" a/** ",), exclude=(r"b\*.tmp",))
    normalized = normalize_rules(rules)
    assert normalized.include == ("a/**",)
    assert normalized.exclude == ("b/*.tmp",)
