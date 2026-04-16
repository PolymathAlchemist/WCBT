from __future__ import annotations

import pytest

from backup_engine.job_binding import JobBinding
from backup_engine.profile_store.errors import InvalidRuleError
from backup_engine.profile_store.rules import (
    normalize_patterns,
    normalize_template_selection_rules,
)
from backup_engine.template_policy import TemplatePolicy, TemplateSelectionRules


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


def test_normalize_template_selection_rules_applies_to_both_lists() -> None:
    rules = TemplateSelectionRules(include=(" a/** ",), exclude=(r"b\*.tmp",))
    normalized = normalize_template_selection_rules(rules)
    assert normalized.include == ("a/**",)
    assert normalized.exclude == ("b/*.tmp",)


def test_template_policy_carries_selection_rules_and_compression() -> None:
    rules = TemplateSelectionRules(include=("a/**",), exclude=("b/**",))
    policy = TemplatePolicy(selection_rules=rules, compression="zip")

    assert policy.selection_rules == TemplateSelectionRules(
        include=("a/**",),
        exclude=("b/**",),
    )
    assert policy.compression == "zip"


def test_job_binding_contains_only_identity_template_reference_and_target_binding() -> None:
    binding = JobBinding(
        job_id="job-1",
        job_name="Example Job",
        template_id="template-1",
        source_root="C:/games/world",
    )

    assert binding.job_id == "job-1"
    assert binding.job_name == "Example Job"
    assert binding.template_id == "template-1"
    assert binding.source_root == "C:/games/world"
