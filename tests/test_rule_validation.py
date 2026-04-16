from __future__ import annotations

import pytest

from backup_engine.job_binding import JobBinding
from backup_engine.profile_store.api import JobBackupDefaults, RuleSet
from backup_engine.profile_store.errors import InvalidRuleError
from backup_engine.profile_store.rules import normalize_patterns, normalize_rules
from backup_engine.template_policy import TemplateSelectionRules


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


def test_ruleset_exposes_template_selection_rules_view() -> None:
    rules = RuleSet(include=("a/**",), exclude=("b/**",))

    assert rules.to_template_selection_rules() == TemplateSelectionRules(
        include=("a/**",),
        exclude=("b/**",),
    )


def test_job_backup_defaults_exposes_template_policy_without_source_root() -> None:
    defaults = JobBackupDefaults(source_root="C:/games/world", compression="zip")

    derived_policy = defaults.to_template_policy(RuleSet(include=("a/**",), exclude=("b/**",)))

    assert derived_policy.selection_rules == TemplateSelectionRules(
        include=("a/**",),
        exclude=("b/**",),
    )
    assert derived_policy.compression == "zip"


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
