"""
Rule validation and normalization for ProfileStore.

This module provides deterministic, syntax-only checks for user-authored
glob patterns. It performs no filesystem access and does not scan archives.

Invariants
----------
- Root-relative: patterns must not start with '/' and must not contain a drive hint ':'
- Uses '/' separators (backslashes are normalized to '/')
- Empty patterns are dropped after stripping
"""

from __future__ import annotations

from dataclasses import replace
from typing import Sequence

from backup_engine.template_policy import TemplateSelectionRules

from .api import RuleSet
from .errors import InvalidRuleError


def normalize_patterns(values: Sequence[str]) -> tuple[str, ...]:
    """
    Normalize user-authored glob patterns.

    Parameters
    ----------
    values:
        Raw patterns from UI input.

    Returns
    -------
    tuple[str, ...]
        Normalized patterns, preserving order.

    Raises
    ------
    InvalidRuleError
        If any pattern violates invariants.
    """
    out: list[str] = []
    for raw in values:
        cleaned = str(raw).strip().replace("\\", "/")
        if not cleaned:
            continue
        if cleaned.startswith("/"):
            raise InvalidRuleError("Patterns must be root-relative and must not start with '/'.")
        if ":" in cleaned:
            raise InvalidRuleError("Patterns must be root-relative and must not contain ':'.")
        out.append(cleaned)
    return tuple(out)


def normalize_rules(rules: RuleSet) -> RuleSet:
    """
    Normalize and validate a RuleSet.

    Notes
    -----
    This is a syntax-only operation. It does not touch disk.

    Parameters
    ----------
    rules:
        Raw include/exclude patterns.

    Returns
    -------
    RuleSet
        Normalized rules.

    Raises
    ------
    InvalidRuleError
        If any pattern violates invariants.
    """
    return replace(
        rules,
        include=normalize_patterns(rules.include),
        exclude=normalize_patterns(rules.exclude),
    )


def normalize_template_selection_rules(
    selection_rules: TemplateSelectionRules,
) -> TemplateSelectionRules:
    """
    Normalize and validate Template-owned selection rules.

    Parameters
    ----------
    selection_rules:
        Template-owned include/exclude rules to normalize.

    Returns
    -------
    TemplateSelectionRules
        Normalized Template-owned selection rules.

    Raises
    ------
    InvalidRuleError
        If any pattern violates invariants.
    """

    return TemplateSelectionRules(
        include=normalize_patterns(selection_rules.include),
        exclude=normalize_patterns(selection_rules.exclude),
    )
