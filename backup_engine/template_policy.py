"""Template-owned backup policy contracts for WCBT.

Notes
-----
Template is the authoritative owner of backup policy in WCBT.
Job remains responsible for target binding such as the live source root or
other target-identifying values. Run remains responsible for execution facts.
Convenience defaults and runtime metadata must not be modeled as Template data.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class TemplateSelectionRules:
    """
    Template-owned source selection rules within a Job-owned target root.

    Attributes
    ----------
    include:
        Root-relative include globs using '/' separators.
    exclude:
        Root-relative exclude globs using '/' separators.

    Notes
    -----
    These rules define what content is selected once a Job has identified the
    live target root. They do not identify or locate the target itself.
    """

    include: tuple[str, ...] = field(default_factory=tuple)
    exclude: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class TemplatePolicy:
    """
    Authoritative Template-owned backup policy for the initial boundary set.

    Attributes
    ----------
    selection_rules:
        Template-owned source selection rules applied within a Job-owned target
        binding.
    compression:
        Template-owned compression mode for backup artifact production.

    Notes
    -----
    This contract intentionally excludes:

    - Job target binding such as ``source_root`` or target path selection
    - Run metadata such as timestamps, outcomes, or artifact-instance facts
    - schedule trigger metadata
    - UI convenience defaults
    - runtime operational state
    """

    selection_rules: TemplateSelectionRules = field(default_factory=TemplateSelectionRules)
    compression: str = "none"
