"""Job-owned binding contracts for WCBT.

Notes
-----
Job is the authoritative owner of live binding only in WCBT. A Job binds the
current Template policy to a live target and carries stable Job identity.
Template remains the authoritative owner of backup policy. Run remains the
authoritative owner of execution facts. Convenience defaults, schedule meaning,
and compatibility residue must not be modeled as Job meaning.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class JobBinding:
    """
    Authoritative Job-owned binding of Template policy to a live target.

    Attributes
    ----------
    job_id:
        Stable identifier for the live Job definition.
    job_name:
        Human-friendly display name for the Job.
    template_id:
        Reference to the currently bound Template policy.
    source_root:
        Job-owned live target binding for the current backup definition.

    Notes
    -----
    This contract intentionally excludes:

    - Template-owned policy such as selection rules or compression
    - schedule trigger metadata or schedule meaning
    - run metadata or artifact-instance facts
    - UI convenience defaults
    - runtime operational residue
    - compatibility mirror payloads
    """

    job_id: str
    job_name: str
    template_id: str
    source_root: str
