"""
Restore-domain exceptions.

This module defines the exception hierarchy used by restore planning, staging,
verification, and promotion. These errors are intentionally domain-specific to
make failure modes explicit and testable.
"""

from __future__ import annotations

from backup_engine.errors import WcbtError


class RestoreError(WcbtError):
    """
    Base class for restore-domain errors.

    Notes
    -----
    All restore-specific exceptions inherit from this type to allow callers to
    catch restore failures without matching unrelated WCBT errors.
    """


class RestoreManifestError(RestoreError):
    """
    Raised when a run manifest is invalid for restore planning.

    Notes
    -----
    This indicates missing required fields or violated invariants in the source
    run manifest (e.g., schema/version mismatch or malformed operation entries).
    """


class RestoreSafetyError(RestoreError):
    """
    Raised when restore intent violates safety constraints.

    Notes
    -----
    Examples include attempts to restore outside the destination root or unsafe
    path relationships detected during planning.
    """


class RestoreArtifactError(RestoreError):
    """
    Raised when restore artifacts cannot be written deterministically.

    Notes
    -----
    Artifacts are inspectable on-disk outputs (e.g., plan JSON, JSONL results).
    Failures here are typically filesystem or serialization related.
    """


class RestoreMaterializationError(RestoreError):
    """
    Raised when restore candidate materialization fails.

    Notes
    -----
    This indicates the plan could not be translated into concrete candidate
    operations (e.g., archive paths could not be resolved).
    """


class RestoreIntentError(RestoreError):
    """
    Raised when restore intent is invalid.

    Notes
    -----
    Examples include destination root problems or inconsistent user inputs that
    prevent planning from proceeding safely.
    """


class RestoreConflictError(RestoreError):
    """
    Raised when restore conflicts with existing destination content.

    Notes
    -----
    This is primarily used for add-only restores when a destination path already
    exists and overwriting is not permitted.
    """
