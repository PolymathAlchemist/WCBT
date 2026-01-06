"""Domain exceptions for ProfileStore."""

from __future__ import annotations


class ProfileStoreError(RuntimeError):
    """Base error for profile store operations."""


class UnknownJobError(ProfileStoreError):
    """Raised when a job_id is not known to the store."""


class InvalidRuleError(ProfileStoreError):
    """Raised when a rule pattern violates invariants."""
