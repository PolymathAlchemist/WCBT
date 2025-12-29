"""Domain exception hierarchy for WCBT."""

from __future__ import annotations


class WCBTError(Exception):
    """Base exception for WCBT."""


class ManifestError(WCBTError):
    """Raised for manifest-related failures."""


class ManifestValidationError(ManifestError):
    """Raised when a manifest fails validation."""


class ManifestIOError(ManifestError):
    """Raised when a manifest cannot be read or written."""
