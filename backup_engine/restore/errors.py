from __future__ import annotations

from backup_engine.errors import WcbtError


class RestoreError(WcbtError):
    """Base class for restore-domain errors."""


class RestoreManifestError(RestoreError):
    """Raised when a run manifest is missing required fields or violates invariants."""


class RestoreSafetyError(RestoreError):
    """Raised when restore intent violates safety constraints."""


class RestoreArtifactError(RestoreError):
    """Raised when restore artifacts cannot be written deterministically."""


class RestoreMaterializationError(RestoreError):
    """Raised when restore candidate materialization fails."""


class RestoreIntentError(RestoreError):
    """Raised when restore intent is invalid (e.g., destination root problems)."""
