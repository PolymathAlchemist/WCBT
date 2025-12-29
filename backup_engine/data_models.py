"""Data models for WCBT.

This module defines the canonical, typed representation of a backup manifest.
Manifests are the source of truth in WCBT; all other indexes (SQLite) are derived.

The models in this module are intentionally standard-library-only (dataclasses)
to keep the core engine lightweight and deterministic.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Self
from uuid import UUID

ISO_8601_UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class ManifestSchemaVersion(int, Enum):
    """Supported manifest schema versions."""

    V1 = 1


class SourceType(str, Enum):
    """Supported source types (v1)."""

    DEDICATED_SERVER = "dedicated_server"


class ArchiveFormat(str, Enum):
    """Supported archive formats (v1)."""

    ZIP = "zip"
    TAR_GZ = "tar_gz"
    TAR_ZST = "tar_zst"


def utc_now() -> datetime:
    """Return the current UTC time as an aware datetime.

    Returns
    -------
    datetime
        A timezone-aware datetime with UTC timezone.
    """

    return datetime.now(tz=timezone.utc)


def datetime_to_iso_utc(dt: datetime) -> str:
    """Serialize a datetime as a UTC ISO-8601 string.

    Parameters
    ----------
    dt
        A timezone-aware datetime.

    Returns
    -------
    str
        ISO-8601 UTC timestamp, normalized to the `Z` suffix.

    Raises
    ------
    ValueError
        If `dt` is naive (has no timezone).
    """

    if dt.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return dt.astimezone(timezone.utc).strftime(ISO_8601_UTC_FORMAT)


def datetime_from_iso_utc(value: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp as an aware datetime.

    Parameters
    ----------
    value
        ISO-8601 timestamp in the exact format ``YYYY-MM-DDTHH:MM:SSZ``.

    Returns
    -------
    datetime
        A timezone-aware datetime with UTC timezone.

    Raises
    ------
    ValueError
        If parsing fails or the format is not the expected canonical format.
    """

    dt = datetime.strptime(value, ISO_8601_UTC_FORMAT)
    return dt.replace(tzinfo=timezone.utc)


def _require_keys(payload: Mapping[str, Any], keys: set[str], *, context: str) -> None:
    missing = keys.difference(payload.keys())
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"Missing required keys in {context}: {missing_str}")


@dataclass(frozen=True, slots=True)
class EnvironmentInfo:
    """Minecraft environment metadata captured at backup time."""

    minecraft_version: str
    loader: str
    loader_version: str
    java_version: str

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        """Construct an :class:`EnvironmentInfo` from a mapping."""

        _require_keys(
            payload,
            {"minecraft_version", "loader", "loader_version", "java_version"},
            context="environment",
        )
        return cls(
            minecraft_version=str(payload["minecraft_version"]),
            loader=str(payload["loader"]),
            loader_version=str(payload["loader_version"]),
            java_version=str(payload["java_version"]),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert this model to a JSON-serializable dict."""

        return {
            "minecraft_version": self.minecraft_version,
            "loader": self.loader,
            "loader_version": self.loader_version,
            "java_version": self.java_version,
        }


@dataclass(frozen=True, slots=True)
class DedicatedServerSource:
    """Source metadata for a Fabric dedicated server world."""

    type: SourceType
    server_root: str
    world_folder: str

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        """Construct a :class:`DedicatedServerSource` from a mapping."""

        _require_keys(payload, {"type", "server_root", "world_folder"}, context="source")
        source_type = SourceType(str(payload["type"]))
        if source_type is not SourceType.DEDICATED_SERVER:
            raise ValueError(f"Unsupported source.type: {source_type}")
        return cls(
            type=source_type,
            server_root=str(payload["server_root"]),
            world_folder=str(payload["world_folder"]),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert this model to a JSON-serializable dict."""

        return {
            "type": self.type.value,
            "server_root": self.server_root,
            "world_folder": self.world_folder,
        }


@dataclass(frozen=True, slots=True)
class ArchiveInfo:
    """Archive metadata for a backup."""

    format: ArchiveFormat
    filename: str
    size_bytes: int
    sha256: str | None = None

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        """Construct :class:`ArchiveInfo` from a mapping."""

        _require_keys(payload, {"format", "filename", "size_bytes"}, context="archive")
        archive_format = ArchiveFormat(str(payload["format"]))
        sha256_value = payload.get("sha256")
        return cls(
            format=archive_format,
            filename=str(payload["filename"]),
            size_bytes=int(payload["size_bytes"]),
            sha256=str(sha256_value) if sha256_value is not None else None,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert this model to a JSON-serializable dict."""

        payload: dict[str, Any] = {
            "format": self.format.value,
            "filename": self.filename,
            "size_bytes": self.size_bytes,
        }
        if self.sha256 is not None:
            payload["sha256"] = self.sha256
        return payload


@dataclass(frozen=True, slots=True)
class BackupMetadata:
    """User-facing metadata for organizing and protecting backups."""

    tags: list[str] = field(default_factory=list)
    note: str = ""
    epoch: str = ""
    pinned: bool = False

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        """Construct :class:`BackupMetadata` from a mapping."""

        tags_raw = payload.get("tags", [])
        if not isinstance(tags_raw, list):
            raise ValueError("metadata.tags must be a list")
        tags = [str(x) for x in tags_raw]
        return cls(
            tags=tags,
            note=str(payload.get("note", "")),
            epoch=str(payload.get("epoch", "")),
            pinned=bool(payload.get("pinned", False)),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert this model to a JSON-serializable dict."""

        return {
            "tags": list(self.tags),
            "note": self.note,
            "epoch": self.epoch,
            "pinned": self.pinned,
        }


@dataclass(frozen=True, slots=True)
class BackupManifest:
    """Canonical backup manifest (schema v1).

    Notes
    -----
    - Manifests are canonical in WCBT. SQLite and other indexes must be rebuildable from manifests.
    - Manifests must never contain secrets. Credential IDs are acceptable; secret material is not.
    """

    schema_version: ManifestSchemaVersion
    backup_id: UUID
    world_id: UUID
    created_at_utc: datetime
    profile_name: str

    environment: EnvironmentInfo
    source: DedicatedServerSource
    archive: ArchiveInfo
    metadata: BackupMetadata = field(default_factory=BackupMetadata)
    telemetry: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        """Validate invariant rules for this manifest.

        Raises
        ------
        ValueError
            If the manifest violates schema constraints or invariants.
        """

        if self.schema_version is not ManifestSchemaVersion.V1:
            raise ValueError(f"Unsupported schema_version: {self.schema_version}")

        if self.created_at_utc.tzinfo is None:
            raise ValueError("created_at_utc must be timezone-aware")
        # Basic guardrails: deterministic, non-empty identifiers
        if not str(self.profile_name).strip():
            raise ValueError("profile_name must be non-empty")

        if self.archive.size_bytes < 0:
            raise ValueError("archive.size_bytes must be >= 0")

        if self.archive.sha256 is not None:
            sha = self.archive.sha256.lower()
            if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
                raise ValueError("archive.sha256 must be a lowercase hex SHA-256")

    @classmethod
    def new(
        cls,
        *,
        backup_id: UUID,
        world_id: UUID,
        created_at_utc: datetime | None,
        profile_name: str,
        environment: EnvironmentInfo,
        source: DedicatedServerSource,
        archive: ArchiveInfo,
        metadata: BackupMetadata | None = None,
        telemetry: Mapping[str, Any] | None = None,
    ) -> Self:
        """Create a new manifest with consistent defaults.

        Parameters
        ----------
        backup_id
            Unique ID for this snapshot.
        world_id
            Stable ID for the world.
        created_at_utc
            Timestamp for the backup. If ``None``, uses current UTC time.
        profile_name
            Profile name associated with this backup.
        environment
            Minecraft and runtime environment information.
        source
            Backup source description (v1: dedicated server).
        archive
            Archive description for the produced backup file.
        metadata
            Optional user metadata such as tags, note, pinned status.
        telemetry
            Optional telemetry payload. Failures in telemetry are non-fatal and should not block backups.

        Returns
        -------
        BackupManifest
            A validated manifest instance.
        """

        manifest = cls(
            schema_version=ManifestSchemaVersion.V1,
            backup_id=backup_id,
            world_id=world_id,
            created_at_utc=created_at_utc or utc_now(),
            profile_name=profile_name,
            environment=environment,
            source=source,
            archive=archive,
            metadata=metadata or BackupMetadata(),
            telemetry=dict(telemetry or {}),
        )
        manifest.validate()
        return manifest

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        """Parse a manifest from a JSON mapping.

        Parameters
        ----------
        payload
            A mapping that matches the manifest schema.

        Returns
        -------
        BackupManifest
            Parsed and validated manifest.

        Raises
        ------
        ValueError
            If the payload is missing required fields or fails validation.
        """

        _require_keys(
            payload,
            {
                "schema_version",
                "backup_id",
                "world_id",
                "created_at_utc",
                "profile_name",
                "environment",
                "source",
                "archive",
                "metadata",
            },
            context="manifest root",
        )

        schema_version = ManifestSchemaVersion(int(payload["schema_version"]))
        manifest = cls(
            schema_version=schema_version,
            backup_id=UUID(str(payload["backup_id"])),
            world_id=UUID(str(payload["world_id"])),
            created_at_utc=datetime_from_iso_utc(str(payload["created_at_utc"])),
            profile_name=str(payload["profile_name"]),
            environment=EnvironmentInfo.from_dict(payload["environment"]),
            source=DedicatedServerSource.from_dict(payload["source"]),
            archive=ArchiveInfo.from_dict(payload["archive"]),
            metadata=BackupMetadata.from_dict(payload.get("metadata", {})),
            telemetry=dict(payload.get("telemetry", {})),
        )
        manifest.validate()
        return manifest

    def to_dict(self) -> dict[str, Any]:
        """Convert this manifest to a JSON-serializable dictionary."""

        self.validate()
        payload: dict[str, Any] = {
            "schema_version": int(self.schema_version.value),
            "backup_id": str(self.backup_id),
            "world_id": str(self.world_id),
            "created_at_utc": datetime_to_iso_utc(self.created_at_utc),
            "profile_name": self.profile_name,
            "environment": self.environment.to_dict(),
            "source": self.source.to_dict(),
            "archive": self.archive.to_dict(),
            "metadata": self.metadata.to_dict(),
        }
        if self.telemetry:
            payload["telemetry"] = dict(self.telemetry)
        return payload


def compute_sha256_hex(file_bytes: bytes) -> str:
    """Compute SHA-256 hex digest for bytes.

    Parameters
    ----------
    file_bytes
        The bytes to hash.

    Returns
    -------
    str
        Lowercase hex SHA-256 digest.
    """

    return hashlib.sha256(file_bytes).hexdigest()
