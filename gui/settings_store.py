from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from backup_engine.paths_and_safety import default_data_root


@dataclass(frozen=True, slots=True)
class GuiSettings:
    """
    Persisted GUI settings.

    Notes
    -----
    These settings are a GUI control surface. They do not replace the engine's
    canonical on-disk run directories; they only control defaults.
    """

    data_root: Path | None
    archives_root: Path | None
    default_compression: str  # "tar.zst" | "zip" | "none"
    default_run_mode: str  # "plan" | "materialize" | "execute" | "execute+compress"
    restore_mode: str = "add-only"  # "add-only" | "overwrite"
    restore_verify: str = "size"  # "none" | "size"
    restore_dry_run: bool = True
    pre_restore_backup_compression: str = "zip"  # "tar.zst" | "zip" | "none"
    last_selected_run_job_id: str | None = None
    last_selected_restore_job_selection: str | None = None

    @staticmethod
    def defaults() -> "GuiSettings":
        return GuiSettings(
            data_root=None,
            archives_root=None,
            default_compression="none",
            default_run_mode="plan",
            restore_mode="add-only",
            restore_verify="size",
            restore_dry_run=True,
            pre_restore_backup_compression="zip",
            last_selected_run_job_id=None,
            last_selected_restore_job_selection=None,
        )


def _settings_path(data_root: Path | None) -> Path:
    root = default_data_root() if data_root is None else data_root
    return root / "gui_settings.json"


def load_gui_settings(*, data_root: Path | None) -> GuiSettings:
    """
    Load GUI settings from disk.

    Parameters
    ----------
    data_root:
        Engine data root. If None, the engine default is used.

    Returns
    -------
    GuiSettings
        Loaded settings, or defaults if missing/unreadable.
    """
    path = _settings_path(data_root)
    try:
        raw = path.read_text(encoding="utf-8")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            return GuiSettings.defaults()

        def _p(v: object) -> Path | None:
            if v is None:
                return None
            if isinstance(v, str) and v.strip():
                return Path(v)
            return None

        data_root_val = _p(payload.get("data_root"))
        archives_root_val = _p(payload.get("archives_root"))

        default_compression = payload.get("default_compression", "none")
        if default_compression not in {"tar.zst", "zip", "none"}:
            default_compression = "none"

        default_run_mode = payload.get("default_run_mode", "plan")
        if default_run_mode not in {"plan", "materialize", "execute", "execute+compress"}:
            default_run_mode = "plan"

        restore_mode = payload.get("restore_mode", "add-only")
        if restore_mode not in {"add-only", "overwrite"}:
            restore_mode = "add-only"

        restore_verify = payload.get("restore_verify", "size")
        if restore_verify not in {"none", "size"}:
            restore_verify = "size"

        restore_dry_run = payload.get("restore_dry_run", True)
        if not isinstance(restore_dry_run, bool):
            restore_dry_run = True

        pre_restore_backup_compression = payload.get("pre_restore_backup_compression", "zip")
        if pre_restore_backup_compression not in {"tar.zst", "zip", "none"}:
            pre_restore_backup_compression = "zip"

        last_selected_run_job_id = payload.get("last_selected_run_job_id")
        if not isinstance(last_selected_run_job_id, str) or not last_selected_run_job_id.strip():
            last_selected_run_job_id = None

        last_selected_restore_job_selection = payload.get("last_selected_restore_job_selection")
        if (
            not isinstance(last_selected_restore_job_selection, str)
            or not last_selected_restore_job_selection.strip()
        ):
            last_selected_restore_job_selection = None

        return GuiSettings(
            data_root=data_root_val,
            archives_root=archives_root_val,
            default_compression=str(default_compression),
            default_run_mode=str(default_run_mode),
            restore_mode=str(restore_mode),
            restore_verify=str(restore_verify),
            restore_dry_run=restore_dry_run,
            pre_restore_backup_compression=str(pre_restore_backup_compression),
            last_selected_run_job_id=last_selected_run_job_id,
            last_selected_restore_job_selection=last_selected_restore_job_selection,
        )
    except FileNotFoundError:
        return GuiSettings.defaults()
    except Exception:
        return GuiSettings.defaults()


def save_gui_settings(*, data_root: Path | None, settings: GuiSettings) -> None:
    """
    Save GUI settings to disk.

    Parameters
    ----------
    data_root:
        Engine data root. If None, the engine default is used.
    settings:
        Settings to persist.
    """
    path = _settings_path(data_root)
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "data_root": str(settings.data_root) if settings.data_root is not None else None,
        "archives_root": str(settings.archives_root)
        if settings.archives_root is not None
        else None,
        "default_compression": settings.default_compression,
        "default_run_mode": settings.default_run_mode,
        "restore_mode": settings.restore_mode,
        "restore_verify": settings.restore_verify,
        "restore_dry_run": settings.restore_dry_run,
        "pre_restore_backup_compression": settings.pre_restore_backup_compression,
        "last_selected_run_job_id": settings.last_selected_run_job_id,
        "last_selected_restore_job_selection": settings.last_selected_restore_job_selection,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
