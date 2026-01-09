from __future__ import annotations

import tarfile
import zipfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable

import zstandard as zstd


class CompressionFormat(str, Enum):
    """
    Supported compression formats for derived backup artifacts.
    """

    TAR_ZST = "tar.zst"
    ZIP = "zip"
    NONE = "none"


@dataclass(frozen=True, slots=True)
class CompressionResult:
    """
    Result of compressing a run directory.

    Attributes
    ----------
    format:
        Compression format actually used.
    archive_path:
        Path to the created archive file.
    """

    format: CompressionFormat
    archive_path: Path


def compress_run_directory(
    *,
    run_root: Path,
    output_path: Path,
    format: CompressionFormat,
    overwrite: bool = False,
) -> CompressionResult:
    """
    Create a derived compressed artifact from a materialized run directory.

    Parameters
    ----------
    run_root:
        Materialized run directory (contains manifest.json and plan.txt).
    output_path:
        Target archive file path (e.g., .../20260101_010203Z.zip).
    format:
        Compression format to use.
    overwrite:
        If True, overwrite an existing output_path.

    Returns
    -------
    CompressionResult
        Compression result.

    Raises
    ------
    ValueError
        If inputs are invalid.
    OSError
        If filesystem writes fail.
    RuntimeError
        If tar.zst is requested but the zstandard module is not available.
    """
    run_root = run_root.resolve()
    if not run_root.exists() or not run_root.is_dir():
        raise ValueError(f"run_root must be an existing directory: {run_root}")

    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        if not overwrite:
            raise ValueError(f"Refusing to overwrite existing archive: {output_path}")
        output_path.unlink()

    if format is CompressionFormat.NONE:
        raise ValueError("format must not be 'none' when compressing")

    if format is CompressionFormat.ZIP:
        _write_zip(run_root=run_root, output_path=output_path)
        return CompressionResult(format=CompressionFormat.ZIP, archive_path=output_path)

    if format is CompressionFormat.TAR_ZST:
        _write_tar_zst(run_root=run_root, output_path=output_path)
        return CompressionResult(format=CompressionFormat.TAR_ZST, archive_path=output_path)

    raise ValueError(f"Unsupported compression format: {format!r}")


def extract_archive(
    *,
    archive_path: Path,
    destination_dir: Path,
) -> Path:
    """
    Extract a supported archive into destination_dir.

    Parameters
    ----------
    archive_path:
        Path to .zip or .tar.zst archive.
    destination_dir:
        Directory to extract into (created if missing).

    Returns
    -------
    pathlib.Path
        The destination_dir after extraction.

    Raises
    ------
    ValueError
        If the archive extension is unsupported.
    RuntimeError
        If tar.zst extraction requires zstandard but it is unavailable.
    OSError
        If extraction fails.
    """
    archive_path = archive_path.resolve()
    destination_dir = destination_dir.resolve()
    destination_dir.mkdir(parents=True, exist_ok=True)

    lower = archive_path.name.lower()
    if lower.endswith(".zip"):
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(destination_dir)
        return destination_dir

    if lower.endswith(".tar.zst") or lower.endswith(".tarzst"):
        _extract_tar_zst(archive_path=archive_path, destination_dir=destination_dir)
        return destination_dir

    raise ValueError(f"Unsupported archive type: {archive_path}")


def _iter_files_for_archive(run_root: Path) -> Iterable[Path]:
    for p in run_root.rglob("*"):
        if p.is_file():
            yield p


def _write_zip(*, run_root: Path, output_path: Path) -> None:
    # Store files with relative paths rooted at run_root name, so extraction recreates a run folder.
    base_name = run_root.name
    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in _iter_files_for_archive(run_root):
            rel_inside_run = file_path.relative_to(run_root)
            arcname = Path(base_name) / rel_inside_run
            zf.write(file_path, arcname.as_posix())


def _write_tar_zst(*, run_root: Path, output_path: Path) -> None:
    base_name = run_root.name
    with output_path.open("wb") as raw:
        cctx = zstd.ZstdCompressor()
        with cctx.stream_writer(raw) as zst_stream:
            with tarfile.open(fileobj=zst_stream, mode="w|") as tf:
                for file_path in _iter_files_for_archive(run_root):
                    rel_inside_run = file_path.relative_to(run_root)
                    arcname = (Path(base_name) / rel_inside_run).as_posix()
                    tf.add(file_path, arcname=arcname, recursive=False)


def _extract_tar_zst(*, archive_path: Path, destination_dir: Path) -> None:
    with archive_path.open("rb") as raw:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(raw) as reader:
            with tarfile.open(fileobj=reader, mode="r|") as tf:
                tf.extractall(destination_dir)  # noqa: S202
