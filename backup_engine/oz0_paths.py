"""
Authoritative OZ0 artifact path derivation.

Notes
-----
OZ0 artifact placement is target-relative by contract. All runtime OZ0 flows
must derive their final artifact root from the selected backup source root, not
from profile-owned archive directories.
"""

from __future__ import annotations

from pathlib import Path


def resolve_oz0_artifact_root(source_root: Path) -> Path:
    """
    Return the authoritative final OZ0 artifact directory for a source root.

    Parameters
    ----------
    source_root : Path
        Validated backup source root.

    Returns
    -------
    Path
        Target-relative OZ0 artifact root.
    """
    return source_root.parent / "OZ0"
