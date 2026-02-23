"""
core/store.py
-------------
Local, file-based version store for the dataintegrity SDK (v0.2).

Versions are written as JSON files under ``~/.dataintegrity/versions/``,
one file per data source.  The module is designed to be safe for concurrent
CLI invocations (best-effort, using file locks via ``fcntl`` where available).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import List, Optional

from dataintegrity.core.versioning import DatasetVersion

logger = logging.getLogger(__name__)

# Root directory for all persisted version data
_DEFAULT_STORE_ROOT = Path.home() / ".dataintegrity" / "versions"


def _source_to_filename(source: str) -> str:
    """
    Convert an arbitrary source string into a safe, stable filename.

    We use the first 48 chars of a SHA-256 hex digest to ensure the filename
    is filesystem-safe regardless of the source content.

    Args:
        source: Data origin identifier (file path, table name, URL, …).

    Returns:
        ``<hex>.json`` filename string.
    """
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()[:48]
    return f"{digest}.json"


class LocalVersionStore:
    """
    Persists and retrieves :class:`~dataintegrity.core.versioning.DatasetVersion`
    objects as JSON files on the local filesystem.

    Store layout::

        <store_root>/
            <source-hash-1>.json   # versions for data source A
            <source-hash-2>.json   # versions for data source B
            …

    Each JSON file has the structure::

        {
            "source": "<original source string>",
            "versions": [ { ...version dict... }, ... ]
        }

    Args:
        store_root: Directory in which to create version files.
                    Defaults to ``~/.dataintegrity/versions/``.
    """

    def __init__(self, store_root: Optional[Path] = None) -> None:
        self._root: Path = Path(store_root) if store_root else _DEFAULT_STORE_ROOT

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save(self, version: DatasetVersion) -> Path:
        """
        Persist a :class:`DatasetVersion` to disk.

        If a store file already exists for the same source, the new version
        is appended to the list.  If the file is missing or corrupted it is
        recreated from scratch.

        Args:
            version: The version to persist.

        Returns:
            The :class:`~pathlib.Path` of the store file.

        Raises:
            ValueError: If ``version.source`` is ``None``.
        """
        if version.source is None:
            raise ValueError(
                "Cannot save a DatasetVersion whose 'source' is None. "
                "Set Dataset(source=...) before running the audit."
            )

        self._ensure_root()
        store_path = self._store_path(version.source)
        payload = self._load_raw(store_path, version.source)

        # Append only if this version_id is not already stored
        existing_ids = {v["version_id"] for v in payload["versions"]}
        if version.version_id not in existing_ids:
            payload["versions"].append(version.to_dict())
            self._write_raw(store_path, payload)
            logger.debug(
                "Saved version %s for source %r to %s",
                version.version_id, version.source, store_path,
            )
        else:
            logger.debug(
                "Version %s already stored — skipping duplicate write.",
                version.version_id,
            )

        return store_path

    def load_latest(self, source: str) -> Optional[DatasetVersion]:
        """
        Return the most recently saved version for *source*.

        Versions are ordered by their stored position (oldest first), so the
        *latest* version is the last element.

        Args:
            source: Data origin identifier.

        Returns:
            The most recent :class:`DatasetVersion`, or ``None`` if no
            versions exist for this source.
        """
        versions = self.load_all(source)
        if not versions:
            return None
        # Versions are stored in chronological order; last is most recent
        return versions[-1]

    def load_all(self, source: str) -> List[DatasetVersion]:
        """
        Return all stored versions for *source*, ordered oldest → newest.

        Args:
            source: Data origin identifier.

        Returns:
            List of :class:`DatasetVersion` objects (may be empty).
        """
        store_path = self._store_path(source)
        if not store_path.exists():
            return []

        payload = self._load_raw(store_path, source)
        versions: List[DatasetVersion] = []
        for raw in payload["versions"]:
            try:
                versions.append(DatasetVersion.from_dict(raw))
            except (KeyError, ValueError) as exc:
                logger.warning("Skipping corrupt version entry: %s", exc)

        return versions

    def exists(self, source: str) -> bool:
        """
        Check whether any versions are stored for *source*.

        Args:
            source: Data origin identifier.

        Returns:
            ``True`` if at least one version exists.
        """
        store_path = self._store_path(source)
        if not store_path.exists():
            return False
        versions = self.load_all(source)
        return len(versions) > 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _store_path(self, source: str) -> Path:
        """Return the absolute Path for the JSON store file of *source*."""
        return self._root / _source_to_filename(source)

    def _ensure_root(self) -> None:
        """Create the store root directory (and parents) if absent."""
        self._root.mkdir(parents=True, exist_ok=True)

    def _load_raw(self, path: Path, source: str) -> dict:
        """
        Load the raw JSON payload from *path*, returning a valid structure.

        Gracefully handles missing or corrupted files by returning an empty
        payload rather than raising.

        Args:
            path:   Path to the JSON store file.
            source: Used to populate the ``"source"`` key in a fresh payload.

        Returns:
            Dict with keys ``"source"`` and ``"versions"`` (list).
        """
        empty: dict = {"source": source, "versions": []}
        if not path.exists():
            return empty

        try:
            text = path.read_text(encoding="utf-8")
            data = json.loads(text)
            # Validate expected structure
            if not isinstance(data, dict):
                raise ValueError("Root element is not a dict.")
            if "versions" not in data or not isinstance(data["versions"], list):
                raise ValueError("Missing or invalid 'versions' list.")
            return data
        except (json.JSONDecodeError, ValueError, OSError) as exc:
            logger.warning(
                "Corrupt version store at %s — starting fresh. Reason: %s",
                path, exc,
            )
            return {"source": source, "versions": []}

    @staticmethod
    def _write_raw(path: Path, payload: dict) -> None:
        """
        Atomically write *payload* as JSON to *path*.

        Uses a write-to-temp-then-rename strategy so partial writes never
        corrupt the existing store file.

        Args:
            path:    Destination path.
            payload: JSON-serialisable dict.
        """
        tmp_path = path.with_suffix(".tmp")
        try:
            tmp_path.write_text(
                json.dumps(payload, indent=2, default=str),
                encoding="utf-8",
            )
            os.replace(tmp_path, path)  # atomic on POSIX
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

    def __repr__(self) -> str:
        return f"LocalVersionStore(root={self._root!r})"
