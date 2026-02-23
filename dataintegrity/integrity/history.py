"""
integrity/history.py
--------------------
Local, append-only integrity audit history tracker.

History is stored as **one JSON file per dataset fingerprint** in
``~/.dataintegrity/history/<fingerprint>.json``.  Each file is a JSON array
of audit summary objects appended after every call to :meth:`IntegrityHistoryTracker.record`.

No external database is required — the store is entirely local and file-based.

Usage
-----
::

    from dataintegrity.integrity.history import IntegrityHistoryTracker

    tracker = IntegrityHistoryTracker()
    tracker.record(audit_result)

    history = tracker.load_history(fingerprint)
    trend   = tracker.get_score_trend(fingerprint)
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from dataintegrity.core.result_schema import DatasetAuditResult

# ---------------------------------------------------------------------------
# Default storage root
# ---------------------------------------------------------------------------

_DEFAULT_HISTORY_ROOT: Path = Path.home() / ".dataintegrity" / "history"


class IntegrityHistoryTracker:
    """
    Local, append-only tracker for dataset audit history.

    Each dataset (identified by its SHA-256 fingerprint) gets a dedicated
    JSON file.  Entries are appended on each :meth:`record` call, giving a
    chronological audit trail.

    Args:
        storage_root: Directory in which history files are stored.
                      Defaults to ``~/.dataintegrity/history``.

    Example::

        tracker = IntegrityHistoryTracker()
        tracker.record(audit_result)
        trend = tracker.get_score_trend(fingerprint)
        # [87.3, 89.1, 91.0]
    """

    def __init__(self, storage_root: Optional[Path] = None) -> None:
        self.storage_root: Path = storage_root or _DEFAULT_HISTORY_ROOT

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _history_path(self, fingerprint: str) -> Path:
        """Return the Path for a given dataset fingerprint's history file."""
        return self.storage_root / f"{fingerprint}.json"

    def _ensure_dir(self) -> None:
        """Create the storage directory if it does not exist."""
        self.storage_root.mkdir(parents=True, exist_ok=True)

    def _read_file(self, path: Path) -> List[Dict[str, Any]]:
        """
        Read and parse an existing history file, returning an empty list on any error.
        """
        try:
            with path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
                if isinstance(data, list):
                    return data
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            pass
        return []

    def _write_file(self, path: Path, entries: List[Dict[str, Any]]) -> None:
        """Atomically write the history list to disk as JSON."""
        path.write_text(
            json.dumps(entries, indent=2, default=str),
            encoding="utf-8",
        )

    def _summary_from_result(
        self, result: "DatasetAuditResult"
    ) -> Dict[str, Any]:
        """
        Extract a minimal summary entry from a :class:`DatasetAuditResult`.

        We intentionally store only the manifest + score to keep history files
        small.  Full audit details are available in the :class:`DatasetAuditResult`
        itself.
        """
        return {
            "run_id": result.manifest.run_id,
            "timestamp": result.manifest.timestamp,
            "dataset_fingerprint": result.manifest.dataset_fingerprint,
            "overall_score": result.overall_score,
            "rules_executed": result.manifest.rules_executed,
            "sdk_version": result.manifest.sdk_version,
            "source": result.source,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record(self, result: "DatasetAuditResult") -> Path:
        """
        Append an audit result to the history file for its dataset fingerprint.

        Args:
            result: A :class:`~dataintegrity.core.result_schema.DatasetAuditResult`
                    returned by :meth:`~dataintegrity.integrity.engine.IntegrityEngine.run`.

        Returns:
            The :class:`~pathlib.Path` of the history file that was updated.
        """
        self._ensure_dir()
        fingerprint = result.manifest.dataset_fingerprint
        path = self._history_path(fingerprint)
        entries = self._read_file(path)
        entries.append(self._summary_from_result(result))
        self._write_file(path, entries)
        return path

    def load_history(self, fingerprint: str) -> List[Dict[str, Any]]:
        """
        Load the full audit history for a dataset by its fingerprint.

        Args:
            fingerprint: SHA-256 fingerprint of the dataset to look up.

        Returns:
            List of summary dicts in chronological order (oldest first).
            Returns an empty list if no history exists yet.
        """
        path = self._history_path(fingerprint)
        return self._read_file(path)

    def get_score_trend(self, fingerprint: str) -> List[float]:
        """
        Return an ordered list of :attr:`~DatasetAuditResult.overall_score` values.

        Args:
            fingerprint: SHA-256 fingerprint of the dataset.

        Returns:
            List of float scores in chronological order (oldest first).
            Returns an empty list if no history exists.

        Example::

            trend = tracker.get_score_trend(fp)
            # [82.5, 87.3, 91.0] — improving over time
        """
        return [
            entry["overall_score"]
            for entry in self.load_history(fingerprint)
            if isinstance(entry.get("overall_score"), (int, float))
        ]

    def list_tracked_fingerprints(self) -> List[str]:
        """
        List all dataset fingerprints that have stored history.

        Returns:
            List of fingerprint strings (filename stems from the storage dir).
        """
        if not self.storage_root.exists():
            return []
        return [p.stem for p in self.storage_root.glob("*.json")]
