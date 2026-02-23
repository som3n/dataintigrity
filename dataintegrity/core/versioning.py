"""
core/versioning.py
------------------
Dataset versioning abstraction for the dataintegrity SDK (v0.2).

A :class:`DatasetVersion` captures a deterministic snapshot of a dataset's
quality profile at a specific point in time.  Versions can be serialised to
JSON and reloaded from JSON, enabling persistent historical comparison.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from dataintegrity.core.dataset import Dataset


class DatasetVersion:
    """
    Immutable snapshot of a dataset's integrity profile at a point in time.

    The ``version_id`` is **deterministic**: it is derived from the SHA-256
    hash of the dataset fingerprint concatenated with the ISO-8601 timestamp,
    so the same dataset audited at the same moment always produces the same ID.

    Attributes:
        version_id:       Deterministic identifier (hex string).
        timestamp:        UTC datetime when the version was created.
        fingerprint:      SHA-256 data fingerprint of the underlying dataset.
        data_score:       Composite quality score (0–100), or ``None`` if not
                          yet computed.
        dimension_scores: Per-dimension quality scores (0–1 floats).
        source:           Data origin identifier (file path, table name, …).
        raw_df:           Optional reference to the underlying pandas DataFrame
                          (retained in-memory only; not persisted to disk).
    """

    def __init__(
        self,
        dataset: Dataset,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """
        Create a DatasetVersion from a profiled Dataset.

        Args:
            dataset:   A :class:`~dataintegrity.core.dataset.Dataset` that has
                       already been run through the integrity engine so that
                       ``dataset.profile`` is populated.
            timestamp: UTC datetime to stamp this version.  Defaults to
                       ``datetime.now(timezone.utc)``.
        """
        self.timestamp: datetime = timestamp or datetime.now(timezone.utc)
        self.fingerprint: str = dataset.fingerprint
        self.data_score: Optional[float] = dataset.profile.get("data_score")
        self.dimension_scores: Dict[str, float] = dict(
            dataset.profile.get("dimension_scores", {})
        )
        self.source: Optional[str] = dataset.source

        # Deterministic version ID
        self.version_id: str = self._make_version_id(
            self.fingerprint, self.timestamp
        )

        # Keep a transient reference to the raw data for in-process drift
        # comparison.  This is NOT serialised to disk.
        self._raw_df = dataset.df.copy()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_version_id(fingerprint: str, timestamp: datetime) -> str:
        """Produce a deterministic, short hex identifier."""
        ts_str = timestamp.isoformat()
        payload = f"{fingerprint}|{ts_str}".encode("utf-8")
        return hashlib.sha256(payload).hexdigest()[:16]

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialise this version to a plain JSON-compatible dictionary.

        Returns:
            Dict with string-serialisable values only (no pandas objects).
        """
        return {
            "version_id": self.version_id,
            "timestamp": self.timestamp.isoformat(),
            "fingerprint": self.fingerprint,
            "data_score": self.data_score,
            "dimension_scores": self.dimension_scores,
            "source": self.source,
        }

    def to_json(self, indent: int = 2) -> str:
        """
        Serialise this version to a pretty-printed JSON string.

        Args:
            indent: JSON indentation width (default: 2).

        Returns:
            JSON string representation.
        """
        return json.dumps(self.to_dict(), indent=indent, default=str)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatasetVersion":
        """
        Reconstruct a DatasetVersion from a serialised dictionary.

        Note:
            The ``_raw_df`` attribute will be ``None`` on deserialized versions
            (no raw data is stored to disk).  Drift comparison will fall back
            to dimension-level deltas in that case.

        Args:
            data: Dictionary produced by :meth:`to_dict`.

        Returns:
            A new :class:`DatasetVersion` instance (without raw DataFrame).
        """
        instance = object.__new__(cls)
        instance.version_id = data["version_id"]
        instance.timestamp = datetime.fromisoformat(data["timestamp"])
        instance.fingerprint = data["fingerprint"]
        instance.data_score = data.get("data_score")
        instance.dimension_scores = data.get("dimension_scores", {})
        instance.source = data.get("source")
        instance._raw_df = None  # not persisted
        return instance

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        ts = self.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
        return (
            f"DatasetVersion(id={self.version_id!r}, "
            f"score={self.data_score}, ts={ts!r}, source={self.source!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DatasetVersion):
            return NotImplemented
        return self.version_id == other.version_id
