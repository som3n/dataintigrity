"""
integrity/comparison.py
-----------------------
Historical comparison engine for the dataintegrity SDK (v0.2).

Compares two :class:`~dataintegrity.core.versioning.DatasetVersion` objects
and produces a structured delta report that includes:

* Composite DataScore delta
* Per-dimension score deltas
* Drift analysis (KS test) when raw data is available
* A human-readable severity classification

Severity Scale
--------------
+----------+----------------------------------+
| Level    | Condition                        |
+==========+==================================+
| stable   | |delta| < 2                      |
+----------+----------------------------------+
| minor    | 2 ≤ |delta| < 5                  |
+----------+----------------------------------+
| moderate | 5 ≤ |delta| < 10                 |
+----------+----------------------------------+
| critical | |delta| ≥ 10                     |
+----------+----------------------------------+
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from dataintegrity.core.config import IntegrityConfig, DEFAULT_CONFIG
from dataintegrity.core.versioning import DatasetVersion

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Severity helpers
# ---------------------------------------------------------------------------

def _classify_severity(score_delta: float) -> str:
    """
    Map a raw score delta to a severity label.

    Args:
        score_delta: Signed numeric delta (current − previous).  The absolute
                     value is used for the threshold comparison.

    Returns:
        One of ``"stable"``, ``"minor"``, ``"moderate"``, ``"critical"``.
    """
    abs_delta = abs(score_delta)
    if abs_delta < 2.0:
        return "stable"
    if abs_delta < 5.0:
        return "minor"
    if abs_delta < 10.0:
        return "moderate"
    return "critical"


# ---------------------------------------------------------------------------
# Comparator
# ---------------------------------------------------------------------------

class IntegrityComparator:
    """
    Compares two dataset versions and produces a delta report.

    The comparator is stateless: all configuration is passed at construction
    time (or at call time) so it can safely be reused across multiple
    comparisons.

    Args:
        config: Optional :class:`~dataintegrity.core.config.IntegrityConfig`.
                Reads ``drift_p_threshold`` when running the KS test.
    """

    def __init__(self, config: Optional[IntegrityConfig] = None) -> None:
        self.config: IntegrityConfig = config or DEFAULT_CONFIG

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compare_versions(
        self,
        current: DatasetVersion,
        previous: DatasetVersion,
    ) -> Dict[str, Any]:
        """
        Produce a structured delta report between two dataset versions.

        When both versions carry an in-memory ``_raw_df``, a full KS-based
        drift analysis is performed across all shared numeric columns.  If raw
        data is unavailable (deserialized versions loaded from disk), the
        report falls back to comparing dimension-level deltas only.

        Args:
            current:  The newer version (just audited).
            previous: The older baseline version (loaded from the store).

        Returns:
            A dict with the following keys:

            ``"score_delta"``
                Float — current DataScore minus previous DataScore.
            ``"previous_score"``
                Float — the baseline DataScore.
            ``"current_score"``
                Float — the current DataScore.
            ``"dimension_deltas"``
                Dict mapping each dimension name to its signed delta.
            ``"drifted_columns"``
                List of column names where KS drift was detected.
                Empty if raw data unavailable.
            ``"severity"``
                One of ``"stable"``, ``"minor"``, ``"moderate"``,
                ``"critical"``.
            ``"drift_available"``
                Bool — ``True`` if KS analysis was performed.
            ``"previous_version_id"``
                The version ID of the baseline.
        """
        prev_score = previous.data_score or 0.0
        curr_score = current.data_score or 0.0
        score_delta = round(curr_score - prev_score, 4)

        dimension_deltas = self._compute_dimension_deltas(current, previous)
        severity = _classify_severity(score_delta)

        drifted_columns: List[str] = []
        drift_available = False

        # Attempt KS drift analysis if raw DataFrames are available
        if current._raw_df is not None and previous._raw_df is not None:
            drift_available = True
            drifted_columns = self._run_ks_drift(current, previous)
        else:
            logger.debug(
                "Raw DataFrames not available for drift analysis — "
                "falling back to dimension deltas only."
            )

        return {
            "score_delta": score_delta,
            "previous_score": round(prev_score, 4),
            "current_score": round(curr_score, 4),
            "dimension_deltas": dimension_deltas,
            "drifted_columns": drifted_columns,
            "severity": severity,
            "drift_available": drift_available,
            "previous_version_id": previous.version_id,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _compute_dimension_deltas(
        self,
        current: DatasetVersion,
        previous: DatasetVersion,
    ) -> Dict[str, float]:
        """
        Compute signed score-delta for each shared quality dimension.

        Unshared dimensions are omitted.  Scores are in [0.0, 1.0].

        Returns:
            Dict mapping dimension name → (current_score − previous_score).
        """
        curr_dims = current.dimension_scores or {}
        prev_dims = previous.dimension_scores or {}
        shared = set(curr_dims.keys()) & set(prev_dims.keys())

        return {
            dim: round(curr_dims[dim] - prev_dims[dim], 6)
            for dim in sorted(shared)
        }

    def _run_ks_drift(
        self,
        current: DatasetVersion,
        previous: DatasetVersion,
    ) -> List[str]:
        """
        Run column-by-column KS drift analysis using in-memory DataFrames.

        Args:
            current:  Newer version with ``_raw_df`` populated.
            previous: Baseline version with ``_raw_df`` populated.

        Returns:
            Sorted list of column names where drift was detected.
        """
        try:
            from dataintegrity.drift.ks import compare_dataset_columns
        except ImportError as exc:
            logger.warning("Could not import KS drift module: %s", exc)
            return []

        try:
            results = compare_dataset_columns(
                old_df=previous._raw_df,
                new_df=current._raw_df,
                config=self.config,
            )
        except Exception as exc:  # pragma: no cover
            logger.warning("KS drift analysis failed: %s", exc)
            return []

        drifted = [
            col
            for col, info in results.items()
            if isinstance(info, dict) and info.get("drift_detected", False)
        ]
        return sorted(drifted)
