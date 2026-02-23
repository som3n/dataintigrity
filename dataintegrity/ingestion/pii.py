"""
ingestion/pii.py
----------------
Regex-based PII (Personally Identifiable Information) detection engine.

Scans every string/object column in a Dataset for known PII patterns and
returns a structured per-column report.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

import pandas as pd

from dataintegrity.core.config import IntegrityConfig, DEFAULT_CONFIG
from dataintegrity.core.dataset import Dataset


class PIIDetector:
    """
    Detects PII in DataFrame columns using configurable regex patterns.

    Args:
        config: An :class:`~dataintegrity.core.config.IntegrityConfig` instance.
                Uses :data:`~dataintegrity.core.config.DEFAULT_CONFIG` if omitted.

    The detector scans every column whose dtype is ``object`` (string-like)
    and optionally also numeric/datetime columns when their string representation
    might match a pattern (e.g. SSNs stored as integers).
    """

    def __init__(self, config: Optional[IntegrityConfig] = None) -> None:
        self.config = config or DEFAULT_CONFIG
        self._compiled: Dict[str, re.Pattern] = {
            name: re.compile(pattern)
            for name, pattern in self.config.pii_patterns.items()
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan(self, dataset: Dataset) -> Dict[str, Dict[str, Any]]:
        """
        Scan all columns in the dataset for PII patterns.

        Only ``object`` / ``string`` dtype columns are scanned (PII is
        almost always stored as text).  If a column contains mixed types,
        values are coerced to ``str`` before matching.

        Args:
            dataset: The dataset to inspect.

        Returns:
            A dict keyed by column name, each value being::

                {
                    "pii_detected": bool,
                    "count":        int,                  # rows with ≥1 PII hit
                    "patterns_hit": {pattern_name: hits}  # per-pattern match counts
                }
        """
        df = dataset.df
        report: Dict[str, Dict[str, Any]] = {}

        for col in df.columns:
            series = df[col]

            # Skip non-text dtypes (numeric, bool, datetime) — they cannot
            # contain email/phone PII in their native representation.
            if pd.api.types.is_numeric_dtype(series):
                continue
            if pd.api.types.is_bool_dtype(series):
                continue
            if pd.api.types.is_datetime64_any_dtype(series):
                continue
            if not (pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)):
                continue

            # Heuristic: skip columns whose non-null values are predominantly
            # ISO-style date strings (YYYY-MM-DD …) — avoids phone regex FPs.
            sample = series.dropna().astype(str).head(20)
            if len(sample) > 0:
                iso_date_hits = sample.str.match(r"^\d{4}-\d{2}-\d{2}").sum()
                if iso_date_hits / len(sample) >= 0.8:
                    continue

            col_report = self._scan_column(series)
            report[col] = col_report

        return report

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _scan_column(self, series: pd.Series) -> Dict[str, Any]:
        """Scan a single Series and return its PII report dict."""
        # Drop nulls, cast to str
        string_values = series.dropna().astype(str)

        patterns_hit: Dict[str, int] = {}
        rows_with_any_hit: set = set()

        for pattern_name, regex in self._compiled.items():
            matched_indices = set()
            for idx, val in zip(string_values.index, string_values):
                if regex.search(val):
                    matched_indices.add(idx)

            patterns_hit[pattern_name] = len(matched_indices)
            rows_with_any_hit |= matched_indices

        pii_count = len(rows_with_any_hit)
        return {
            "pii_detected": pii_count > 0,
            "count": pii_count,
            "patterns_hit": patterns_hit,
        }
