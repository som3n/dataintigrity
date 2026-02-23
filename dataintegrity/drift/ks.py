"""
drift/ks.py
-----------
Kolmogorov-Smirnov test based statistical drift detection.

Compares two numeric distributions and flags significant distributional
shift using SciPy's two-sample KS test.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from dataintegrity.core.config import IntegrityConfig, DEFAULT_CONFIG


def compare_distributions(
    old_series: pd.Series,
    new_series: pd.Series,
    config: Optional[IntegrityConfig] = None,
) -> Dict[str, Any]:
    """
    Compare two numeric distributions using the two-sample Kolmogorov-Smirnov test.

    Null hypothesis: the two samples come from the same distribution.
    When the p-value falls below the configured threshold the null is rejected
    and ``drift_detected`` is set to ``True``.

    Args:
        old_series: Reference (baseline) distribution as a pandas Series.
        new_series: Current distribution to compare against the baseline.
        config:     Configuration object (uses DEFAULT_CONFIG if omitted).
                    Reads ``drift_p_threshold`` to decide detection status.

    Returns:
        Dict with keys:

        * ``"ks_statistic"``  — float, KS test statistic D in [0, 1].
        * ``"p_value"``       — float, associated p-value.
        * ``"drift_detected"``— bool, True iff p_value < threshold.
        * ``"threshold"``     — the p-value threshold used.
        * ``"old_n"``         — sample size of the reference distribution.
        * ``"new_n"``         — sample size of the current distribution.

    Raises:
        ImportError: If ``scipy`` is not installed.
        ValueError:  If either series is empty after dropping nulls.
        TypeError:   If either series contains non-numeric values.
    """
    try:
        from scipy.stats import ks_2samp
    except ImportError as exc:
        raise ImportError(
            "scipy is required for drift detection. "
            "Install it with: pip install scipy"
        ) from exc

    cfg = config or DEFAULT_CONFIG

    # Drop nulls and coerce to float
    old_clean = pd.to_numeric(old_series.dropna(), errors="raise").values.astype(float)
    new_clean = pd.to_numeric(new_series.dropna(), errors="raise").values.astype(float)

    if len(old_clean) == 0:
        raise ValueError("old_series contains no valid numeric values after dropping nulls.")
    if len(new_clean) == 0:
        raise ValueError("new_series contains no valid numeric values after dropping nulls.")

    ks_stat, p_value = ks_2samp(old_clean, new_clean)

    return {
        "ks_statistic": round(float(ks_stat), 6),
        "p_value": round(float(p_value), 6),
        "drift_detected": bool(p_value < cfg.drift_p_threshold),
        "threshold": cfg.drift_p_threshold,
        "old_n": len(old_clean),
        "new_n": len(new_clean),
    }


def compare_dataset_columns(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    columns: Optional[list] = None,
    config: Optional[IntegrityConfig] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Run column-by-column drift comparison across two DataFrames.

    Only numeric columns that exist in both DataFrames are tested.

    Args:
        old_df:   Reference DataFrame.
        new_df:   Current DataFrame.
        columns:  Optional list of column names to test. If None, all shared
                  numeric columns are used.
        config:   Configuration object.

    Returns:
        Dict mapping column name → KS test result dict (see :func:`compare_distributions`).
    """
    cfg = config or DEFAULT_CONFIG
    shared_cols = set(old_df.columns) & set(new_df.columns)

    if columns is not None:
        shared_cols &= set(columns)

    numeric_cols = [
        c for c in shared_cols
        if pd.api.types.is_numeric_dtype(old_df[c])
        and pd.api.types.is_numeric_dtype(new_df[c])
    ]

    results: Dict[str, Dict[str, Any]] = {}
    for col in sorted(numeric_cols):
        try:
            results[col] = compare_distributions(old_df[col], new_df[col], config=cfg)
        except (ValueError, TypeError) as exc:
            results[col] = {"error": str(exc)}

    return results
