"""
integrity/rules.py
------------------
Modular data quality rule implementations.

Each rule accepts a :class:`~dataintegrity.core.dataset.Dataset` and an
optional configuration object, and returns a float score in [0.0, 1.0].

Rules
-----
* completeness  — fraction of non-null values across the dataset.
* uniqueness    — fraction of unique rows (after normalisation).
* validity      — fraction of columns whose dtype is self-consistent.
* consistency   — absence of cross-column null mismatches.
* timeliness    — fraction of datetime values newer than the configured max age.
"""

from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from dataintegrity.core.config import IntegrityConfig, DEFAULT_CONFIG
from dataintegrity.core.dataset import Dataset


# ---------------------------------------------------------------------------
# Individual rule functions
# ---------------------------------------------------------------------------


def check_completeness(dataset: Dataset, **_kwargs) -> float:
    """
    Compute the overall completeness score.

    Score = (total non-null values) / (total cells).
    A perfectly complete dataset scores 1.0.

    Args:
        dataset: Input dataset.

    Returns:
        Completeness score in [0.0, 1.0].
    """
    df = dataset.df
    total_cells = df.size
    if total_cells == 0:
        return 1.0
    non_null = df.notna().sum().sum()
    return float(non_null / total_cells)


def check_uniqueness(dataset: Dataset, **_kwargs) -> float:
    """
    Compute the uniqueness score based on duplicate rows.

    Score = (unique rows) / (total rows).
    Only one perfectly unique row per dataset scores 1.0.

    Args:
        dataset: Input dataset.

    Returns:
        Uniqueness score in [0.0, 1.0].
    """
    df = dataset.df
    n_rows = len(df)
    if n_rows == 0:
        return 1.0
    n_unique = len(df.drop_duplicates())
    return float(n_unique / n_rows)


def check_validity(dataset: Dataset, **_kwargs) -> float:
    """
    Assess dtype consistency (validity) per column.

    For every column we check whether the actual dtype is a well-defined,
    non-generic type (object dtype is used as a proxy for inconsistency when
    the column could reasonably be numeric/datetime but wasn't parsed as such).

    Score = (columns with non-object dtype) / (total columns).

    Args:
        dataset: Input dataset.

    Returns:
        Validity score in [0.0, 1.0].
    """
    df = dataset.df
    n_cols = len(df.columns)
    if n_cols == 0:
        return 1.0

    typed_cols = sum(
        1
        for col in df.columns
        if not pd.api.types.is_object_dtype(df[col])
    )
    return float(typed_cols / n_cols)


def check_consistency(
    dataset: Dataset,
    column_groups: Optional[List[List[str]]] = None,
    **_kwargs,
) -> float:
    """
    Check for cross-column null consistency.

    A row is *inconsistent* if, within a related column group, some values are
    null while others in the same row are present.  If ``column_groups`` is not
    supplied, all columns are treated as a single group.

    Score = (consistent rows) / (total rows).

    Args:
        dataset:       Input dataset.
        column_groups: List of column-name lists to check together.

    Returns:
        Consistency score in [0.0, 1.0].
    """
    df = dataset.df
    n_rows = len(df)
    if n_rows == 0:
        return 1.0

    if column_groups is None:
        # Default: all columns are one group
        column_groups = [list(df.columns)]

    inconsistent_rows = set()

    for group in column_groups:
        valid_group = [c for c in group if c in df.columns]
        if len(valid_group) < 2:
            continue
        sub = df[valid_group].isna()
        # A row is inconsistent if not all-null and not all-present
        mask = sub.any(axis=1) & ~sub.all(axis=1)
        inconsistent_rows.update(df.index[mask].tolist())

    n_inconsistent = len(inconsistent_rows)
    return float((n_rows - n_inconsistent) / n_rows)


def check_timeliness(
    dataset: Dataset,
    config: Optional[IntegrityConfig] = None,
    timestamp_columns: Optional[List[str]] = None,
    **_kwargs,
) -> float:
    """
    Measure how recent the datetime values are.

    For every datetime column found, we count the fraction of non-null values
    that fall within the configured ``timeliness_max_age_days`` window.

    If no datetime columns exist, returns 1.0 (timeliness is not applicable).

    Args:
        dataset:           Input dataset.
        config:            Config object (uses DEFAULT_CONFIG if omitted).
        timestamp_columns: Override list of column names to inspect.

    Returns:
        Timeliness score in [0.0, 1.0].
    """
    cfg = config or DEFAULT_CONFIG
    df = dataset.df
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(
        days=cfg.timeliness_max_age_days
    )

    # Determine which columns to inspect
    if timestamp_columns:
        dt_cols = [c for c in timestamp_columns if c in df.columns]
    else:
        dt_cols = [
            c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])
        ]

    if not dt_cols:
        return 1.0  # No datetime columns → timeliness N/A, treat as perfect

    totals: List[float] = []
    for col in dt_cols:
        series = df[col].dropna()
        if series.empty:
            totals.append(0.0)
            continue
        # Localise tz-naive series to UTC for comparison
        if series.dt.tz is None:
            series = series.dt.tz_localize("UTC")
        recent = (series >= cutoff).sum()
        totals.append(float(recent / len(series)))

    return float(sum(totals) / len(totals))


# ---------------------------------------------------------------------------
# Rule registry — dynamic lookups for backward compatibility
# ---------------------------------------------------------------------------

from dataintegrity.integrity.plugins import (
    IntegrityRule,
    register_rule,
    get_registered_rules,
)
from dataintegrity.core.result_schema import RuleResult


@register_rule
class CompletenessRule(IntegrityRule):
    id = "completeness"
    description = "Fraction of non-null values across all cells in the dataset."
    severity = "HIGH"

    def evaluate(self, dataset: Dataset, config: IntegrityConfig) -> RuleResult:
        score = check_completeness(dataset)
        return RuleResult(
            rule_id=self.id,
            description=self.description,
            metric_value=score,
            threshold=0.5,
            passed=score >= 0.5,
            severity=self.severity,
        )


@register_rule
class UniquenessRule(IntegrityRule):
    id = "uniqueness"
    description = "Fraction of unique rows (absence of exact duplicates)."
    severity = "MEDIUM"

    def evaluate(self, dataset: Dataset, config: IntegrityConfig) -> RuleResult:
        score = check_uniqueness(dataset)
        return RuleResult(
            rule_id=self.id,
            description=self.description,
            metric_value=score,
            threshold=0.5,
            passed=score >= 0.5,
            severity=self.severity,
        )


@register_rule
class ValidityRule(IntegrityRule):
    id = "validity"
    description = "Fraction of columns with a well-defined, non-generic dtype."
    severity = "MEDIUM"

    def evaluate(self, dataset: Dataset, config: IntegrityConfig) -> RuleResult:
        score = check_validity(dataset)
        return RuleResult(
            rule_id=self.id,
            description=self.description,
            metric_value=score,
            threshold=0.5,
            passed=score >= 0.5,
            severity=self.severity,
        )


@register_rule
class ConsistencyRule(IntegrityRule):
    id = "consistency"
    description = "Absence of cross-column null mismatches within related column groups."
    severity = "HIGH"

    def evaluate(self, dataset: Dataset, config: IntegrityConfig) -> RuleResult:
        # Note: In a real system we'd pull column_groups from config or engine context.
        # Here we maintain the existing functional interface for the scorer/engine.
        score = check_consistency(dataset)
        return RuleResult(
            rule_id=self.id,
            description=self.description,
            metric_value=score,
            threshold=0.5,
            passed=score >= 0.5,
            severity=self.severity,
        )


@register_rule
class TimelinessRule(IntegrityRule):
    id = "timeliness"
    description = "Fraction of datetime values falling within the configured maximum age window."
    severity = "LOW"

    def evaluate(self, dataset: Dataset, config: IntegrityConfig) -> RuleResult:
        score = check_timeliness(dataset, config=config)
        return RuleResult(
            rule_id=self.id,
            description=self.description,
            metric_value=score,
            threshold=0.5,
            passed=score >= 0.5,
            severity=self.severity,
        )


# Backward compatibility shims — derived from the registry
# These mappings allow IntegrityEngine (v0.2.0 style) to keep working.

def _get_registry_map():
    return {rid: rcls().evaluate for rid, rcls in get_registered_rules().items()}

RULE_REGISTRY: Dict[str, Any] = {
    "completeness": check_completeness,
    "uniqueness": check_uniqueness,
    "validity": check_validity,
    "consistency": check_consistency,
    "timeliness": check_timeliness,
}

RULE_SEVERITY: Dict[str, str] = {
    rid: rcls.severity for rid, rcls in get_registered_rules().items()
}

RULE_DESCRIPTIONS: Dict[str, str] = {
    rid: rcls.description for rid, rcls in get_registered_rules().items()
}

