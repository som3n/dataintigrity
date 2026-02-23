"""
ingestion/normalizer.py
-----------------------
Column-name and datatype normalisation for raw DataFrames.
"""

from __future__ import annotations

import re
from typing import List, Optional

import pandas as pd

from dataintegrity.core.dataset import Dataset


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize DataFrame column names:

    * Strip leading/trailing whitespace.
    * Convert to lowercase.
    * Replace spaces and special characters with underscores.
    * Collapse consecutive underscores.

    Args:
        df: Input DataFrame.

    Returns:
        New DataFrame with normalized column names (data unchanged).
    """
    rename_map = {}
    for col in df.columns:
        new_col = str(col).strip()
        new_col = new_col.lower()
        new_col = re.sub(r"[\s\-/\\\.]+", "_", new_col)
        new_col = re.sub(r"[^a-z0-9_]", "", new_col)
        new_col = re.sub(r"_+", "_", new_col).strip("_")
        rename_map[col] = new_col or f"col_{list(df.columns).index(col)}"

    return df.rename(columns=rename_map)


def normalize_datatypes(
    df: pd.DataFrame,
    timestamp_columns: Optional[List[str]] = None,
    coerce_numeric: bool = True,
) -> pd.DataFrame:
    """
    Attempt lightweight automatic datatype normalization:

    * Optionally coerce object columns to numeric where possible.
    * Parse explicitly listed columns as :class:`pandas.Timestamp`.

    Args:
        df:                The DataFrame to normalize.
        timestamp_columns: Column names to parse as datetime.
        coerce_numeric:    If True, attempt numeric coercion on object columns.

    Returns:
        A new DataFrame with improved dtypes.
    """
    df = df.copy()
    timestamp_columns = timestamp_columns or []

    if coerce_numeric:
        for col in df.select_dtypes(include="object").columns:
            if col in timestamp_columns:
                continue
            converted = pd.to_numeric(df[col], errors="coerce")
            # Only apply if the coercion is mostly successful (>50% valid)
            non_null_original = df[col].notna().sum()
            converted_valid = converted.notna().sum()
            if non_null_original > 0 and (converted_valid / non_null_original) >= 0.5:
                df[col] = converted

    for col in timestamp_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    return df


class Normalizer:
    """
    Stateful normalizer that applies column-name and dtype normalisation
    in sequence, returning a refreshed :class:`~dataintegrity.core.dataset.Dataset`.

    Args:
        timestamp_columns: Columns to parse as datetime.
        coerce_numeric:    Whether to attempt numeric coercion.
    """

    def __init__(
        self,
        timestamp_columns: Optional[List[str]] = None,
        coerce_numeric: bool = True,
    ) -> None:
        self.timestamp_columns = timestamp_columns or []
        self.coerce_numeric = coerce_numeric

    def normalize(self, dataset: Dataset) -> Dataset:
        """
        Apply all normalisation steps to the dataset in-place.

        Column names are normalised first; timestamp column references are
        updated to reflect the new names before dtype normalisation runs.

        Args:
            dataset: The :class:`~dataintegrity.core.dataset.Dataset` to normalise.

        Returns:
            The same dataset object with ``df``, ``schema``, and ``fingerprint`` refreshed.
        """
        df = normalize_column_names(dataset.df)

        # Remap caller-provided timestamp column names to the normalised versions
        ts_cols_normalised = []
        for col in self.timestamp_columns:
            normalised = normalize_column_names(
                pd.DataFrame(columns=[col])
            ).columns[0]
            ts_cols_normalised.append(normalised)

        df = normalize_datatypes(
            df,
            timestamp_columns=ts_cols_normalised,
            coerce_numeric=self.coerce_numeric,
        )

        dataset.df = df
        dataset.refresh_schema()
        dataset.refresh_fingerprint()
        dataset.metadata.setdefault("normalized", True)
        return dataset
