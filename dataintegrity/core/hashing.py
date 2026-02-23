"""
core/hashing.py
---------------
Deterministic SHA-256 fingerprinting utilities for Dataset objects.
"""

import hashlib
import pandas as pd


def compute_dataframe_fingerprint(df: pd.DataFrame) -> str:
    """
    Compute a deterministic SHA-256 fingerprint of a DataFrame.

    The fingerprint is derived from the sorted column list and the raw CSV
    bytes of the data, making it stable across row orderings when data is
    sorted first. Useful for deduplication and lineage tracking.

    Args:
        df: The pandas DataFrame to fingerprint.

    Returns:
        A 64-character hexadecimal SHA-256 digest string.
    """
    # Normalise by sorting columns and then rows for a stable hash
    sorted_df = df.reindex(sorted(df.columns), axis=1)
    try:
        sorted_df = sorted_df.sort_values(by=sorted_df.columns.tolist())
    except TypeError:
        # Fall back when columns contain mixed/unhashable types
        pass

    csv_bytes = sorted_df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()


def compute_string_fingerprint(value: str) -> str:
    """
    Compute a SHA-256 fingerprint of an arbitrary string value.

    Args:
        value: String to hash.

    Returns:
        A 64-character hexadecimal SHA-256 digest string.
    """
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
