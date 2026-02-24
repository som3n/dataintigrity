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


def compute_structured_fingerprint(df: pd.DataFrame) -> dict:
    """
    Compute a structured fingerprint of a DataFrame.

    Components:
    - Structural: SHA-256 of sorted column names and dtypes.
    - Statistical: SHA-256 of mean, std, 25%, 50%, 75% for numeric columns.
    - Row Count: Integer row count.
    - Combined: SHA-256 of the above three.

    Args:
        df: The pandas DataFrame to fingerprint.

    Returns:
        Dict with keys: structural, statistical, row_count, combined.
    """
    import json

    # 1. Structural Fingerprint
    cols = sorted(df.columns.tolist())
    struct_data = {col: str(df[col].dtype) for col in cols}
    struct_payload = json.dumps(struct_data, sort_keys=True).encode("utf-8")
    struct_hash = hashlib.sha256(struct_payload).hexdigest()

    # 2. Statistical Fingerprint
    numeric_df = df.select_dtypes(include=["number"])
    stats_data = {}
    if not numeric_df.empty:
        stats = numeric_df.describe().loc[["mean", "std", "25%", "50%", "75%"]]
        # Round to avoid float precision issues across platforms if any
        stats_data = stats.round(6).to_dict()
    
    stats_payload = json.dumps(stats_data, sort_keys=True).encode("utf-8")
    stats_hash = hashlib.sha256(stats_payload).hexdigest()

    # 3. Row Count
    row_count = int(len(df))

    # 4. Combined Fingerprint
    combined_payload = f"{struct_hash}|{stats_hash}|{row_count}".encode("utf-8")
    combined_hash = hashlib.sha256(combined_payload).hexdigest()

    return {
        "structural": struct_hash,
        "statistical": stats_hash,
        "row_count": row_count,
        "combined": combined_hash,
    }


def compute_string_fingerprint(value: str) -> str:
    """
    Compute a SHA-256 fingerprint of an arbitrary string value.

    Args:
        value: String to hash.

    Returns:
        A 64-character hexadecimal SHA-256 digest string.
    """
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
