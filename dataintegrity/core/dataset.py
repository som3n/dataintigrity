"""
core/dataset.py
---------------
Central Dataset abstraction — the canonical internal representation
of any data flowing through the dataintegrity SDK.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from dataintegrity.core.hashing import compute_dataframe_fingerprint


class Dataset:
    """
    Wraps a pandas DataFrame with schema metadata and a cryptographic fingerprint.

    This is the primary data contract object passed between pipeline stages.
    All connectors, ingestion helpers, and integrity checks operate on Dataset
    instances rather than raw DataFrames, ensuring a consistent interface.

    Attributes:
        df:          The underlying pandas DataFrame.
        metadata:    Arbitrary key/value metadata supplied by the caller.
        schema:      Inferred column -> dtype mapping.
        fingerprint: SHA-256 hex digest of the data snapshot.
        profile:     Populated by the integrity engine after analysis.
        source:      Optional string identifying the data origin (file path, table, …).
    """

    def __init__(
        self,
        dataframe: pd.DataFrame,
        metadata: Optional[Dict[str, Any]] = None,
        source: Optional[str] = None,
    ) -> None:
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError(
                f"Expected a pandas DataFrame, got {type(dataframe).__name__!r}."
            )

        self.df: pd.DataFrame = dataframe.copy()
        self.metadata: Dict[str, Any] = metadata or {}
        self.source: Optional[str] = source
        self.schema: Dict[str, str] = self._infer_schema()
        self.fingerprint: str = compute_dataframe_fingerprint(self.df)
        self.profile: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _infer_schema(self) -> Dict[str, str]:
        """
        Return a column -> pandas dtype name mapping inferred from the DataFrame.

        Returns:
            Dict mapping column name to its string dtype representation.
        """
        return {col: str(dtype) for col, dtype in self.df.dtypes.items()}

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def refresh_fingerprint(self) -> str:
        """
        Recompute and store the fingerprint after in-place mutations to ``df``.

        Returns:
            The new fingerprint string.
        """
        self.fingerprint = compute_dataframe_fingerprint(self.df)
        return self.fingerprint

    def refresh_schema(self) -> Dict[str, str]:
        """
        Recompute and store the schema after column-type mutations.

        Returns:
            The updated schema dict.
        """
        self.schema = self._infer_schema()
        return self.schema

    @property
    def shape(self) -> tuple:
        """Expose DataFrame shape for convenience."""
        return self.df.shape

    @property
    def columns(self):
        """Expose DataFrame columns for convenience."""
        return self.df.columns

    def __repr__(self) -> str:
        return (
            f"Dataset(rows={self.df.shape[0]}, cols={self.df.shape[1]}, "
            f"fingerprint={self.fingerprint[:12]}…, source={self.source!r})"
        )
