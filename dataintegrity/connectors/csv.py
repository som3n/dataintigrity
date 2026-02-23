"""
connectors/csv.py
-----------------
CSV file connector — reads local CSV files into pandas DataFrames.

v0.2 additions
--------------
* ``sample_size`` — cap the number of rows returned (applied after read).
* ``chunk_size``  — used when the file is very large to limit memory usage.
* Files larger than 50 MB are automatically sampled when neither parameter is
  provided, with a warning logged to the caller.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import pandas as pd

from dataintegrity.connectors.base import BaseConnector

logger = logging.getLogger(__name__)

# Trigger auto-sampling above this threshold (bytes)
_LARGE_FILE_THRESHOLD = 50 * 1024 * 1024  # 50 MB
_AUTO_SAMPLE_ROWS = 100_000               # rows read when auto-sampling


class CSVConnector(BaseConnector):
    """
    Loads a CSV file from the local filesystem into a pandas DataFrame.

    Args:
        filepath:    Absolute or relative path to the CSV file.
        encoding:    File encoding (default: ``'utf-8-sig'`` handles BOM).
        delimiter:   Column delimiter (default: ``','``).
        sample_size: Optional maximum number of rows to return.  Applied
                     *after* reading, so it works alongside ``chunk_size``.
        chunk_size:  Optional number of rows per chunk used for reading large
                     files.  When set, the file is read in chunks and only the
                     first ``sample_size`` (or ``chunk_size``) rows are kept.
        kwargs:      Additional keyword arguments forwarded to
                     :func:`pandas.read_csv`.

    Large-file safety
    -----------------
    When the file on disk exceeds 50 MB **and** neither ``sample_size`` nor
    ``chunk_size`` is specified, the connector automatically reads only the
    first :data:`_AUTO_SAMPLE_ROWS` rows and emits a ``WARNING`` log message.
    This prevents out-of-memory crashes on large datasets without forcing
    callers to reconfigure anything.

    Example::

        connector = CSVConnector("/data/customers.csv", sample_size=50_000)
        connector.connect()
        df = connector.fetch()
    """

    def __init__(
        self,
        filepath: str,
        encoding: str = "utf-8-sig",
        delimiter: str = ",",
        sample_size: Optional[int] = None,
        chunk_size: Optional[int] = None,
        **kwargs,
    ) -> None:
        self.filepath = filepath
        self.encoding = encoding
        self.delimiter = delimiter
        self.sample_size = sample_size
        self.chunk_size = chunk_size
        self._kwargs = kwargs
        self._connected: bool = False
        self._df: Optional[pd.DataFrame] = None

    # ------------------------------------------------------------------
    # BaseConnector interface
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """
        Validate that the CSV file exists and is readable.

        Raises:
            FileNotFoundError: If the file does not exist at the given path.
            PermissionError:   If the process lacks read permission.
            ValueError:        If the path points to a directory.
        """
        if not os.path.exists(self.filepath):
            raise FileNotFoundError(
                f"CSV file not found: {self.filepath!r}"
            )
        if os.path.isdir(self.filepath):
            raise ValueError(
                f"Expected a file path, got a directory: {self.filepath!r}"
            )
        if not os.access(self.filepath, os.R_OK):
            raise PermissionError(
                f"No read permission for file: {self.filepath!r}"
            )
        self._connected = True

    def fetch(self) -> pd.DataFrame:
        """
        Read the CSV file and return a DataFrame.

        Large-file behaviour
        --------------------
        1. If the file exceeds 50 MB and no row limits were set, the connector
           reads only the first :data:`_AUTO_SAMPLE_ROWS` rows and logs a
           ``WARNING``.
        2. If ``chunk_size`` is set, the file is streamed in chunks and at most
           ``sample_size`` (or ``chunk_size``) rows are concatenated.
        3. If only ``sample_size`` is set, ``nrows`` is passed directly to
           :func:`pandas.read_csv`.

        Returns:
            pandas DataFrame parsed from the CSV file.

        Raises:
            RuntimeError: If :meth:`connect` was not called first.
            pandas.errors.ParserError: If the file cannot be parsed as CSV.
        """
        if not self._connected:
            raise RuntimeError("Call connect() before fetch().")

        file_size = os.path.getsize(self.filepath)
        is_large = file_size > _LARGE_FILE_THRESHOLD

        # Determine effective row limits
        effective_sample = self.sample_size
        effective_chunk = self.chunk_size

        if is_large and effective_sample is None and effective_chunk is None:
            logger.warning(
                "File %r is %.1f MB which exceeds the 50 MB safety threshold. "
                "Automatically sampling the first %d rows. "
                "Pass sample_size= or chunk_size= to control this behaviour.",
                self.filepath,
                file_size / (1024 * 1024),
                _AUTO_SAMPLE_ROWS,
            )
            effective_sample = _AUTO_SAMPLE_ROWS

        try:
            if effective_chunk is not None:
                self._df = self._read_chunked(effective_chunk, effective_sample)
            elif effective_sample is not None:
                self._df = self._read_nrows(effective_sample)
            else:
                self._df = self._read_full()
        except UnicodeDecodeError:
            logger.warning(
                "UTF-8 decode failed for %r — retrying with latin-1.",
                self.filepath,
            )
            self._df = self._read_full(encoding_override="latin-1")

        return self._df.copy()

    # ------------------------------------------------------------------
    # Internal read helpers
    # ------------------------------------------------------------------

    def _read_full(self, encoding_override: Optional[str] = None) -> pd.DataFrame:
        """Read the entire file without row limits."""
        return pd.read_csv(
            self.filepath,
            encoding=encoding_override or self.encoding,
            sep=self.delimiter,
            **self._kwargs,
        )

    def _read_nrows(self, nrows: int) -> pd.DataFrame:
        """Read at most *nrows* rows from the file."""
        try:
            return pd.read_csv(
                self.filepath,
                encoding=self.encoding,
                sep=self.delimiter,
                nrows=nrows,
                **self._kwargs,
            )
        except UnicodeDecodeError:
            return pd.read_csv(
                self.filepath,
                encoding="latin-1",
                sep=self.delimiter,
                nrows=nrows,
                **self._kwargs,
            )

    def _read_chunked(
        self,
        chunk_size: int,
        max_rows: Optional[int],
    ) -> pd.DataFrame:
        """
        Stream the file in chunks and return up to *max_rows* rows.

        Args:
            chunk_size: Rows to read per iteration.
            max_rows:   Total row cap.  ``None`` means read all chunks.

        Returns:
            Concatenated pandas DataFrame.
        """
        collected_chunks = []
        rows_read = 0

        try:
            reader = pd.read_csv(
                self.filepath,
                encoding=self.encoding,
                sep=self.delimiter,
                chunksize=chunk_size,
                **self._kwargs,
            )
            for chunk in reader:
                if max_rows is not None:
                    remaining = max_rows - rows_read
                    if remaining <= 0:
                        break
                    chunk = chunk.iloc[:remaining]

                collected_chunks.append(chunk)
                rows_read += len(chunk)

                if max_rows is not None and rows_read >= max_rows:
                    break

        except UnicodeDecodeError:
            collected_chunks = []
            reader = pd.read_csv(
                self.filepath,
                encoding="latin-1",
                sep=self.delimiter,
                chunksize=chunk_size,
                **self._kwargs,
            )
            rows_read = 0
            for chunk in reader:
                if max_rows is not None:
                    remaining = max_rows - rows_read
                    if remaining <= 0:
                        break
                    chunk = chunk.iloc[:remaining]
                collected_chunks.append(chunk)
                rows_read += len(chunk)
                if max_rows is not None and rows_read >= max_rows:
                    break

        if not collected_chunks:
            return pd.DataFrame()
        return pd.concat(collected_chunks, ignore_index=True)

    def __repr__(self) -> str:
        return f"CSVConnector(filepath={self.filepath!r}, connected={self._connected})"
