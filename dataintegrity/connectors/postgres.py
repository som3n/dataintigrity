"""
connectors/postgres.py
----------------------
PostgreSQL connector using SQLAlchemy — executes a query and
returns the result as a pandas DataFrame.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from dataintegrity.connectors.base import BaseConnector


class PostgresConnector(BaseConnector):
    """
    Connects to a PostgreSQL database via SQLAlchemy and returns query results.

    Args:
        host:     Database host (e.g. ``'localhost'``).
        port:     Database port (default: ``5432``).
        database: Database name.
        user:     Username.
        password: Password.
        query:    SQL query string to execute.
        schema:   Optional schema name (used in the connection URL).

    Example::

        connector = PostgresConnector(
            host="localhost", port=5432,
            database="mydb", user="admin", password="secret",
            query="SELECT * FROM customers LIMIT 1000"
        )
        connector.connect()
        df = connector.fetch()
    """

    def __init__(
        self,
        host: str,
        port: int = 5432,
        database: str = "",
        user: str = "",
        password: str = "",
        query: str = "",
        schema: Optional[str] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.query = query
        self.schema = schema
        self._engine = None
        self._connected: bool = False

    # ------------------------------------------------------------------
    # BaseConnector interface
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """
        Build the SQLAlchemy engine and verify connectivity.

        Raises:
            ImportError:  If ``sqlalchemy`` is not installed.
            RuntimeError: If the connection test fails.
        """
        try:
            from sqlalchemy import create_engine, text  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "sqlalchemy is required for PostgresConnector. "
                "Install it with: pip install sqlalchemy psycopg2-binary"
            ) from exc

        from sqlalchemy import create_engine, text

        url = (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

        try:
            self._engine = create_engine(url, future=True)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as exc:
            raise RuntimeError(
                f"Unable to connect to PostgreSQL at "
                f"{self.host}:{self.port}/{self.database} — {exc}"
            ) from exc

        self._connected = True

    def fetch(self) -> pd.DataFrame:
        """
        Execute the configured query and return results as a DataFrame.

        Returns:
            pandas DataFrame with query results.

        Raises:
            RuntimeError: If :meth:`connect` was not called first, or the query is empty.
        """
        if not self._connected or self._engine is None:
            raise RuntimeError("Call connect() before fetch().")
        if not self.query.strip():
            raise ValueError("A non-empty SQL query must be provided.")

        df = pd.read_sql(self.query, con=self._engine)
        return df

    def close(self) -> None:
        """Dispose of the underlying SQLAlchemy engine and its connection pool."""
        if self._engine is not None:
            self._engine.dispose()
            self._connected = False

    def __repr__(self) -> str:
        return (
            f"PostgresConnector(host={self.host!r}, port={self.port}, "
            f"database={self.database!r}, connected={self._connected})"
        )
