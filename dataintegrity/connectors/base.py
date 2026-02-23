"""
connectors/base.py
------------------
Abstract base class that every data source connector must implement.
"""

from abc import ABC, abstractmethod

import pandas as pd


class BaseConnector(ABC):
    """
    Abstract interface for all dataintegrity data source connectors.

    Subclasses must implement :meth:`connect` and :meth:`fetch`.  The
    recommended usage pattern is::

        connector = SomeConnector(...)
        connector.connect()
        df = connector.fetch()
    """

    @abstractmethod
    def connect(self) -> None:
        """
        Establish / validate the connection to the data source.

        Should raise a meaningful exception if the connection cannot be made
        (e.g. ``FileNotFoundError``, ``sqlalchemy.exc.OperationalError``).
        """

    @abstractmethod
    def fetch(self) -> pd.DataFrame:
        """
        Retrieve data from the connected source and return a DataFrame.

        Returns:
            A pandas DataFrame containing the retrieved data.

        Raises:
            RuntimeError: If :meth:`connect` has not been called first.
        """

    def connect_and_fetch(self) -> pd.DataFrame:
        """
        Convenience method: connect then immediately fetch.

        Returns:
            A pandas DataFrame containing the retrieved data.
        """
        self.connect()
        return self.fetch()
