# Copyright 2025 Entalpic
from abc import ABC, abstractmethod
from dataclasses import dataclass
from threading import local
from typing import Any, List, Optional

from lematerial_fetcher.database.postgres import DatasetVersions, StructuresDatabase
from lematerial_fetcher.utils.config import FetcherConfig
from lematerial_fetcher.utils.logging import logger


@dataclass
class ItemsInfo:
    """Information about items to be processed.
    For APIs, this will hold the beginning offset, and eventually the total count if supported.
    For download sources, this will hold the list of items to download.
    """

    start_offset: int
    total_count: Optional[int] = None
    items: Optional[List[Any]] = (
        None  # can hold S3 keys for MP or be None for Alexandria
    )


class BaseFetcher(ABC):
    """
    Abstract base class for all material data fetchers.

    This class defines the common interface and shared functionality that all fetchers
    must implement. It handles common operations like database setup and error handling
    while allowing specific implementations to define their own data retrieval logic.
    """

    def __init__(self, config: FetcherConfig):
        """
        Initialize the fetcher with configuration.

        Parameters
        ----------
        config : FetcherConfig
            Configuration object containing necessary parameters for the fetcher
        """
        self.config = config
        self._thread_local = local()

    @property
    def db(self) -> StructuresDatabase:
        """
        Get the database connection for the current thread.
        Creates a new connection if one doesn't exist.

        Returns
        -------
        StructuresDatabase
            Database connection for the current thread
        """
        if not hasattr(self._thread_local, "db"):
            self._thread_local.db = self._create_db_connection()
        return self._thread_local.db

    @property
    def version_db(self) -> DatasetVersions:
        """
        Get the version tracking database connection for the current thread.
        Creates a new connection if one doesn't exist.

        Returns
        -------
        DatasetVersions
            Version tracking database connection for the current thread
        """
        if not hasattr(self._thread_local, "version_db"):
            self._thread_local.version_db = DatasetVersions(self.config.db_conn_str)
        return self._thread_local.version_db

    def _create_db_connection(
        self, table_name: Optional[str] = None
    ) -> StructuresDatabase:
        """
        Create a new database connection.

        Parameters
        ----------
        table_name : Optional[str]
            Name of the table to use. If None, uses the one from config.

        Returns
        -------
        StructuresDatabase
            New database connection
        """
        table = table_name or self.config.table_name
        db = StructuresDatabase(self.config.db_conn_str, table)
        db.create_table()
        return db

    def setup_database(self, table_name: Optional[str] = None) -> None:
        """
        Set up the main database connection and create necessary tables.

        Parameters
        ----------
        table_name : Optional[str]
            Name of the table to create. If None, uses the one from config.
        """
        db = self._create_db_connection(table_name)
        db.create_table()

        self.version_db.create_table()

    def get_current_version(self) -> Optional[str]:
        """
        Get the current version of the dataset.

        Returns
        -------
        Optional[str]
            The current version identifier, or None if not set
        """
        return self.version_db.get_last_synced_version(self.config.table_name)

    def update_version(self, version: str) -> None:
        """
        Update the version information for the current dataset.

        Parameters
        ----------
        version : str
            New version identifier
        """
        self.version_db.update_version(self.config.table_name, version)

    def fetch(self) -> None:
        """
        Main entry point for fetching data. Implements the template method pattern
        for the fetching process.

        This method orchestrates the fetching process by calling the abstract methods
        that subclasses must implement.

        Raises
        ------
        Exception
            If any error occurs during the fetching process.
        """
        try:
            logger.info(f"Starting fetch process for {self.__class__.__name__}")

            # Get current version
            current_version = self.get_current_version()
            logger.info(f"Current dataset version: {current_version or 'Not set'}")

            # Initialize resources
            self.setup_resources()

            # Get the data source items to process
            items_info = self.get_items_to_process()
            logger.info(
                f"Found {items_info.total_count} items to process starting from offset {items_info.start_offset}"
            )

            # Process the items
            self.process_items(items_info)

            # Update version after successful processing
            new_version = self.get_new_version()
            if new_version != current_version:
                self.update_version(new_version)
                logger.info(f"Updated dataset version to: {new_version}")

            # Cleanup
            self.cleanup_resources()

            logger.info("Successfully completed fetch process")

        except Exception as e:
            logger.fatal(f"Error during fetch: {str(e)}")
            raise

    @staticmethod
    def is_critical_error(error: Exception) -> bool:
        """
        Determine if an error should be considered critical and stop processing.

        Parameters
        ----------
        error : Exception
            The error to evaluate

        Returns
        -------
        bool
            True if the error is critical, False otherwise
        """
        if error is None:
            return False

        error_str = str(error).lower()
        critical_conditions = [
            "connection refused",
            "no such host",
            "connection reset",
            "database error",
        ]
        return any(condition in error_str for condition in critical_conditions)

    @abstractmethod
    def setup_resources(self) -> None:
        """
        Set up any necessary resources (e.g., API clients, database connections).
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def get_items_to_process(self) -> ItemsInfo:
        """
        Get information about items to process.

        Returns
        -------
        ItemsInfo
            Information about where to start processing and optionally total count
        """
        pass

    @abstractmethod
    def process_items(self, items_info: ItemsInfo) -> None:
        """
        Process items in parallel, starting from the given offset.

        Parameters
        ----------
        items_info : ItemsInfo
            Information about where to start processing
        """
        pass

    @abstractmethod
    def get_new_version(self) -> str:
        """
        Get the new version identifier for the current dataset.
        Must be implemented by subclasses.

        Returns
        -------
        str
            New version identifier
        """
        pass

    def cleanup_resources(self) -> None:
        """
        Clean up any resources that were created during the fetch process.
        Must be implemented by subclasses.
        """
        if hasattr(self._thread_local, "db"):
            delattr(self._thread_local, "db")
        if hasattr(self._thread_local, "version_db"):
            delattr(self._thread_local, "version_db")
