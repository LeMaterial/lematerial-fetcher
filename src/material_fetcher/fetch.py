# Copyright 2025 Entalpic
from abc import ABC, abstractmethod
from typing import Any, List, Optional

from material_fetcher.database.postgres import StructuresDatabase
from material_fetcher.model.models import RawStructure
from material_fetcher.utils.config import FetcherConfig
from material_fetcher.utils.logging import logger


class BaseFetcher(ABC):
    """
    Abstract base class for all material data fetchers.

    This class defines the common interface and shared functionality that all fetchers
    must implement. It handles common operations like database setup and error handling
    while allowing specific implementations to define their own data retrieval logic.

    Parameters
    ----------
    config : FetcherConfig
        Configuration object containing necessary parameters for the fetcher
    """

    def __init__(self, config: FetcherConfig):
        self.config = config
        self.db = None

    def setup_database(self, table_name: Optional[str] = None) -> None:
        """
        Set up the database connection and create necessary tables.

        Parameters
        ----------
        table_name : Optional[str]
            Name of the table to create. If None, uses the one from config.
        """
        table = table_name or self.config.table_name
        self.db = StructuresDatabase(self.config.db_conn_str, table)
        self.db.create_table()

    def fetch(self) -> None:
        """
        Main entry point for fetching data. Implements the template method pattern
        for the fetching process.

        This method orchestrates the fetching process by calling the abstract methods
        that subclasses must implement.

        It starts by setting up the resources, then gets the items to process (S3 keys, API offset, etc.),
        then processes the items in parallel (using a thread pool), and finally cleans up the resources.

        Raises
        ------
        Exception
            If any error occurs during the fetching process.
        """
        try:
            logger.info(f"Starting fetch process for {self.__class__.__name__}")

            # Initialize resources
            self.setup_resources()

            # Get the data source items to process
            items = self.get_items_to_process()
            logger.info(f"Found {len(items)} items to process")

            # Process the items
            self.process_items(items)

            # Cleanup
            self.cleanup_resources()

            logger.info("Successfully completed fetch process")

        except Exception as e:
            logger.fatal(f"Error during fetch: {str(e)}")
            raise

    @abstractmethod
    def setup_resources(self) -> None:
        """
        Set up any necessary resources (e.g., API clients, database connections).
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def get_items_to_process(self) -> List[Any]:
        """
        Get the list of items that need to be processed.
        Must be implemented by subclasses.

        Returns
        -------
        List[Any]
            List of items to be processed
        """
        pass

    @abstractmethod
    def process_items(self, items: List[Any]) -> None:
        """
        Process the items and store them in the database.
        Must be implemented by subclasses.

        Parameters
        ----------
        items : List[Any]
            List of items to process
        """
        pass

    @abstractmethod
    def cleanup_resources(self) -> None:
        """
        Clean up any resources that were created during the fetch process.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def transform_item(self, item: Any) -> RawStructure:
        """
        Transform a single item into a RawStructure.
        Must be implemented by subclasses.

        Parameters
        ----------
        item : Any
            The item to transform

        Returns
        -------
        RawStructure
            The transformed structure
        """
        pass
