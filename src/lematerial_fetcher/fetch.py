# Copyright 2025 Entalpic
import functools
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from multiprocessing import Manager
from typing import Any, List, Optional

from lematerial_fetcher.database.postgres import DatasetVersions, StructuresDatabase
from lematerial_fetcher.utils.config import FetcherConfig
from lematerial_fetcher.utils.logging import logger


@dataclass
class BatchInfo:
    """Information about a batch to be processed."""

    offset: int
    limit: int


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

    def __init__(self, config: FetcherConfig, debug: bool = False):
        """
        Initialize the fetcher with configuration.

        Parameters
        ----------
        config : FetcherConfig
            Configuration object containing necessary parameters for the fetcher
        debug : bool
            If True, all processing will be done in the main process for debugging
        """
        self.config = config
        self.debug = debug
        self._db = None
        # self.version_db = DatasetVersions(self.config.db_conn_str)
        # self.version_db.create_table()

    @property
    def db(self) -> StructuresDatabase:
        """
        Get the database connection.
        Creates a new connection if one doesn't exist.

        Returns
        -------
        StructuresDatabase
            Database connection
        """
        if self._db is None:
            self._db = self._create_db_connection()
        return self._db

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
            # current_version = self.get_current_version()
            # logger.info(f"Current dataset version: {current_version or 'Not set'}")

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
            # new_version = self.get_new_version()
            # if new_version != current_version:
            #     self.update_version(new_version)
            #     logger.info(f"Updated dataset version to: {new_version}")

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

    def process_items(self, items_info: ItemsInfo) -> None:
        """
        Process items using either pagination or list-based processing depending on context.
        This is a template method that can be overridden by subclasses if needed.

        Parameters
        ----------
        items_info : ItemsInfo
            Information about where to start processing
        """
        if items_info.items is not None:
            self._process_list(items_info)
        else:
            self._process_pagination(items_info)

    def _process_list(self, items_info: ItemsInfo) -> None:
        """
        Process items in either parallel or debug mode for list-based processing.

        Parameters
        ----------
        items_info : ItemsInfo
            Information about items to process
        """
        logger.info(
            f"Starting {'debug' if self.debug else 'parallel'} list processing from offset {items_info.start_offset}"
        )

        if self.debug:
            # Debug mode - process sequentially in main process
            self.manager_dict = {"occurred": False, "latest_modified": None}
            for i in range(items_info.start_offset, len(items_info.items)):
                try:
                    has_more_data = self._process_batch(
                        items_info.items[i], self.config, self.manager_dict
                    )
                    if not has_more_data:
                        logger.warning(f"Failed to process item at index {i}")
                    logger.info(
                        f"Successfully processed item at index {i} containing {items_info.items[i]}"
                    )
                except Exception as e:
                    logger.error(f"Error processing item at index {i}: {str(e)}")
                    if BaseFetcher.is_critical_error(e):
                        raise
        else:
            # Parallel mode - process using process pool
            if not hasattr(self, "manager"):
                self.manager = Manager()
                self.manager_dict = self.manager.dict()
                self.manager_dict["occurred"] = False
                self.manager_dict["latest_modified"] = None

            with ProcessPoolExecutor(max_workers=self.config.num_workers) as executor:
                futures = set()
                current_index = items_info.start_offset
                more_data = True
                worker_id = 0  # Initialize worker counter

                process_func = functools.partial(
                    self.__class__._process_batch,
                    config=self.config,
                    manager_dict=self.manager_dict,
                )

                # Submit initial batches
                initial_batches = self.config.num_workers
                for _ in range(initial_batches):
                    if current_index >= len(items_info.items):
                        break
                    future = executor.submit(
                        process_func,
                        items_info.items[current_index],
                        worker_id=worker_id,
                    )
                    futures.add((items_info.items[current_index], future))
                    current_index += 1
                    worker_id = (
                        worker_id + 1
                    )  # we don't care about cycling through worker IDs, we just increment

                # Process remaining batches with work stealing
                while futures and more_data:
                    done_futures = set()
                    for key, future in futures:
                        if future.done():
                            try:
                                has_more_data = future.result()
                                if not has_more_data:
                                    logger.warning(
                                        f"Failed to process batch {key}. This might be because there is no more data to process at the given URL."
                                    )

                                if self.manager_dict.get("occurred", False):
                                    logger.critical(
                                        "Critical error detected, shutting down process pool"
                                    )
                                    executor.shutdown(wait=False)
                                    raise RuntimeError(
                                        "Critical error occurred during processing"
                                    )

                                logger.info(f"Successfully processed batch {key}")

                                if current_index < len(items_info.items):
                                    next_future = executor.submit(
                                        process_func,
                                        items_info.items[current_index],
                                        worker_id=worker_id,
                                    )
                                    futures.add(
                                        (items_info.items[current_index], next_future)
                                    )
                                    current_index += 1
                                    worker_id = worker_id + 1
                                else:
                                    more_data = False

                            except Exception as e:
                                logger.error(f"Error processing batch {key}: {str(e)}")
                                if BaseFetcher.is_critical_error(e):
                                    logger.critical(
                                        "Critical error detected, shutting down"
                                    )
                                    executor.shutdown(wait=False)
                                    raise

                                if current_index < len(items_info.items):
                                    next_future = executor.submit(
                                        process_func,
                                        items_info.items[current_index],
                                        worker_id=worker_id,
                                    )
                                    futures.add((current_index, next_future))
                                    current_index += 1
                                    worker_id = worker_id + 1
                                else:
                                    more_data = False

                            done_futures.add((key, future))
                            break

                    futures -= done_futures

                # Wait for remaining futures
                for index, future in futures:
                    try:
                        result = future.result()
                        if not result:
                            logger.warning(f"Failed to process batch at index {index}")
                    except Exception as e:
                        logger.error(
                            f"Error processing batch at index {index}: {str(e)}"
                        )

    def _process_pagination(self, items_info: ItemsInfo) -> None:
        """
        Process items in either parallel or debug mode for pagination-based processing.

        Parameters
        ----------
        items_info : ItemsInfo
            Information about items to process
        """
        logger.info(
            f"Starting {'debug' if self.debug else 'parallel'} pagination processing from offset {items_info.start_offset}"
        )

        if self.debug:
            # Debug mode - process sequentially in main process
            current_index = items_info.start_offset
            more_data = True
            self.manager_dict = {"occurred": False, "latest_modified": None}

            while more_data:
                if (
                    items_info.total_count is not None
                    and current_index >= items_info.total_count
                ):
                    more_data = False
                    break
                try:
                    batch_info = BatchInfo(
                        offset=current_index, limit=self.config.page_limit
                    )
                    has_more_data = self._process_batch(
                        batch_info, self.config, self.manager_dict
                    )
                    if not has_more_data:
                        logger.warning(
                            f"Failed to process batch at offset {current_index}"
                        )
                        more_data = False
                    logger.info(
                        f"Successfully processed batch at offset {current_index}"
                    )
                    current_index += self.config.page_limit
                except Exception as e:
                    logger.error(
                        f"Error processing batch at offset {current_index}: {str(e)}"
                    )
                    if BaseFetcher.is_critical_error(e):
                        raise
                    more_data = False
        else:
            # Parallel mode - process using process pool
            if not hasattr(self, "manager"):
                self.manager = Manager()
                self.manager_dict = self.manager.dict()
                self.manager_dict["occurred"] = False

            with ProcessPoolExecutor(max_workers=self.config.num_workers) as executor:
                futures = set()
                current_index = items_info.start_offset
                more_data = True
                if items_info.total_count is not None:
                    more_data = more_data and (current_index <= items_info.total_count)
                worker_id = 0  # Initialize worker counter

                process_func = functools.partial(
                    self.__class__._process_batch,
                    config=self.config,
                    manager_dict=self.manager_dict,
                )

                # Submit initial batches
                initial_batches = self.config.num_workers
                for _ in range(initial_batches):
                    if (
                        items_info.total_count is not None
                        and current_index > items_info.total_count
                    ):
                        break
                    batch_info = BatchInfo(
                        offset=current_index, limit=self.config.page_limit
                    )
                    future = executor.submit(
                        process_func, batch_info, worker_id=worker_id
                    )
                    futures.add((current_index, future))
                    current_index += self.config.page_limit
                    worker_id = (
                        worker_id + 1
                    ) % self.config.num_workers  # Cycle through worker IDs

                # Process remaining batches with work stealing
                while futures and more_data:
                    done_futures = set()
                    for index, future in futures:
                        if future.done():
                            try:
                                has_more_data = future.result()
                                if not has_more_data:
                                    logger.warning(
                                        f"Failed to process batch at offset {index}"
                                    )
                                    more_data = False

                                if self.manager_dict.get("occurred", False):
                                    logger.critical(
                                        "Critical error detected, shutting down process pool"
                                    )
                                    executor.shutdown(wait=False)
                                    raise RuntimeError(
                                        "Critical error occurred during processing"
                                    )

                                logger.info(
                                    f"Successfully processed batch at offset {index}"
                                )

                                # Submit next batch if more data is available
                                if has_more_data:
                                    batch_info = BatchInfo(
                                        offset=current_index,
                                        limit=self.config.page_limit,
                                    )
                                    next_future = executor.submit(
                                        process_func, batch_info, worker_id=worker_id
                                    )
                                    futures.add((current_index, next_future))
                                    current_index += self.config.page_limit
                                    worker_id = (
                                        worker_id + 1
                                    ) % self.config.num_workers
                                else:
                                    more_data = False

                            except Exception as e:
                                logger.error(
                                    f"Error processing batch at offset {index}: {str(e)}"
                                )
                                if BaseFetcher.is_critical_error(e):
                                    logger.critical(
                                        "Critical error detected, shutting down"
                                    )
                                    executor.shutdown(wait=False)
                                    raise

                                if more_data:
                                    batch_info = BatchInfo(
                                        offset=current_index,
                                        limit=self.config.page_limit,
                                    )
                                    next_future = executor.submit(
                                        process_func, batch_info, worker_id=worker_id
                                    )
                                    futures.add((current_index, next_future))
                                    current_index += self.config.page_limit
                                    worker_id = (
                                        worker_id + 1
                                    ) % self.config.num_workers
                                else:
                                    more_data = False

                            done_futures.add((index, future))
                            break

                    futures -= done_futures

                # Wait for remaining futures
                for index, future in futures:
                    try:
                        result = future.result()
                        if not result:
                            logger.warning(f"Failed to process batch at index {index}")
                    except Exception as e:
                        logger.error(
                            f"Error processing batch at index {index}: {str(e)}"
                        )

    @staticmethod
    @abstractmethod
    def _process_batch(
        batch: Any, config: FetcherConfig, manager_dict: dict, worker_id: int = 0
    ) -> bool:
        """
        Process a single batch. Must be implemented by subclasses.

        Parameters
        ----------
        batch : Any
            The batch to process (e.g., S3 object key, API batch info)
        config : FetcherConfig
            Configuration object
        manager_dict : dict
            Shared dictionary for inter-process communication
        worker_id: int
            The id of the worker executing the task

        Returns
        -------
        bool
            True if successful and more data is available, False if failed or no more data
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
        self._db = None
        self._version_db = None
