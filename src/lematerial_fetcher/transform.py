# Copyright 2025 Entalpic
import sys
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from multiprocessing import Manager
from typing import Any, Generic, Optional, Type, TypeVar

from tqdm import tqdm

from lematerial_fetcher.database.postgres import (
    DatasetVersions,
    OptimadeDatabase,
    StructuresDatabase,
)
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import OptimadeStructure
from lematerial_fetcher.utils.config import TransformerConfig, load_transformer_config
from lematerial_fetcher.utils.logging import logger

# type variables for the database and structure types
TDatabase = TypeVar("TDatabase")
TStructure = TypeVar("TStructure")


def process_batch(
    worker_id: int,
    offset: int,
    limit: int,
    task_table_name: Optional[str],
    config: TransformerConfig,
    database_class: Type[TDatabase],
    structure_class: Type[TStructure],
    transformer_class: Type["BaseTransformer[TDatabase, TStructure]"],
    manager_dict: dict,
) -> None:
    """
    Process a range of rows in a worker process using a server-side cursor.

    Parameters
    ----------
    worker_id : int
        Identifier for the worker
    offset : int
        The offset to start fetching rows from
    limit : int
        The total number of rows to process (end_offset - start_offset)
    task_table_name : Optional[str]
        Task table name to read targets or trajectories from.
        This is only used for Materials Project.
    config : TransformerConfig
        Configuration object
    database_class : Type[TDatabase]
        The class to use for the target database
    structure_class : Type[TStructure]
        The class to use for the transformed structures
    transformer_class : Type["BaseTransformer[TDatabase, TStructure]"]
        The transformer class to use for transformation
    manager_dict : dict
        Shared dictionary to signal critical errors across processes
    """
    try:
        # Create new database connections for this process
        source_db = StructuresDatabase(
            config.source_db_conn_str, config.source_table_name
        )
        target_db = database_class(config.dest_db_conn_str, config.dest_table_name)

        # transform the rows into TStructure objects
        transformer = transformer_class(
            config=config,
            database_class=database_class,
            structure_class=structure_class,
        )

        processed_count = 0
        for raw_structure in (
            pbar := tqdm(
                source_db.fetch_items_iter(
                    offset=offset,
                    limit=limit,
                    batch_size=config.db_fetch_batch_size,
                    cursor_name=f"transform_cursor_{worker_id}",
                ),
                total=limit,
                position=worker_id,
                desc=f"Worker {worker_id} ({offset} -> {offset + limit})",
                leave=False,
                file=sys.stdout,
                dynamic_ncols=True,
                mininterval=1.0,
                maxinterval=10.0,
                miniters=1,
            )
        ):
            try:
                structures = transformer.transform_row(
                    raw_structure, source_db=source_db, task_table_name=task_table_name
                )

                target_db.batch_insert_data(structures)
                del structures

                processed_count += 1
                pbar.update(1)

            except Exception as e:
                logger.warning(f"Error processing {raw_structure.id} row: {str(e)}")
                # Check if this is a critical error
                if BaseTransformer.is_critical_error(e):
                    manager_dict["occurred"] = True  # shared across processes
                    return

    except Exception as e:
        logger.error(f"Process initialization error: {str(e)}")
        if BaseTransformer.is_critical_error(e):
            manager_dict["occurred"] = True  # shared across processes

    finally:
        source_db.close()
        target_db.close()


class BaseTransformer(ABC, Generic[TDatabase, TStructure]):
    """
    Abstract base class for all material data transformers.

    This class defines the common interface and shared functionality that all transformers
    must implement. It handles common operations like database setup and parallel processing
    while allowing specific implementations to define their own transformation logic.

    Parameters
    ----------
    config : TransformerConfig, optional
        Configuration object containing necessary parameters for the transformer.
    database_class : Type[TDatabase]
        The class to use for the target database
    structure_class : Type[TStructure]
        The class to use for the transformed structures
    debug : bool, optional
        If True, runs transformations in the main process for debugging purposes.
        Defaults to False.
    """

    def __init__(
        self,
        config: Optional[TransformerConfig] = None,
        database_class: Type[TDatabase] = OptimadeDatabase,
        structure_class: Type[TStructure] = OptimadeStructure,
        debug: bool = False,
    ):
        self.config = config or load_transformer_config()
        self._database_class = database_class
        self._structure_class = structure_class
        self.debug = debug
        if not debug:
            self.manager = Manager()
            self.manager_dict = self.manager.dict()
            self.manager_dict["occurred"] = False
        else:
            self.manager_dict = {}

    def setup_databases(self) -> None:
        """Set up source and target database tables."""
        target_db = self._database_class(
            self.config.dest_db_conn_str, self.config.dest_table_name
        )
        target_db.create_table()

    def transform(self) -> None:
        """
        Main entry point for transforming data. Implements the template method pattern
        for the transformation process.

        This method orchestrates the transformation process by setting up databases,
        processing rows in parallel, and handling any errors.

        Raises
        ------
        Exception
            If any error occurs during the transformation process
        """
        try:
            logger.info(f"Starting transform process for {self.__class__.__name__}")

            current_version = self.get_transform_version()
            logger.info(f"Current transform version: {current_version or 'Not set'}")

            self.setup_databases()

            self._process_rows()

            new_version = self.get_new_transform_version()
            if new_version != current_version:
                self.update_transform_version(new_version)
                logger.info(f"Updated transform version to: {new_version}")

            logger.info("Successfully completed transforming database records")
        except Exception as e:
            logger.fatal(f"Error during transform: {str(e)}")
            raise
        finally:
            self.cleanup_resources()

    def get_transform_version(self) -> Optional[str]:
        """
        Get the current transform version.
        This version tracks the state of transformed data.

        Returns
        -------
        Optional[str]
            Current transform version or None if not set
        """
        try:
            version_db = DatasetVersions(self.config.dest_db_conn_str)
            current_version = version_db.get_last_synced_version(
                f"{self.config.dest_table_name}_transform"
            )
            return current_version
        except Exception as e:
            logger.error(f"Error getting transform version: {str(e)}")
            return None

    def update_transform_version(self, version: str) -> None:
        """
        Update the transform version.

        Parameters
        ----------
        version : str
            New transform version
        """
        version_db = DatasetVersions(self.config.dest_db_conn_str)
        version_db.update_version(f"{self.config.dest_table_name}_transform", version)

    def get_new_transform_version(self) -> str:
        """
        Get the new transform version.
        Subclasses should implement specific version generation logic.

        Returns
        -------
        str
            New transform version
        """
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def cleanup_resources(self) -> None:
        """Clean up database connections and process manager."""
        if hasattr(self, "manager"):
            self.manager.shutdown()

    def _process_rows(self) -> None:
        """
        Process rows from source database in parallel, transform them, and store in target database.
        Processes rows in batches to avoid memory issues. Uses a work-stealing approach where workers
        can grab new work immediately without waiting for other workers.

        Raises
        ------
        Exception
            If a critical error occurs during processing
        """
        batch_size = self.config.batch_size
        offset = self.config.page_offset
        total_processed = 0
        task_table_name = self.config.mp_task_table_name

        if self.config.max_offset is not None:
            max_offset = self.config.max_offset
        else:
            max_offset = float("inf")

        if self.debug:
            # Debug mode: process in main process
            while True:
                if offset >= max_offset:
                    break

                # Process batch in main process
                process_batch(
                    0,
                    offset,
                    batch_size,
                    task_table_name,
                    self.config,
                    self._database_class,
                    self._structure_class,
                    self.__class__,
                    self.manager_dict,
                )

                # Check if we should continue
                source_db = StructuresDatabase(
                    self.config.source_db_conn_str, self.config.source_table_name
                )
                rows = source_db.fetch_items(offset=offset + batch_size, batch_size=1)
                source_db.close()
                if not rows:
                    break

                total_processed += batch_size
                logger.info(f"Total processed: {total_processed}")
                offset += batch_size

            logger.info(f"Completed processing {total_processed} total rows")
            return

        # Normal mode: process in parallel with work stealing
        with ProcessPoolExecutor(max_workers=self.config.num_workers) as executor:
            futures = set()

            # Submit initial batch of tasks
            for i in range(self.config.num_workers):
                if offset + (i * batch_size) >= max_offset:
                    break

                future = executor.submit(
                    process_batch,
                    i,
                    offset + (i * batch_size),
                    batch_size,
                    task_table_name,
                    self.config,
                    self._database_class,
                    self._structure_class,
                    self.__class__,
                    self.manager_dict,
                )
                futures.add((i, offset + (i * batch_size), future))
                total_processed += batch_size

            offset += batch_size * self.config.num_workers
            more_data = True

            while futures and more_data:
                # Check for completed futures and remove them
                done_futures = set()
                for worker_id, current_offset, future in futures:
                    if future.done():
                        try:
                            future.result()

                            if self.manager_dict.get("occurred", False):
                                logger.critical(
                                    "Critical error detected, shutting down process pool"
                                )
                                executor.shutdown(wait=False)
                                raise RuntimeError(
                                    "Critical error occurred during processing"
                                )

                            # Check if there might be more data
                            source_db = StructuresDatabase(
                                self.config.source_db_conn_str,
                                self.config.source_table_name,
                            )
                            check_rows = source_db.fetch_items(
                                offset=offset, batch_size=1
                            )
                            source_db.close()

                            if check_rows:
                                if offset >= max_offset:
                                    more_data = False

                                # Submit new task
                                next_future = executor.submit(
                                    process_batch,
                                    worker_id,
                                    offset,
                                    batch_size,
                                    task_table_name,
                                    self.config,
                                    self._database_class,
                                    self._structure_class,
                                    self.__class__,
                                    self.manager_dict,
                                )
                                futures.add((worker_id, offset, next_future))
                                offset += batch_size
                            else:
                                more_data = False

                            logger.info(
                                f"Successfully processed batch at offsets {current_offset} -> {current_offset + batch_size}"
                            )

                        except Exception as e:
                            logger.error(f"Critical error encountered: {str(e)}")
                            executor.shutdown(wait=False)
                            raise

                        done_futures.add((worker_id, current_offset, future))
                        break

                futures -= done_futures

            # Wait for remaining futures
            for worker_id, current_offset, future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(
                        f"Error processing batch at offset {current_offset}: {str(e)}"
                    )

            logger.info(
                f"Completed processing approximately {total_processed} total rows"
            )

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
    def transform_row(
        self,
        raw_structure: RawStructure | dict[str, Any],
        source_db: Optional[StructuresDatabase] = None,
        task_table_name: Optional[str] = None,
    ) -> list[TStructure]:
        """
        Transform a raw structure into structures of type TStructure.
        Must be implemented by subclasses.

        Parameters
        ----------
        raw_structure : RawStructure
            RawStructure object from the dumped database
        source_db : Optional[StructuresDatabase]
            Source database connection
        task_table_name : Optional[str]
            Task table name to read targets or trajectories from.
            This is only used for Materials Project.

        Returns
        -------
        list[TStructure]
            List of transformed structure objects.
            If the list is empty, the row will be skipped.
        """
        pass
