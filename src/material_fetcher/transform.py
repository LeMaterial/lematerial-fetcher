# Copyright 2025 Entalpic
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from threading import local
from typing import Optional

from material_fetcher.database.postgres import (
    OptimadeDatabase,
    StructuresDatabase,
)
from material_fetcher.model.models import RawStructure
from material_fetcher.model.optimade import OptimadeStructure
from material_fetcher.utils.config import TransformerConfig, load_transformer_config
from material_fetcher.utils.logging import logger


class BaseTransformer(ABC):
    """
    Abstract base class for all material data transformers.

    This class defines the common interface and shared functionality that all transformers
    must implement. It handles common operations like database setup and parallel processing
    while allowing specific implementations to define their own transformation logic.

    Parameters
    ----------
    config : TransformerConfig, optional
        Configuration object containing necessary parameters for the transformer.
        If None, loads from default location.
    """

    def __init__(self, config: Optional[TransformerConfig] = None):
        self.config = config or load_transformer_config()
        self._thread_local = local()

    @property
    def source_db(self) -> StructuresDatabase:
        """
        Get the source database connection for the current thread.
        Creates a new connection if one doesn't exist.

        Returns
        -------
        StructuresDatabase
            Source database connection for the current thread
        """
        if not hasattr(self._thread_local, "source_db"):
            self._thread_local.source_db = StructuresDatabase(
                self.config.source_db_conn_str, self.config.source_table_name
            )
        return self._thread_local.source_db

    @property
    def target_db(self) -> OptimadeDatabase:
        """
        Get the target database connection for the current thread.
        Creates a new connection if one doesn't exist.

        Returns
        -------
        OptimadeDatabase
            Target database connection for the current thread
        """
        if not hasattr(self._thread_local, "target_db"):
            self._thread_local.target_db = OptimadeDatabase(
                self.config.dest_db_conn_str, self.config.dest_table_name
            )
        return self._thread_local.target_db

    def setup_databases(self) -> None:
        """Set up source and target database tables."""
        target_db = OptimadeDatabase(
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
            self.setup_databases()
            self._process_rows()
            logger.info("Successfully completed transforming database records")
        except Exception as e:
            logger.fatal(f"Error during transform: {str(e)}")
            raise
        finally:
            self.cleanup_resources()

    def cleanup_resources(self) -> None:
        """Clean up database connections for all threads."""
        if hasattr(self._thread_local, "source_db"):
            delattr(self._thread_local, "source_db")
        if hasattr(self._thread_local, "target_db"):
            delattr(self._thread_local, "target_db")

    def _process_rows(self) -> None:
        """
        Process rows from source database in parallel, transform them, and store in target database.
        Processes rows in batches to avoid memory issues.

        Raises
        ------
        Exception
            If a critical error occurs during processing
        """
        with ThreadPoolExecutor(max_workers=self.config.num_workers) as executor:
            batch_size = self.config.batch_size
            offset = 0
            total_processed = 0
            task_table_name = self.config.mp_task_table_name

            while True:
                # get batch of rows from source table
                rows = self.source_db.fetch_items(offset=offset, batch_size=batch_size)
                if not rows:
                    break

                total_processed += len(rows)
                logger.info(
                    f"Processing batch of {len(rows)} rows (total processed: {total_processed})"
                )

                # submit transformation tasks for this batch
                futures = [
                    executor.submit(
                        self._worker,
                        total_processed - len(rows) + i,
                        row,
                        task_table_name,
                    )
                    for i, row in enumerate(rows, 1)
                ]

                # wait for batch to complete
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Critical error encountered: {str(e)}")
                        executor.shutdown(wait=False)
                        raise

                offset += batch_size

            logger.info(f"Completed processing {total_processed} total rows")

    def _worker(
        self,
        worker_id: int,
        raw_structure: RawStructure,
        task_table_name: Optional[str] = None,
    ) -> None:
        """
        Transform a single row and store it in the target database.

        Parameters
        ----------
        worker_id : int
            Identifier for the worker thread
        raw_structure : RawStructure
            RawStructure object from the dumped database
        task_table_name : Optional[str]
            Task table name to read targets or trajectories from.
            This is only used for Materials Project.

        Raises
        ------
        Exception
            If an error occurs during transformation or database insertion
        """
        try:
            # transform the row into OptimadeStructures
            optimade_structures = self.transform_row(raw_structure, task_table_name)

            for optimade_structure in optimade_structures:
                self.target_db.insert_data(optimade_structure)

            if worker_id % self.config.log_every == 0:
                logger.info(f"Transformed {worker_id} records")

        except Exception as e:
            logger.error(f"Worker {worker_id} error processing row: {str(e)}")
            raise

    @abstractmethod
    def transform_row(
        self,
        raw_structure: RawStructure,
        task_table_name: Optional[str] = None,
    ) -> list[OptimadeStructure]:
        """
        Transform a raw structure into OptimadeStructures.
        Must be implemented by subclasses.

        Parameters
        ----------
        raw_structure : RawStructure
            RawStructure object from the dumped database
        task_table_name : Optional[str]
            Task table name to read targets or trajectories from.
            This is only used for Materials Project.

        Returns
        -------
        list[OptimadeStructure]
            List of transformed OptimadeStructure objects.
            If the list is empty, the row will be skipped.
        """
        pass
