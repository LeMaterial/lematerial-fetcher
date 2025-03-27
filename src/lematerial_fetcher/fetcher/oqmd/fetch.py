# Copyright 2025 Entalpic
from multiprocessing import Manager

from lematerial_fetcher.database.mysql import MySQLDatabase
from lematerial_fetcher.fetch import BaseFetcher, BatchInfo, ItemsInfo
from lematerial_fetcher.fetcher.oqmd.utils import (
    download_and_process_oqmd_sql,
    get_oqmd_version_if_exists,
)
from lematerial_fetcher.utils.config import FetcherConfig, load_fetcher_config
from lematerial_fetcher.utils.logging import logger


class OQMDFetcher(BaseFetcher):
    """Fetcher for the OQMD database."""

    def __init__(self, config: FetcherConfig = None, debug: bool = False):
        """Initialize the fetcher."""
        super().__init__(config or load_fetcher_config(), debug)
        self.manager = Manager()
        self.manager_dict = self.manager.dict()
        self.manager_dict["occurred"] = False

    def setup_resources(self) -> None:
        """Set up necessary resources.

        This method does the heavy lifting of downloading and processing the OQMD database.
        The output is a MySQL database with all the OQMD entries, which is considered to be the
        dump from OQMD.
        """
        logger.info("Setting up OQMD fetcher resources")

        # Download and process the SQL database if needed
        download_and_process_oqmd_sql(
            self.config.mysql_config,
            self.config.base_url,
            self.config.oqmd_download_dir,
        )
        logger.info("SQL database processing completed successfully")

    def get_items_to_process(self) -> ItemsInfo:
        """Get information about batches to process from the MySQL database.

        Returns
        -------
        ItemsInfo
            Information about the total number of entries to process and where to start.
        """
        # Connect to MySQL database
        db = MySQLDatabase(**self.config.mysql_config)

        try:
            # Get the total count of entries
            result = db.fetch_items(query="SELECT COUNT(id) as count FROM entries")[0]
            total_count = result["count"]

            logger.info(
                f"{total_count} entries are available in the MySQL database {self.config.mysql_config['database']} for OQMD"
            )

            # Return pagination info
            return ItemsInfo(start_offset=0, total_count=0)

        finally:
            db.close()

    @staticmethod
    def _process_batch(
        batch: BatchInfo, config: FetcherConfig, manager_dict: dict, worker_id: int = 0
    ) -> bool:
        """Process a batch of entries from the OQMD database.

        For OQMD, the database is already downloaded and processed in the setup_resources method.
        This method is just a placeholder to satisfy the BaseFetcher interface.

        Parameters
        ----------
        batch : BatchInfo
            Information about the batch to process, including offset and limit
        config : FetcherConfig
            Configuration object
        manager_dict : dict
            Shared dictionary for inter-process communication
        worker_id : int
            The id of the worker executing the task

        Returns
        -------
        bool
            True if successful and more data might be available, False otherwise
        """
        return False

    def cleanup_resources(self) -> None:
        """Clean up resources."""
        logger.info("Cleaning up OQMD fetcher resources")
        super().cleanup_resources()

    def get_new_version(self) -> str:
        """Get a new version string."""
        version_config = self.config.mysql_config.copy()
        version_config["database"] = f"{self.config.mysql_config['database']}_version"
        return get_oqmd_version_if_exists(db_config=version_config)
