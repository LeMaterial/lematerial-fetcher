# Copyright 2025 Entalpic
from datetime import datetime
from multiprocessing import Manager
from typing import Any

from lematerial_fetcher.database.postgres import StructuresDatabase
from lematerial_fetcher.fetch import BaseFetcher, ItemsInfo
from lematerial_fetcher.fetcher.mp.utils import add_s3_object_to_db
from lematerial_fetcher.utils.aws import (
    get_aws_client,
    list_s3_objects,
)
from lematerial_fetcher.utils.config import FetcherConfig, load_fetcher_config
from lematerial_fetcher.utils.logging import logger


class MPFetcher(BaseFetcher):
    """
    Materials Project data fetcher implementation.
    Fetches data from the Materials Project AWS OpenData source.

    Parameters
    ----------
    config : FetcherConfig, optional
        Configuration for the fetcher. If None, loads from default location.
    """

    def __init__(self, config: FetcherConfig = None, debug: bool = False):
        super().__init__(config or load_fetcher_config(), debug)
        self.aws_client = None
        # Create a Manager for sharing state between processes
        self.manager = Manager()
        self.manager_dict = self.manager.dict()
        self.manager_dict["occurred"] = False

    def setup_resources(self) -> None:
        """Set up AWS client and database connection."""
        self.aws_client = get_aws_client()
        self.setup_database()

    def get_items_to_process(self) -> ItemsInfo:
        """
        Get list of S3 object keys to process, filtered by modification date.
        Only includes objects that have been modified since the last dataset version.

        Returns
        -------
        ItemsInfo
            Information about S3 objects to process
        """
        # get current dataset version date
        current_version = self.get_current_version()
        current_version_date = None
        if current_version:
            try:
                current_version_date = datetime.strptime(current_version, "%Y-%m-%d")
            except ValueError:
                logger.warning(
                    f"Invalid version date format: {current_version}, will process all items"
                )

        object_keys = list_s3_objects(
            self.aws_client, self.config.mp_bucket_name, self.config.mp_bucket_prefix
        )

        filtered_keys = []
        latest_modified = None  # used to update the dataset version
        for key in object_keys:
            # filter out manifest files and non-JSONL files
            if not (
                key["key"].endswith(".jsonl.gz")
                and "manifest.jsonl.gz" not in key["key"]
            ):
                continue

            try:
                metadata = key["metadata"]
                last_modified = metadata.get("LastModified")

                if latest_modified is None or last_modified > latest_modified:
                    latest_modified = last_modified

                # Include file if:
                # 1. No current version (first sync)
                # 2. Invalid current version date
                # 3. File was modified after current version date
                if (
                    not current_version_date
                    or last_modified.date() >= current_version_date.date()
                ):
                    filtered_keys.append(key["key"])
                    logger.debug(f"Including {key['key']} (modified: {last_modified})")
                else:
                    logger.debug(
                        f"Skipping {key['key']} (not modified since {current_version})"
                    )

            except Exception as e:
                logger.warning(f"Error checking metadata for {key['key']}: {str(e)}")
                # include the file if we can't check its metadata
                filtered_keys.append(key["key"])

        logger.info(
            f"Found {len(filtered_keys)} files to process out of {len(object_keys)} total files"
        )

        self.latest_modified = latest_modified

        return ItemsInfo(
            start_offset=0,
            total_count=len(filtered_keys),
            items=filtered_keys,
        )

    @staticmethod
    def _process_batch(
        batch: Any, config: FetcherConfig, manager_dict: dict, worker_id: int = 0
    ) -> bool:
        """
        Process a single S3 object batch.

        Parameters
        ----------
        batch : str
            The S3 object key to process
        config : FetcherConfig
            Configuration object
        manager_dict : dict
            Shared dictionary for inter-process communication
        worker_id : int
            The id of the worker executing the task

        Returns
        -------
        bool
            True if successful, False if failed
        """
        try:
            # Create new AWS client for this process
            aws_client = get_aws_client()

            # Create new database connection for this process
            db = StructuresDatabase(config.db_conn_str, config.table_name)

            add_s3_object_to_db(
                aws_client, config.mp_bucket_name, batch, db, config.log_every
            )
            return True
        except Exception as e:
            shared_critical_error = BaseFetcher.is_critical_error(e)
            if shared_critical_error and manager_dict is not None:
                manager_dict["occurred"] = True  # shared across processes

            return False

    def cleanup_resources(self) -> None:
        """Clean up AWS client, database connection, and process manager."""
        if self.aws_client:
            # AWS client doesn't need explicit cleanup
            self.aws_client = None

        # Clean up the manager
        if hasattr(self, "manager"):
            self.manager.shutdown()

        super().cleanup_resources()

    def get_new_version(self) -> str:
        """
        Get the new version identifier for the MP dataset.
        Uses the latest modification timestamp from S3 objects.

        Returns
        -------
        str
            New version identifier in YYYY-MM-DD format
        """
        latest_timestamp = self.latest_modified
        if latest_timestamp:
            return latest_timestamp.strftime("%Y-%m-%d")
        return datetime.now().strftime("%Y-%m-%d")  # Fallback to current date


def fetch():
    """
    Fetch materials data from the Materials Project AWS OpenData source.
    This is the main entry point for the MP fetcher.
    """
    fetcher = MPFetcher()
    fetcher.fetch()


if __name__ == "__main__":
    fetch()
