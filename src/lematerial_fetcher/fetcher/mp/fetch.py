# Copyright 2025 Entalpic
import functools
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from multiprocessing import Manager

from lematerial_fetcher.database.postgres import StructuresDatabase
from lematerial_fetcher.fetch import BaseFetcher, ItemsInfo
from lematerial_fetcher.fetcher.mp.utils import add_s3_object_to_db
from lematerial_fetcher.utils.aws import (
    get_aws_client,
    list_s3_objects,
)
from lematerial_fetcher.utils.config import FetcherConfig, load_fetcher_config
from lematerial_fetcher.utils.logging import logger


def process_s3_object(bucket_name, object_key, config, log_every, manager_dict=None):
    """
    Process a single S3 object in a worker process. Each process creates its own
    database connection and AWS client.

    Parameters
    ----------
    bucket_name : str
        Name of the S3 bucket
    object_key : str
        Key of the S3 object to process
    config : FetcherConfig
        Configuration object
    log_every : int
        How often to log progress
    manager_dict : dict, optional
        Shared dictionary to signal critical errors across processes

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

        add_s3_object_to_db(aws_client, bucket_name, object_key, db, log_every)
        return True
    except Exception as e:
        shared_critical_error = BaseFetcher.is_critical_error(e)
        if shared_critical_error and manager_dict is not None:
            manager_dict["occurred"] = True  # shared across processes

        return False


class MPFetcher(BaseFetcher):
    """
    Materials Project data fetcher implementation.
    Fetches data from the Materials Project AWS OpenData source.

    Parameters
    ----------
    config : FetcherConfig, optional
        Configuration for the fetcher. If None, loads from default location.
    """

    def __init__(self, config: FetcherConfig = None):
        super().__init__(config or load_fetcher_config())
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

    def process_items(self, items_info: ItemsInfo) -> None:
        """
        Process S3 objects in parallel using a process pool.

        Parameters
        ----------
        items_info : ItemsInfo
            Information about S3 objects to process
        """
        if not items_info.items:
            logger.warning("No items to process")
            return

        # Reset critical error flag before starting
        self.manager_dict["occurred"] = False

        # Create a partial function with fixed parameters
        process_func = functools.partial(
            process_s3_object,
            self.config.mp_bucket_name,
            config=self.config,
            log_every=self.config.log_every,
            manager_dict=self.manager_dict,
        )

        with ProcessPoolExecutor(max_workers=self.config.num_workers) as executor:
            futures = []

            # Submit all jobs
            for key in items_info.items:
                futures.append((key, executor.submit(process_func, key)))

            # Process results as they complete
            failed_count = 0
            for key, future in futures:
                try:
                    result = future.result()
                    if not result:
                        failed_count += 1

                    # Check for critical errors and shutdown if needed
                    if self.manager_dict.get("occurred", False):
                        logger.critical(
                            "Critical error detected, shutting down process pool"
                        )
                        executor.shutdown(wait=False)
                        raise RuntimeError("Critical error occurred during processing")

                    logger.info(f"Successfully processed {key}")

                except Exception as e:
                    logger.error(f"Error processing S3 object {key}: {str(e)}")
                    failed_count += 1

            if failed_count > 0:
                logger.warning(f"{failed_count} items failed to process")

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
