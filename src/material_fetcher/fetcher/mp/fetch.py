# Copyright 2025 Entalpic
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from material_fetcher.fetch import BaseFetcher, ItemsInfo
from material_fetcher.fetcher.mp.utils import add_s3_object_to_db
from material_fetcher.utils.aws import (
    get_aws_client,
    list_s3_objects,
)
from material_fetcher.utils.config import FetcherConfig, load_fetcher_config
from material_fetcher.utils.logging import logger


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
        Process S3 objects in parallel using a thread pool.

        Parameters
        ----------
        items_info : ItemsInfo
            Information about S3 objects to process
        """
        if not items_info.items:
            logger.warning("No items to process")
            return

        with ThreadPoolExecutor(max_workers=self.config.num_workers) as executor:
            futures = []

            for key in items_info.items:  # Use the stored keys directly
                future = executor.submit(
                    self._process_s3_object, self.config.mp_bucket_name, key
                )
                futures.append((key, future))

            for key, future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing S3 object {key}: {str(e)}")
                    if self.is_critical_error(e):
                        logger.error("Critical error encountered, shutting down")
                        executor.shutdown(wait=False)
                        raise

    def cleanup_resources(self) -> None:
        """Clean up AWS client and database connection."""
        if self.aws_client:
            # AWS client doesn't need explicit cleanup
            self.aws_client = None
        super().cleanup_resources()

    def _process_s3_object(self, bucket_name: str, object_key: str) -> None:
        """
        Process a single S3 object in a worker thread.

        Parameters
        ----------
        bucket_name : str
            Name of the S3 bucket
        object_key : str
            Key of the S3 object to process

        Raises
        ------
        Exception
            If a critical error occurs during processing
        """
        logger.info(f"Processing file: {object_key}")
        try:
            add_s3_object_to_db(
                self.aws_client, bucket_name, object_key, self.db, self.config.log_every
            )
        except Exception as e:
            logger.error(f"Error processing {object_key}: {str(e)}")
            if self.is_critical_error(e):
                raise

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
