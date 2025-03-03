# Copyright 2025 Entalpic
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from material_fetcher.fetch import BaseFetcher, ItemsInfo
from material_fetcher.fetcher.mp.utils import add_s3_object_to_db
from material_fetcher.model.models import RawStructure
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
        Get list of S3 object keys to process.

        Returns
        -------
        ItemsInfo
            Information about S3 objects to process
        """
        object_keys = list_s3_objects(
            self.aws_client, self.config.mp_bucket_name, self.config.mp_bucket_prefix
        )
        # Filter out non-JSONL files and manifests
        filtered_keys = [
            key
            for key in object_keys
            if key.endswith(".jsonl.gz") and "manifest.jsonl.gz" not in key
        ]

        return ItemsInfo(
            start_offset=0,
            total_count=len(filtered_keys),
            items=filtered_keys,  # Store the keys in ItemsInfo
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

    def read_item(self, item: Any) -> RawStructure:
        """
        Read a JSON line into a RawStructure.

        Parameters
        ----------
        item : Any
            JSON data from S3 object

        Returns
        -------
        RawStructure
            Transformed structure
        """
        if "material_id" not in item:
            # This is a task
            return RawStructure(id=item["task_id"], type="mp-task", attributes=item)
        else:
            # This is a material
            return RawStructure(
                id=item["material_id"], type="mp-material", attributes=item
            )

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


def fetch():
    """
    Fetch materials data from the Materials Project AWS OpenData source.
    This is the main entry point for the MP fetcher.
    """
    fetcher = MPFetcher()
    fetcher.fetch()


if __name__ == "__main__":
    fetch()
