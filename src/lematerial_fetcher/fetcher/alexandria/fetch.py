# Copyright 2025 Entalpic
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from threading import Lock
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lematerial_fetcher.fetch import BaseFetcher, ItemsInfo
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.utils.config import FetcherConfig, load_fetcher_config
from lematerial_fetcher.utils.logging import logger


@dataclass
class BatchInfo:
    """Information about a batch to be processed."""

    offset: int
    limit: int


class AlexandriaFetcher(BaseFetcher):
    """
    Alexandria API data fetcher implementation.
    Fetches structure data from the Alexandria API endpoint.

    Parameters
    ----------
    config : FetcherConfig, optional
        Configuration for the fetcher. If None, loads from default location.
    """

    def __init__(self, config: FetcherConfig = None):
        super().__init__(config or load_fetcher_config())
        self._latest_modified_date: Optional[datetime] = None
        self._lock = Lock()  # For thread-safe updates

    def setup_resources(self) -> None:
        """Set up database connection."""
        self.setup_database()

    def get_items_to_process(self) -> ItemsInfo:
        """Get information about items to process."""
        return ItemsInfo(start_offset=self.config.page_offset)

    def process_items(self, items_info: ItemsInfo) -> None:
        """Process items in parallel, starting from the given offset."""
        with ThreadPoolExecutor(max_workers=self.config.num_workers) as executor:
            futures = []
            offset = items_info.start_offset

            while True:
                future = executor.submit(
                    self._process_batch, offset, self.config.page_limit
                )
                futures.append((offset, future))
                offset += self.config.page_limit

                # Check if we've reached the end of data
                try:
                    if not future.result():
                        break
                except Exception as e:
                    logger.error(f"Error processing batch at offset {offset}: {str(e)}")
                    continue

            # Process remaining futures
            for batch_offset, future in futures:
                if not future.done():
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(
                            f"Error processing batch at offset {batch_offset}: {str(e)}"
                        )

    def _process_batch(self, offset: int, limit: int) -> bool:
        """
        Process a single batch from the API.

        Parameters
        ----------
        offset : int
            Offset for the batch
        limit : int
            Limit for the batch

        Returns
        -------
        bool
            True if the batch was processed successfully, False if it's the end of data
        """
        session = self._create_session()

        try:
            # Fetch the batch
            url = f"{self.config.base_url}?page_limit={limit}&sort=id&page_offset={offset}"
            response = session.get(url)
            response.raise_for_status()
            data = response.json()

            # Process and store items
            for item in data.get("data", []):
                try:
                    structure = self.read_item(item)
                    self.db.insert_data(structure)
                except Exception as e:
                    logger.warning(
                        f"Error processing item {item.get('id', 'unknown')}: {str(e)}"
                    )
                    continue

            logger.info(f"Successfully processed batch at offset {offset}")
            return True

        except Exception as e:
            logger.error(f"Error processing batch at offset {offset}: {str(e)}")
            return False
        finally:
            session.close()

    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry configuration.

        Returns
        -------
        requests.Session
            Configured session object
        """
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def cleanup_resources(self) -> None:
        """Clean up database connections."""
        if hasattr(self._thread_local, "db"):
            delattr(self._thread_local, "db")

    def read_item(self, item: Any) -> RawStructure:
        """Transform a raw API item into a RawStructure."""
        last_modified = item["attributes"].get("last_modified", None)
        if last_modified:
            last_modified = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
            # update the last modified date if it's the latest
            with self._lock:
                if (
                    self._latest_modified_date is None
                    or last_modified > self._latest_modified_date
                ):
                    self._latest_modified_date = last_modified
            last_modified = last_modified.strftime("%Y-%m-%d")
        return RawStructure(
            id=item["id"],
            type=item["type"],
            attributes=item["attributes"],
            last_modified=last_modified,
        )

    def get_new_version(self) -> str:
        """
        Get the new version identifier for the Alexandria dataset.
        Uses the API version or timestamp from the API response.

        Returns
        -------
        str
            New version identifier in YYYY-MM-DD format
        """
        if self.last_modified:
            return self.last_modified.strftime("%Y-%m-%d")
        return self.get_current_version()
