# Copyright 2025 Entalpic
import functools
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Manager
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lematerial_fetcher.database.postgres import StructuresDatabase
from lematerial_fetcher.fetch import BaseFetcher, ItemsInfo
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.utils.config import FetcherConfig, load_fetcher_config
from lematerial_fetcher.utils.logging import logger


@dataclass
class BatchInfo:
    """Information about a batch to be processed."""

    offset: int
    limit: int


def process_batch(base_url, batch_info, config, manager_dict=None):
    """
    Process a single batch from the API in a worker process.

    Parameters
    ----------
    base_url : str
        Base URL for the API
    batch_info : BatchInfo
        Information about the batch to process
    config : FetcherConfig
        Configuration object
    manager_dict : dict, optional
        Shared dictionary to signal critical errors across processes

    Returns
    -------
    bool
        True if successful, False if failed
    """
    try:
        db = StructuresDatabase(config.db_conn_str, config.table_name)

        session = create_session()

        try:
            # Fetch the batch
            url = f"{base_url}?page_limit={batch_info.limit}&sort=id&page_offset={batch_info.offset}"
            response = session.get(url)
            response.raise_for_status()
            data = response.json()

            # Process and store items
            item_count = 0
            for item in data.get("data", []):
                try:
                    structure, last_modified = read_item(
                        item, manager_dict["latest_modified"]
                    )
                    manager_dict["latest_modified"] = last_modified
                    db.insert_data(structure)
                    item_count += 1
                except Exception as e:
                    logger.warning(
                        f"Error processing item {item.get('id', 'unknown')}: {str(e)}"
                    )
                    continue

            return len(data.get("data", [])) > 0

        except Exception as e:
            # Check if this is a critical error
            shared_critical_error = BaseFetcher.is_critical_error(e)
            if shared_critical_error and manager_dict is not None:
                manager_dict["occurred"] = True  # shared across processes

            return False
        finally:
            session.close()

    except Exception as e:
        logger.error(f"Process initialization error: {str(e)}")
        return False


def create_session() -> requests.Session:
    """Create a session with retry capability."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def read_item(item: Any, latest_modified: datetime) -> RawStructure:
    """
    Convert an API item to a RawStructure.

    Parameters
    ----------
    item : Any
        The API item to convert
    latest_modified : datetime
        The latest modified date

    Returns
    -------
    RawStructure
        The converted structure
    """
    last_modified = item["attributes"].get("last_modified", None)
    if last_modified:
        last_modified = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
        # update the last modified date if it's the latest
        if latest_modified is None or last_modified > latest_modified:
            latest_modified = last_modified
        last_modified = last_modified.strftime("%Y-%m-%d")
    return RawStructure(
        id=item["id"],
        type=item["type"],
        attributes=item["attributes"],
        last_modified=last_modified,
    ), latest_modified


class AlexandriaFetcher(BaseFetcher):
    """Fetcher for the Alexandria API."""

    def __init__(self, config: FetcherConfig = None):
        """Initialize the fetcher."""
        super().__init__(config or load_fetcher_config())
        self.manager = Manager()
        self.manager_dict = self.manager.dict()
        self.manager_dict["latest_modified"] = None
        self.manager_dict["occurred"] = False

    def setup_resources(self) -> None:
        """Set up necessary resources."""
        logger.info("Setting up Alexandria fetcher resources")
        self.setup_database()

    def get_items_to_process(self) -> ItemsInfo:
        """Get information about batches to process."""
        # For Alexandria we just return a starting offset
        # Actual batches will be generated dynamically during processing
        return ItemsInfo(start_offset=self.config.page_offset)

    def process_items(self, items_info: ItemsInfo) -> None:
        """
        Process batches in parallel using a process pool.

        Parameters
        ----------
        items_info : ItemsInfo
            Information about batches to process
        """
        logger.info(f"Starting processing from offset {items_info.start_offset}")

        self.manager_dict["occurred"] = False

        with ProcessPoolExecutor(max_workers=self.config.num_workers) as executor:
            futures = []
            current_offset = items_info.start_offset
            more_data = True

            process_func = functools.partial(
                process_batch,
                self.config.base_url,
                config=self.config,
                manager_dict=self.manager_dict,
            )

            initial_batches = min(
                self.config.num_workers * 2, 10
            )  # Start with 2 jobs per worker
            for _ in range(initial_batches):
                batch_info = BatchInfo(
                    offset=current_offset, limit=self.config.page_limit
                )
                futures.append((batch_info, executor.submit(process_func, batch_info)))
                current_offset += self.config.page_limit

            # Process results and submit new jobs as needed
            while futures and more_data:
                for i, (batch_info, future) in enumerate(futures):
                    if future.done():
                        try:
                            has_more_data = future.result()
                            if not has_more_data:
                                more_data = False
                                logger.info("Reached end of data")
                                break

                            if self.manager_dict.get("occurred", False):
                                logger.critical(
                                    "Critical error detected, shutting down process pool"
                                )
                                executor.shutdown(wait=False)
                                raise RuntimeError(
                                    "Critical error occurred during processing"
                                )

                            logger.info(
                                f"Successfully processed batch at offset {batch_info.offset}"
                            )

                            if more_data:
                                new_batch_info = BatchInfo(
                                    offset=current_offset, limit=self.config.page_limit
                                )
                                futures.append(
                                    (
                                        new_batch_info,
                                        executor.submit(process_func, new_batch_info),
                                    )
                                )
                                current_offset += self.config.page_limit

                        except Exception as e:
                            logger.error(
                                f"Error processing batch at offset {batch_info.offset}: {str(e)}"
                            )

                            if BaseFetcher.is_critical_error(e):
                                logger.critical(
                                    "Critical error detected, shutting down"
                                )
                                executor.shutdown(wait=False)
                                raise

                            # Submit a new job even if this one failed
                            if more_data:
                                new_batch_info = BatchInfo(
                                    offset=current_offset, limit=self.config.page_limit
                                )
                                futures.append(
                                    (
                                        new_batch_info,
                                        executor.submit(process_func, new_batch_info),
                                    )
                                )
                                current_offset += self.config.page_limit

                        futures.pop(i)
                        break

            for batch_info, future in futures:
                if not future.done():
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(
                            f"Error processing batch at offset {batch_info.offset}: {str(e)}"
                        )

    def cleanup_resources(self) -> None:
        """Clean up resources."""
        logger.info("Cleaning up Alexandria fetcher resources")

    def get_new_version(self) -> str:
        """Get a new version string."""
        return datetime.utcnow().isoformat()
