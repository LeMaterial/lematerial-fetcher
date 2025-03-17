# Copyright 2025 Entalpic
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
    Convert an OPTiMaDe API item to a RawStructure.

    Parameters
    ----------
    item : Any
        The API item to convert
    latest_modified : datetime
        The latest modified date on all items processed so far

    Returns
    -------
    RawStructure
        The converted structure
    latest_modified : datetime
        The latest modified date on all items processed so far
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


class OQMDFetcher(BaseFetcher):
    """Fetcher for the OQMD OPTiMaDe API."""

    def __init__(self, config: FetcherConfig = None, debug: bool = False):
        """Initialize the fetcher."""
        super().__init__(config or load_fetcher_config(), debug)
        self.manager = Manager()
        self.manager_dict = self.manager.dict()
        self.manager_dict["latest_modified"] = None
        self.manager_dict["occurred"] = False

        # Update base URL to use the OPTiMaDe endpoint
        if not self.config.base_url:
            self.config.base_url = "http://oqmd.org/optimade/structures"

    def setup_resources(self) -> None:
        """Set up necessary resources."""
        logger.info("Setting up OQMD fetcher resources")
        self.setup_database()

    def get_items_to_process(self) -> ItemsInfo:
        """Get information about batches to process."""
        # For OQMD we just return a starting offset
        # Actual batches will be generated dynamically during processing
        return ItemsInfo(start_offset=self.config.page_offset)

    @staticmethod
    def _process_batch(batch: Any, config: FetcherConfig, manager_dict: dict) -> bool:
        """
        Process a single batch from the OQMD API.

        Parameters
        ----------
        batch : BatchInfo
            Information about the batch to process
        config : FetcherConfig
            Configuration object
        manager_dict : dict
            Shared dictionary for inter-process communication

        Returns
        -------
        bool
            True if successful and more data is available, False if failed or no more data
        """
        try:
            db = StructuresDatabase(config.db_conn_str, config.table_name)
            session = create_session()

            try:
                # Construct query parameters for OPTiMaDe API
                params = {
                    "page_limit": batch.limit,
                    "page_offset": batch.offset,
                    "response_format": "json",
                    "sort": "entry_id",
                }

                # Add any additional filters from config
                if hasattr(config, "filters") and config.filters:
                    params["filter"] = config.filters

                # Fetch the batch
                response = session.get(config.base_url, params=params)
                response.raise_for_status()
                data = response.json()

                # Process and store items
                structures = []
                for api_item in data.get("data", []):
                    try:
                        structure, last_modified = read_item(
                            api_item, manager_dict["latest_modified"]
                        )
                        manager_dict["latest_modified"] = last_modified
                        structures.append(structure)
                    except Exception as e:
                        logger.warning(
                            f"Error processing item {api_item.get('id', 'unknown')}: {str(e)}"
                        )
                        continue

                # Insert all structures in a batch
                if structures:
                    db.batch_insert_data(structures)

                # Check if more data is available from the meta information
                return data.get("meta", {}).get("more_data_available", False)

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

    def cleanup_resources(self) -> None:
        """Clean up resources."""
        logger.info("Cleaning up OQMD fetcher resources")

    def get_new_version(self) -> str:
        """Get a new version string."""
        return datetime.utcnow().isoformat()
