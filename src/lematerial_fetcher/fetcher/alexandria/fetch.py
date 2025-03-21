# Copyright 2025 Entalpic
import gc
import os
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Manager
from typing import Any

import orjson
from tqdm import tqdm

from lematerial_fetcher.database.postgres import StructuresDatabase
from lematerial_fetcher.fetch import BaseFetcher, ItemsInfo
from lematerial_fetcher.fetcher.alexandria.utils import sanitize_json
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import Functional
from lematerial_fetcher.utils.config import FetcherConfig, load_fetcher_config
from lematerial_fetcher.utils.io import (
    create_session,
    download_file,
    list_download_links_from_page,
)
from lematerial_fetcher.utils.logging import logger


@dataclass
class BatchInfo:
    """Information about a batch to be processed."""

    offset: int
    limit: int


def get_functional_from_url(url: str) -> Functional:
    """Get the functional from the URL."""
    if "pbe" in url:
        return Functional.PBE
    elif "pbesol" in url:
        return Functional.PBESOL
    elif "scan" in url:
        return Functional.SCAN
    else:
        raise ValueError(f"Unknown functional: {url}")


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

    def __init__(self, config: FetcherConfig = None, debug: bool = False):
        """Initialize the fetcher."""
        super().__init__(config or load_fetcher_config(), debug)
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

    @staticmethod
    def _process_batch(
        batch: Any, config: FetcherConfig, manager_dict: dict, worker_id: int = 0
    ) -> bool:
        """
        Process a single batch from the Alexandria API.

        Parameters
        ----------
        batch : BatchInfo
            Information about the batch to process
        config : FetcherConfig
            Configuration object
        manager_dict : dict
            Shared dictionary for inter-process communication
        worker_id : int
            The ID of the worker

        Returns
        -------
        bool
            True if successful and more data is available, False if failed or no more data
        """
        try:
            db = StructuresDatabase(config.db_conn_str, config.table_name)
            session = create_session()

            try:
                # Fetch the batch
                url = f"{config.base_url}?page_limit={batch.limit}&sort=id&page_offset={batch.offset}"
                response = session.get(url)
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

    def cleanup_resources(self) -> None:
        """Clean up resources."""
        logger.info("Cleaning up Alexandria fetcher resources")

    def get_new_version(self) -> str:
        """Get a new version string."""
        return datetime.utcnow().isoformat()


class AlexandriaTrajectoryFetcher(BaseFetcher):
    """Fetcher for the Alexandria API."""

    def __init__(self, config: FetcherConfig = None, debug: bool = False):
        """Initialize the fetcher."""
        super().__init__(config or load_fetcher_config(), debug)
        self.manager = Manager()
        self.manager_dict = self.manager.dict()
        self.manager_dict["latest_modified"] = None
        self.manager_dict["occurred"] = False

    def setup_resources(self) -> None:
        """Set up necessary resources."""
        logger.info("Setting up Alexandria Trajectory fetcher resources")
        self.setup_database()

    def get_items_to_process(self) -> ItemsInfo:
        """Get information about batches to process."""
        # For Alexandria we just return a starting offset
        # Actual batches will be generated dynamically during processing
        urls = list_download_links_from_page(
            self.config.base_url, pattern=r"\.json\.bz2\s*$"
        )
        logger.info(f"Found {len(urls)} URLs")

        # We only keep the files that have been updated after the latest version's date
        current_version_date = self.get_current_version()
        if current_version_date:
            current_version_date = datetime.fromisoformat(current_version_date)
        filtered_keys = []
        latest_modified = None  # used to update the dataset version
        for url in urls:
            try:
                last_modified = url["last_modified"]
                last_modified = datetime.fromisoformat(
                    last_modified.replace("Z", "+00:00")
                )

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
                    filtered_keys.append((url["url"], last_modified))
                else:
                    logger.debug(
                        f"Skipping {url['url']} (not modified since {current_version_date})"
                    )

            except Exception as e:
                logger.warning(f"Error checking metadata for {url['url']}: {str(e)}")
                # include the file if we can't check its metadata
                filtered_keys.append((url["url"], url["last_modified"]))

        filtered_keys = sorted(filtered_keys, key=lambda x: x[1])
        if self.config.page_offset > 0:
            logger.info(
                f"Skipping {self.config.page_offset} files, starting from {filtered_keys[0][0]}"
            )
        filtered_keys = [(x[0], x[1], i) for i, x in enumerate(filtered_keys)]

        logger.warning(
            f"Running the Alexandria Trajectory fetcher with {len(filtered_keys)} files "
            "to download. This will load whole JSON files into memory, make sure you have enough RAM "
            "for the number of workers that you are spinning up."
        )

        return ItemsInfo(
            start_offset=self.config.page_offset,
            total_count=len(urls),
            items=filtered_keys,
        )

    @staticmethod
    def _process_batch(
        batch: Any, config: FetcherConfig, manager_dict: dict, worker_id: int = 0
    ) -> bool:
        """
        Process a single batch from the Alexandria API.

        Parameters
        ----------
        batch : dict
            Dictionary containing information about the file to process, including 'url'
        config : FetcherConfig
            Configuration object
        manager_dict : dict
            Shared dictionary for inter-process communication
        worker_id : int
            The ID of the worker

        Returns
        -------
        bool
            True if successful, False if failed
        """
        try:
            db = StructuresDatabase(config.db_conn_str, config.table_name)
            file_url, last_modified, offset = batch

            # Download and process the JSON BZ2 file - this can happen in parallel
            file_path = download_file(
                file_url,
                desc=f"Downloading {file_url}",
                decompress="bz2",
                position=worker_id,
            )

            # This is a hack to get the functional from the URL
            functional = get_functional_from_url(file_url)

            structures = []
            file_json = orjson.loads(open(file_path, "rb").read())
            # Get all keys at the root level
            for key, item in tqdm(
                file_json.items(),
                position=worker_id + 1,
                desc=f"Processing {file_url.split('/')[-1]}",
            ):
                item = sanitize_json(item)
                for trajectory in item:
                    trajectory["functional"] = functional

                raw_structure = RawStructure(
                    id=key,
                    type="trajectory",
                    attributes=item,
                    last_modified=last_modified,
                )
                structures.append(raw_structure)

                if len(structures) % config.log_every == 0:
                    db.batch_insert_data(structures)
                    structures = []

            # Insert all remaining structures in a batch
            if structures:
                db.batch_insert_data(structures)

            os.remove(file_path)

            # Update the latest modified date
            manager_dict["latest_modified"] = last_modified

            gc.collect()
            return True

        except Exception as e:
            # Check if this is a critical error
            shared_critical_error = BaseFetcher.is_critical_error(e)
            if shared_critical_error and manager_dict is not None:
                manager_dict["occurred"] = True  # shared across processes
            logger.error(f"Error processing batch: {str(e)} at offset {offset}")
            if os.path.exists(file_path):
                os.remove(file_path)
            gc.collect()
            return False

    def cleanup_resources(self) -> None:
        """Clean up resources."""
        logger.info("Cleaning up Alexandria fetcher resources")

    def get_new_version(self) -> str:
        """Get a new version string."""
        return (
            self.manager_dict["latest_modified"]
            if self.manager_dict["latest_modified"]
            else datetime.min.isoformat()
        )
