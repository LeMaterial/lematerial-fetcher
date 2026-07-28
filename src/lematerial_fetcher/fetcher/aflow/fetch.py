# Copyright 2025 Entalpic
from datetime import datetime
from multiprocessing import Manager
from typing import Any

from lematerial_fetcher.database.postgres import StructuresDatabase
from lematerial_fetcher.fetch import BaseFetcher, ItemsInfo
from lematerial_fetcher.fetcher.aflow.utils import (
    build_aflux_query,
    parse_aflowlib_date,
)
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.utils.config import FetcherConfig, load_fetcher_config
from lematerial_fetcher.utils.io import create_session
from lematerial_fetcher.utils.logging import logger


class AflowFetcher(BaseFetcher):
    """Fetcher for the AFLOW database, through the AFLUX search API.

    Note: AFLOW also exposes an OPTIMADE endpoint, but it is unreliable at the time
    of writing (persistent HTTP 500), so we query AFLUX directly and request the
    geometry properties needed to rebuild each unit cell.
    """

    def __init__(self, config: FetcherConfig = None, debug: bool = False):
        """Initialize the fetcher."""
        super().__init__(config or load_fetcher_config(), debug)
        self.manager = Manager()
        self.manager_dict = self.manager.dict()
        self.manager_dict["occurred"] = False

    def setup_resources(self) -> None:
        """Set up necessary resources."""
        logger.info("Setting up AFLOW fetcher resources")
        self.setup_database()

    def get_items_to_process(self) -> ItemsInfo:
        """Get information about batches to process.

        AFLUX does not expose a cheap total count, so we use pagination mode:
        batches are generated dynamically during processing and we stop as soon
        as a page comes back empty.
        """
        return ItemsInfo(start_offset=self.config.page_offset)

    @staticmethod
    def _process_batch(
        batch: Any, config: FetcherConfig, manager_dict: dict, worker_id: int = 0
    ) -> bool:
        """
        Process a single page from the AFLUX API.

        Parameters
        ----------
        batch : BatchInfo
            Information about the batch to process (offset and limit)
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
                # AFLUX pages are 1-based
                page = batch.offset // batch.limit + 1
                url = build_aflux_query(
                    config.base_url, page=page, per_page=batch.limit
                )

                response = session.get(url, timeout=120)
                response.raise_for_status()
                data = response.json()

                if not isinstance(data, list) or len(data) == 0:
                    return False

                structures = []
                for entry in data:
                    try:
                        auid = str(entry.get("auid", "")).strip()
                        if not auid:
                            continue
                        last_modified = parse_aflowlib_date(entry.get("aflowlib_date"))
                        structures.append(
                            RawStructure(
                                id=auid,
                                type="aflow-structure",
                                attributes=entry,
                                last_modified=(
                                    last_modified.strftime("%Y-%m-%d")
                                    if last_modified
                                    else None
                                ),
                            )
                        )
                    except Exception as e:
                        logger.warning(
                            f"Error processing item {entry.get('auid', 'unknown')}: {str(e)}"
                        )
                        continue

                if structures:
                    db.batch_insert_data(structures)

                return True

            except Exception as e:
                logger.error(
                    f"Error processing batch: {str(e)} at offset {batch.offset}"
                )
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
        logger.info("Cleaning up AFLOW fetcher resources")

    def get_new_version(self) -> str:
        """Get a new version string."""
        return datetime.utcnow().isoformat()
