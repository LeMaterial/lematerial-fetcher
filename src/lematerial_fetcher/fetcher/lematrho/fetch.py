# Copyright 2025 Entalpic
from datetime import datetime
from multiprocessing import Manager
from typing import Any, Optional

from lematerial_fetcher.database.postgres import StructuresDatabase
from lematerial_fetcher.fetch import BaseFetcher, ItemsInfo
from lematerial_fetcher.fetcher.lematrho.utils import (
    build_raw_structure,
    compress_chgcar,
    download_gz_file_from_s3,
    parse_vasprun_structure,
)
from lematerial_fetcher.utils.aws import get_authenticated_aws_client
from lematerial_fetcher.utils.config import FetcherConfig, load_fetcher_config
from lematerial_fetcher.utils.logging import logger

# S3 folder structure constants
STATIC_CALC_TYPE = "LeMatRhoStaticMaker"
RELAX_CALC_TYPE = "LeMatRhoRelaxMaker_1"
STATIC_FILES = ["CHGCAR.gz", "AECCAR0.gz", "AECCAR1.gz", "AECCAR2.gz"]
RELAX_FILES = ["vasprun.xml.gz"]

# Only process materials with these ID prefixes
VALID_PREFIXES = ("oqmd-", "mp-", "agm")

# Conservative default due to high memory usage per CHGCAR (~hundreds of MB)
DEFAULT_MAX_WORKERS = 4


class LeMatRhoFetcher(BaseFetcher):
    """Fetcher for LeMatRho charge density data from an authenticated S3 bucket.

    Downloads CHGCAR/AECCAR files, compresses charge densities via pyrho,
    and stores compressed grids in the raw_structures PostgreSQL table.

    Parameters
    ----------
    config : FetcherConfig, optional
        Configuration for the fetcher. If None, loads from default location.
    debug : bool
        If True, process sequentially for debugging.
    """

    def __init__(self, config: FetcherConfig = None, debug: bool = False):
        super().__init__(config or load_fetcher_config(), debug)
        self.aws_client = None
        self.manager = Manager()
        self.manager_dict = self.manager.dict()
        self.manager_dict["occurred"] = False

    def setup_resources(self) -> None:
        """Set up authenticated AWS client and database connection."""
        self.aws_client = get_authenticated_aws_client()
        self.setup_database()

    def get_items_to_process(self) -> ItemsInfo:
        """List material folder prefixes from S3, filtered by valid ID prefixes.

        Returns
        -------
        ItemsInfo
            List of material folder prefixes to process
        """
        bucket = self.config.lematrho_bucket_name
        if not bucket:
            raise ValueError("lematrho_bucket_name must be set in config")

        client = self.aws_client
        paginator = client.get_paginator("list_objects_v2")

        material_folders = []
        for page in paginator.paginate(Bucket=bucket, Delimiter="/"):
            for prefix_info in page.get("CommonPrefixes", []):
                prefix = prefix_info["Prefix"]
                # Extract folder name (remove trailing slash)
                folder_name = prefix.rstrip("/")
                if folder_name.startswith(VALID_PREFIXES):
                    material_folders.append(folder_name)
                else:
                    logger.debug(f"Skipping folder with unknown prefix: {folder_name}")

        logger.info(
            f"Found {len(material_folders)} material folders to process in bucket {bucket}"
        )

        return ItemsInfo(
            start_offset=0,
            total_count=len(material_folders),
            items=material_folders,
        )

    @staticmethod
    def _process_batch(
        batch: Any, config: FetcherConfig, manager_dict: dict, worker_id: int = 0
    ) -> bool:
        """Process a single material folder from S3.

        Downloads vasprun.xml.gz (for structure), then CHGCAR/AECCAR files,
        compresses charge densities, and inserts into PostgreSQL.

        Parameters
        ----------
        batch : str
            Material folder name (e.g. "agm000001")
        config : FetcherConfig
            Configuration object
        manager_dict : dict
            Shared dictionary for inter-process communication
        worker_id : int
            Worker process identifier

        Returns
        -------
        bool
            True if successful, False if failed
        """
        material_id = batch
        bucket = config.lematrho_bucket_name
        grid_shape = config.lematrho_grid_shape or (15, 15, 15)

        try:
            # Fresh clients per worker (multiprocessing safety)
            aws_client = get_authenticated_aws_client()
            db = StructuresDatabase(config.db_conn_str, config.table_name)

            # Step 1: Download and parse vasprun.xml.gz for structure
            vasprun_key = f"{material_id}/{RELAX_CALC_TYPE}/vasprun.xml.gz"
            try:
                vasprun_bytes = download_gz_file_from_s3(
                    aws_client, bucket, vasprun_key
                )
                structure = parse_vasprun_structure(vasprun_bytes)
                del vasprun_bytes
            except Exception as e:
                logger.warning(
                    f"Failed to parse vasprun.xml.gz for {material_id}: {e}"
                )
                return False

            # Step 2: Download and compress charge density files sequentially
            compressed_grids: dict[str, Optional[list]] = {
                "charge_density": None,
                "aeccar0": None,
                "aeccar1": None,
                "aeccar2": None,
            }
            grid_key_map = {
                "CHGCAR.gz": "charge_density",
                "AECCAR0.gz": "aeccar0",
                "AECCAR1.gz": "aeccar1",
                "AECCAR2.gz": "aeccar2",
            }

            for filename in STATIC_FILES:
                s3_key = f"{material_id}/{STATIC_CALC_TYPE}/{filename}"
                grid_name = grid_key_map[filename]
                try:
                    raw_bytes = download_gz_file_from_s3(aws_client, bucket, s3_key)
                    compressed = compress_chgcar(raw_bytes, grid_shape)
                    compressed_grids[grid_name] = compressed
                    del raw_bytes, compressed
                except Exception as e:
                    logger.warning(
                        f"Failed to process {filename} for {material_id}: {e}"
                    )

            # Step 3: Build and insert raw structure
            raw_structure = build_raw_structure(
                material_id=material_id,
                structure=structure,
                compressed_grids=compressed_grids,
                grid_shape=grid_shape,
                s3_prefix=material_id,
            )
            db.insert_data(raw_structure)

            logger.debug(f"Successfully processed {material_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to process material {material_id}: {e}")
            if BaseFetcher.is_critical_error(e):
                if manager_dict is not None:
                    manager_dict["occurred"] = True
            return False

    def cleanup_resources(self) -> None:
        """Clean up AWS client, database connection, and process manager."""
        if self.aws_client:
            self.aws_client = None

        if hasattr(self, "manager"):
            self.manager.shutdown()

        super().cleanup_resources()

    def get_new_version(self) -> str:
        """Get version identifier for this fetch run.

        Returns
        -------
        str
            Current date in YYYY-MM-DD format
        """
        return datetime.now().strftime("%Y-%m-%d")


def fetch():
    """Fetch charge density data from the LeMatRho S3 bucket."""
    fetcher = LeMatRhoFetcher()
    fetcher.fetch()


if __name__ == "__main__":
    fetch()
