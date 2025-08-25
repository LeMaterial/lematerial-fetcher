# Copyright 2025 Entalpic
from multiprocessing import Manager
import os

from lematerial_fetcher.fetch import BaseFetcher, BatchInfo, ItemsInfo
from lematerial_fetcher.utils.config import FetcherConfig, load_fetcher_config
from lematerial_fetcher.utils.logging import logger
from pathlib import Path
import pandas as pd

import numpy as np
from lematerial_fetcher.fetcher.oc20.utils import (
    download_and_extract,
    uncompress_dir,
    uncompress_xz,
    convert_pyg_data,
)
import lmdb
import pickle


class OC20Fetcher(BaseFetcher):
    """Fetcher for the OC20 database."""

    def __init__(self, config: FetcherConfig = None, debug: bool = False):
        """Initialize the fetcher."""
        super().__init__(config or load_fetcher_config(), debug)
        self.manager = Manager()
        self.manager_dict = self.manager.dict()
        self.manager_dict["occurred"] = False

    def setup_resources(self) -> None:
        logger.info("No external resources to set up for oc20.")

    def get_items_to_process(self) -> ItemsInfo:
        # path = Path(download_and_extract(target_dir=self.config.output_dir))
        # path = uncompress_dir(path, recursive=True, num_workers=self.config.num_workers)
        path = Path(self.config.output_dir)
        lmdb_files = list(path.glob("**/*.lmdb"))
        start_offset = self.config.page_offset
        return ItemsInfo(start_offset, items=lmdb_files, total_count=len(lmdb_files))

    @staticmethod
    def _process_batch(
        batch: BatchInfo, config: FetcherConfig, manager_dict: dict, worker_id: int = 0
    ) -> bool:
        env = lmdb.open(
            str(batch.resolve()),
            subdir=False,
            readonly=True,
            lock=False,
            readahead=True,
            meminit=False,
            max_readers=1,
        )
        env_length = env.stat()["entries"]
        with env.begin() as txn:
            cursor = txn.cursor()
            keys = [key for key, _ in cursor]
            for key in keys:
                item = txn.get(key)
                item = pickle.loads(item)
                data = convert_pyg_data(item)

        logger.info(data)
        return True

    def get_concatenated_df(self) -> pd.DataFrame:
        """
        Reads and concatenates all .pkl DataFrames from a directory.

        Parameters
        ----------
        directory : str
            Path to the directory containing .pkl files.
        save_path : str, optional
            If provided, saves the combined DataFrame to this path.

        Returns
        -------
        pd.DataFrame
            The concatenated DataFrame.
        """
        all_dfs = []
        output_dir = self.config.output_dir

        for fname in os.listdir(output_dir):
            if fname.endswith(".pkl") and "concatenated" not in fname:
                full_path = os.path.join(output_dir, fname)
                try:
                    df = pd.read_pickle(full_path)
                    all_dfs.append(df)
                except Exception as e:
                    print(f"Failed to read {fname}: {e}")

        if not all_dfs:
            logger.info("No valid .pkl files found.")
            return pd.DataFrame()

        combined_df = pd.concat(all_dfs, ignore_index=True)
        print(f"Total number of reactions: {len(combined_df)}")

        output_path = Path(output_dir) / "concatenated_reactions_dataset.pkl"
        combined_df.to_pickle(output_path)
        logger.info("Saved combined DataFrame")

        return combined_df

    def cleanup_resources(self) -> None:
        """Clean up resources."""
        self.get_concatenated_df()
        logger.info("Cleaning up Catalysis-Hub fetcher resources")
        super().cleanup_resources()

    def get_new_version(self) -> str:
        """Get a new version string."""
        return "catalysishub_v1"


if __name__ == "__main__":
    fetcher = OC20Fetcher()
    items_info = fetcher.get_items_to_process()
    print(f"Found {items_info.total_count} files")
