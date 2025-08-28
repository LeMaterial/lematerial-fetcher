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
    data_to_row,
    get_concatenated_df,
    upload_pkl_to_huggingface_dataset,
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
        logger.info("Setting up OC20 resources (downloading mapping if needed)")

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
        mapping_pickle_path = "/home/amandine_rossello_entalpic_ai/lematerial-fetcher/src/lematerial_fetcher/fetcher/oc20/oc20_data_mapping.pkl"

        mapping = pickle.load(open(mapping_pickle_path, "rb"))
        mapping = pd.DataFrame(mapping).T
        mapping = mapping.reset_index().rename(columns={"index": "join_key"})

        rows = []
        with env.begin() as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                item = pickle.loads(value)
                data = convert_pyg_data(item)
                row_dict = data_to_row(data)
                row_dict["join_key"] = "random" + str(row_dict["sid"])
                rows.append(row_dict)

        oc20_df = pd.DataFrame(rows)
        merged = oc20_df.merge(mapping, on="join_key", how="left")
        
        output_path = Path(config.output_dir) / f"reactions_{batch}.pkl"
        merged.to_pickle(output_path)

        logger.info(f"Worker {worker_id} wrote {len(merged)} entries to {output_path}")
        return True

    def formatting(self) -> pd.DataFrame:
        """
        Reads and concatenates all .pkl DataFrames
        from reactions directory, creating the all reactions dataset.
        Filters on adsorption reactions criterias
        and creates an adsorption reactions specific dataset.
        Uploads both datasets on Hugging Face.

        """
        output_dir = self.config.output_dir

        combined_df = get_concatenated_df(output_dir)

        combined_df_output_path = (
            Path(output_dir) / "concatenated_reactions_dataset.pkl"
        )
        combined_df.to_pickle(combined_df_output_path)

        logger.info("Saved all reactions DataFrame")

        # upload_pkl_to_huggingface_dataset(
        #     pkl_path=combined_df_output_path,
        #     dataset_name="Entalpic/Catalysis_Hub_all_reactions_dataset",
        # )

        # logger.info("Uploaded Catalysis_Hub_all_reactions_dataset on HF")

        return len(combined_df)

    def cleanup_resources(self) -> None:
        """Clean up resources."""
        self.formatting()
        logger.info("Cleaning up Catalysis-Hub fetcher resources")
        super().cleanup_resources()

    def get_new_version(self) -> str:
        """Get a new version string."""
        return "catalysishub_v1"


if __name__ == "__main__":
    fetcher = OC20Fetcher()
    items_info = fetcher.get_items_to_process()
    print(f"Found {items_info.total_count} files")
