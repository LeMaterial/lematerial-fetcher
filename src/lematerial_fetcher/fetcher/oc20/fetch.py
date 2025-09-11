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
    get_left_out_fields,
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
        # path = Path(self.config.output_dir)
        path = Path("/home/amandine_rossello_entalpic_ai/fetch/oc20_data")
        # lmdb_files = list(path.glob("**/*.lmdb"))
        lmdb_files = [
            f for f in path.glob("**/*.lmdb") if not f.parent.name.startswith("test")
        ]  # dropping test files missing relaxed structures
        logger.info(f"lmdb_files list : {lmdb_files}")
        # lmdb_files = [lmdb_files[0]]
        start_offset = self.config.page_offset
        return ItemsInfo(start_offset, items=lmdb_files, total_count=len(lmdb_files))

    @staticmethod
    def _process_batch(
        batch: BatchInfo, config: FetcherConfig, manager_dict: dict, worker_id: int = 0
    ) -> bool:
        # breakpoint()
        env = lmdb.open(
            str(batch.resolve()),
            subdir=False,
            readonly=True,
            lock=False,
            readahead=True,
            meminit=False,
            max_readers=1,
        )
        oc20_metadata_path = "/home/amandine_rossello_entalpic_ai/lematerial-fetcher/src/lematerial_fetcher/fetcher/oc20/oc20_data_mapping.pkl"
        oc20_metadata = pickle.load(open(oc20_metadata_path, "rb"))
        oc20_metadata = pd.DataFrame(oc20_metadata).T
        oc20_metadata = oc20_metadata.reset_index().rename(
            columns={"index": "join_key"}
        )

        oc20_slab_path = "/home/amandine_rossello_entalpic_ai/lematerial-fetcher/src/lematerial_fetcher/fetcher/oc20/slab_energy.pkl"
        oc20_slab = pickle.load(open(oc20_slab_path, "rb"))
        oc20_slab = pd.DataFrame(oc20_slab)

        rows = []
        with env.begin() as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                item = pickle.loads(value)
                data = convert_pyg_data(item)
                row_dict = data_to_row(data)
                rows.append(row_dict)

        oc20_df = pd.DataFrame(rows)
        logger.info(f"oc20_df number of rows processed : {len(oc20_df)}")

        oc_20_df_metadata = oc20_df.merge(oc20_metadata, on="join_key", how="left")

        oc20_batch = get_left_out_fields(oc_20_df_metadata, oc20_slab)

        relative_parts = Path(batch).parts[-4:]

        last_part = Path(relative_parts[-1]).stem

        safe_name = (
            "reactions_" + "_".join(list(relative_parts[:-1]) + [last_part]) + ".pkl"
        )
        oc20_batch["publication"] = "oc20-" + "_".join(
            list(relative_parts[:-1]) + [last_part]
        )

        output_path = Path(config.output_dir) / safe_name

        oc20_batch.to_pickle(output_path)

        logger.info(
            f"Worker {worker_id} wrote {len(oc20_batch)} entries to {output_path}"
        )
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

        upload_pkl_to_huggingface_dataset(
            pkl_path=combined_df_output_path,
            dataset_name="Entalpic/LeMat-Cat-oc20",
        )

        logger.info("Uploaded LeMat-Cat-oc20 on HF")

        return len(combined_df)

    def cleanup_resources(self) -> None:
        """Clean up resources."""
        self.formatting()
        logger.info("Cleaning up oc20 fetcher resources")
        super().cleanup_resources()

    def get_new_version(self) -> str:
        """Get a new version string."""
        return "oc20-v1"


if __name__ == "__main__":
    fetcher = OC20Fetcher()
    items_info = fetcher.get_items_to_process()
    print(f"Found {items_info.total_count} files")
