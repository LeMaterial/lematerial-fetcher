# Copyright 2025 Entalpic
from multiprocessing import Manager
import os
from lematerial_fetcher.fetch import BaseFetcher, BatchInfo, ItemsInfo
from lematerial_fetcher.utils.config import FetcherConfig, load_fetcher_config
from lematerial_fetcher.utils.logging import logger
from pathlib import Path
from lematerial_fetcher.fetcher.catalysis_hub.utils import (
    fetch_all_pub_ids,
    reactions_from_dataset,
    aseify_reactions,
    adsorption_reactions_dataset,
    parse_reactions_with_roles,
    get_concatenated_df,
    upload_pkl_to_huggingface_dataset,
)
import pandas as pd


class CatalysisHubFetcher(BaseFetcher):
    """Fetcher for the Catalysis Hub database."""

    def __init__(self, config: FetcherConfig = None, debug: bool = False):
        """Initialize the fetcher."""
        super().__init__(config or load_fetcher_config(), debug)
        self.manager = Manager()
        self.manager_dict = self.manager.dict()
        self.manager_dict["occurred"] = False

    def setup_resources(self) -> None:
        logger.info("No external resources to set up for Catalysis Hub.")

    def get_items_to_process(self) -> ItemsInfo:
        pub_ids = fetch_all_pub_ids()
        # logger.info(f"{pub_ids}")
        # breakpoint()
        pub_ids = ["MamunHighT2019", "JungSpin2025"]

        start_offset = self.config.page_offset
        return ItemsInfo(start_offset, items=pub_ids, total_count=len(pub_ids))

    @staticmethod
    def _process_batch(
        batch: BatchInfo, config: FetcherConfig, manager_dict: dict, worker_id: int = 0
    ) -> bool:
        logger.info(f"Treating publication: {batch}")

        data = parse_reactions_with_roles([batch])

        logger.info(f"Parsed {len(data)} reactions for {batch}")

        df = pd.DataFrame(data)
        if len(df) == 0:
            return True
        os.makedirs(config.output_dir, exist_ok=True)
        output_path = Path(config.output_dir) / f"reactions_{batch}.pkl"
        df.to_pickle(output_path)

        logger.info(f"Worker {worker_id} wrote {len(df)} entries to {output_path}")
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

        adsorption_output_path = Path(output_dir) / "adsorption_reactions_dataset.pkl"

        adsorption_df = adsorption_reactions_dataset(
            df_path=combined_df_output_path,
            store_path=adsorption_output_path,
        )
        logger.info("Saved adsorption reactions DataFrame")

        # upload_pkl_to_huggingface_dataset(
        #     pkl_path=adsorption_df,
        #     dataset_name="Entalpic/Catalysis_Hub_adsorption_reactions_dataset",
        # )

        # logger.info("Uploaded Catalysis_Hub_adsorption_reactions_dataset on HF")

        return len(combined_df), len(adsorption_df)

    def cleanup_resources(self) -> None:
        """Clean up resources."""
        self.formatting()
        logger.info("Cleaning up Catalysis-Hub fetcher resources")
        super().cleanup_resources()

    def get_new_version(self) -> str:
        """Get a new version string."""
        return "catalysishub_v1"
