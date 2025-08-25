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
    parse_reactions,
    parse_reactions_surface,
    parse_reactions_with_roles,
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
        start_offset = self.config.page_offset
        return ItemsInfo(start_offset, items=pub_ids, total_count=len(pub_ids))

    @staticmethod
    def _process_batch(
        batch: BatchInfo, config: FetcherConfig, manager_dict: dict, worker_id: int = 0
    ) -> bool:
        data = parse_reactions_with_roles([batch])
        df = pd.DataFrame(data)
        if len(df) == 0:
            return True
        os.makedirs(config.output_dir, exist_ok=True)
        output_path = Path(config.output_dir) / f"reactions_{batch}.pkl"
        df.to_pickle(output_path)

        logger.info(f"Worker {worker_id} wrote {len(df)} entries to {output_path}")
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
