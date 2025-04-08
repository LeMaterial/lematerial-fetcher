# Copyright 2025 Entalpic
import json
import os
import shutil
import tempfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import psycopg2
from datasets import Dataset, Features, Sequence, Value, load_dataset

from lematerial_fetcher.models.optimade import Functional
from lematerial_fetcher.utils.config import PushConfig
from lematerial_fetcher.utils.logging import get_cache_dir, logger


class Push:
    """
    Push data to a HuggingFace Repository.

    Attributes
    ----------
    config : PushConfig
        Configuration for the push operation
        It must contain the following fields:
        - hf_repo_id: str
            The ID of the HuggingFace Repository to push the data to
        - source_db_conn_str: str
            The connection string to the source database. Example:
            postgresql://user:password@host:port/database
        - source_table_name: str
            The name of the table to push the data from
        - chunk_size: int, default=1000
            The number of rows to process in each chunk
        - data_dir: str, default=$HOME/.cache/lematerial_fetcher/push/database/table_name
            The directory to store the data
        - max_rows: int, default=-1
            The maximum number of rows to push. If -1, all rows will be pushed.
        - force_refresh: bool, default=False
            If True, will clear existing cache before downloading
    data_type : str, default="optimade"
        The type of data to push. Can be any of ['optimade', 'trajectories', 'any'].
        If 'any', the data will be pushed as is without any type or column enforcement.
    debug : bool, default=False
        Whether to print debug information
    **kwargs: dict
        Additional keyword arguments to pass to the push_to_hub operation
    """

    def __init__(
        self,
        config: PushConfig,
        data_type: str = "optimade",
        debug: bool = False,
        **kwargs,
    ):
        self.config = config
        self.data_type = data_type

        assert self.data_type in ["optimade", "trajectories", "any"], (
            f"Invalid data type: {self.data_type}, "
            "must be one of ['optimade', 'trajectories', 'any']"
        )
        if self.data_type == "optimade":
            self.features, self.convert_features_dict = self._get_optimade_features()
        elif self.data_type == "trajectories":
            self.features, self.convert_features_dict = (
                self._get_trajectories_features()
            )
        elif self.data_type == "any":
            self.features = None
            self.convert_features_dict = None

        self.debug = debug
        self.conn_str = self.config.source_db_conn_str
        self.max_rows = self.config.max_rows

        if self.config.data_dir is None:
            self.data_dir = get_cache_dir() / f"push/{self.config.source_table_name}"
        else:
            self.data_dir = Path(self.config.data_dir)
        # if self.max_rows is not None and self.max_rows != -1:
        #     # This replaces the data_dir with a temporary directory
        #     self.use_temp_cache()
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.force_refresh = self.config.force_refresh

        self.push_kwargs = kwargs
        if "private" not in self.push_kwargs:
            self.push_kwargs["private"] = True

    @property
    def columns(self) -> list[str]:
        if self.data_type == "any":
            return None
        return list(self.features.keys())

    def _get_optimade_features(self) -> Features:
        """Get the features with the correct types for the optimade data.
        This returns a Features object that can be used to create a HuggingFace dataset.

        Returns
        -------
        Features: The features for the optimade data
        """
        features = Features(
            {
                "elements": Sequence(Value("string")),
                "nsites": Value("int32"),
                "chemical_formula_anonymous": Value("string"),
                "chemical_formula_reduced": Value("string"),
                "chemical_formula_descriptive": Value("string"),
                "nelements": Value("int8"),
                "dimension_types": Sequence(Value("int8")),
                "nperiodic_dimensions": Value("int8"),
                "lattice_vectors": Sequence(Sequence(Value("float64"))),
                "immutable_id": Value("string"),
                "cartesian_site_positions": Sequence(Sequence(Value("float64"))),
                "species": Value("string"),
                "species_at_sites": Sequence(Value("string")),
                "last_modified": Value("string"),
                "elements_ratios": Sequence(Value("float64")),
                "stress_tensor": Sequence(Sequence(Value("float64"))),
                "energy": Value("float64"),
                "magnetic_moments": Sequence(Value("float64")),
                "forces": Sequence(Sequence(Value("float64"))),
                "total_magnetization": Value("float64"),
                "dos_ef": Value("float64"),
                "functional": Value("string"),
                "cross_compatibility": Value("bool"),
                # "entalpic_fingerprint": Value("string"), # TODO(Ramlaoui): Add this back in later
            }
        )

        # Set convert features dict to json for all fields
        convert_features_dict = {}
        for key in features.keys():
            convert_features_dict[key] = "json"

        convert_features_dict["cross_compatibility"] = "bool"

        return features, convert_features_dict

    def _get_trajectories_features(self) -> Features:
        """Get the features with the correct types for the trajectories data.
        This returns a Features object that can be used to create a HuggingFace dataset.

        Returns
        -------
        Features: The features for the trajectories data
        """
        features, convert_features_dict = self._get_optimade_features()

        features.update(
            {
                "relaxation_step": Value("int32"),
                "relaxation_number": (Value("int32")),
            }
        )
        # We do not have magnetic moments, total magnetization, and dos_ef in trajectories
        del features["magnetic_moments"]
        del features["dos_ef"]
        del features["total_magnetization"]

        convert_features_dict.update(
            {
                "relaxation_step": "int",
                "relaxation_number": "int",
            }
        )

        return features, convert_features_dict

    def push(self):
        """
        Entry point for the push operation.
        This function will download the database as CSV files and push them to the HuggingFace Repository.

        Parameters
        ----------
        hf_repo_id : str
            The ID of the HuggingFace Repository to push the data to
        """
        datasets = self.download_all_functionals()

        for key, dataset in datasets.items():
            dataset.push_to_hub(
                self.config.hf_repo_id,
                key,
                token=self.config.hf_token,
                **self.push_kwargs,
            )

    def clear_cache(self) -> None:
        """
        Clear the cache directory.
        Useful for CI/CD pipelines or to force a fresh download.
        """
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)
            self.data_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Cleared cache directory: {self.data_dir}")

    def use_temp_cache(self) -> None:
        """
        Use a temporary directory for caching.
        Useful for testing with a small number of rows.
        """
        self.data_dir = Path(tempfile.mkdtemp())
        self.data_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Using temporary cache directory: {self.data_dir}")

    def download_all_functionals(self) -> dict[str, Dataset]:
        """
        Download all functionals from the database.

        Returns
        -------
        dict[str, Dataset]: A dictionary of HuggingFace datasets, keyed by name of the split
        (compatible, non-compatible, and the functional name)
        """
        datasets = {}

        if self.force_refresh:
            self.clear_cache()

        output_path = Path(self.data_dir)

        # Cross compatible entries:
        for functional in Functional:
            limit_query = (
                f"WHERE functional = '{functional.value}' AND cross_compatibility = 't'"
            )

            dataset = self.download_db_as_csv(
                limit_query=limit_query,
                data_dir=output_path / f"compatible_{functional.value}",
            )
            if dataset is not None:
                datasets[f"compatible_{functional.value}"] = dataset

        # Non-cross compatible entries:
        limit_query = "WHERE cross_compatibility = 'f'"
        dataset = self.download_db_as_csv(
            limit_query=limit_query,
            data_dir=output_path / "non_compatible",
        )
        if dataset is not None:
            datasets["non_compatible"] = dataset

        return datasets

    def download_db_as_csv(self, limit_query: str, data_dir: Path) -> Dataset | None:
        """
        Downloads the database directly as JSONL files using PostgreSQL COPY command.

        Returns a HuggingFace dataset created from the JSONL files and casted to the correct features.

        Parameters
        ----------
        limit_query : str
            The query to limit the number of rows to download
        data_dir : Path
            The directory to store the JSONL files

        Returns
        -------
        Dataset | None: HuggingFace dataset created from the CSV files or None if no rows are found
        """

        os.makedirs(data_dir, exist_ok=True)

        conn = psycopg2.connect(self.conn_str)
        try:
            # Check if the table is empty
            with conn.cursor(name="server_cursor") as cur:
                query = f"SELECT EXISTS(SELECT 1 FROM {self.config.source_table_name} {limit_query} LIMIT 1);"
                cur.execute(query)
                has_rows = cur.fetchone()[0]

                if not has_rows:
                    return None

            # We use an overestimation of the table size instead of counting all rows
            # Because counting huge databases is really slow

            # Apply max_rows limit if specified
            if self.max_rows is not None and self.max_rows != -1:
                estimated_rows = self.max_rows
            else:
                with conn.cursor(name="server_cursor") as cur:
                    query = f"""
                    SELECT reltuples::bigint 
                    FROM pg_class 
                    WHERE relname = '{self.config.source_table_name}';
                    """
                    cur.execute(query)
                    estimated_rows = cur.fetchone()[0]

                    # This is a really bad way to limit the approximation and is
                    # most likely off
                    if "where" in limit_query.lower():
                        # If there's a WHERE clause, assume it filters out some rows
                        # Use a conservative estimate (e.g., 50% of the total)
                        estimated_rows = max(1, estimated_rows // 2)

            chunk_size = min(self.config.chunk_size, estimated_rows)
            num_chunks = (estimated_rows + chunk_size - 1) // chunk_size

            total_chunks = (
                num_chunks + 1
            )  # Add one more chunk for the last remaining batch
            if self.max_rows is not None and self.max_rows != -1:
                total_chunks -= 1  # Not needed if we have a max_rows limit

            # Will copy all columns if data_type is "any"
            if self.columns is None:
                columns = "*"
            else:
                columns = ", ".join(self.columns)

            # Process chunks in parallel if not in debug mode
            if self.debug:
                for i in range(total_chunks):
                    offset = i * chunk_size
                    self.process_chunk(
                        chunk_index=i,
                        offset=offset,
                        chunk_size=chunk_size,
                        num_chunks=num_chunks,
                        data_dir=data_dir,
                        conn_str=self.conn_str,
                        config=self.config,
                        limit_query=limit_query,
                        columns=columns,
                    )
            else:
                chunk_tasks = [
                    (
                        i,
                        i * chunk_size,
                        chunk_size,
                        num_chunks,
                        data_dir,
                        self.conn_str,
                        self.config,
                        limit_query,
                        columns,
                    )
                    for i in range(total_chunks)
                ]

                with ProcessPoolExecutor(
                    max_workers=self.config.num_workers
                ) as executor:
                    futures = {
                        executor.submit(self.process_chunk, *task): task
                        for task in chunk_tasks
                    }

                    # Process results as they complete
                    for future in futures:
                        try:
                            result = future.result()
                            if not result:
                                logger.warning(
                                    f"Failed to process chunk {futures[future][0]}"
                                )
                        except Exception as e:
                            logger.error(
                                f"Error processing chunk {futures[future][0]}: {str(e)}"
                            )

        finally:
            conn.close()

        # No rows exported
        if len(os.listdir(data_dir)) == 0:
            return None

        return self.load_dataset(data_dir)

    @staticmethod
    def process_chunk(
        chunk_index,
        offset,
        chunk_size,
        num_chunks,
        data_dir,
        conn_str,
        config,
        limit_query,
        columns,
    ):
        chunk_file = data_dir / f"chunk_{chunk_index}.jsonl"

        # Skip if file already exists
        if chunk_file.exists():
            logger.info(f"Skipping chunk {chunk_index} because it already exists")
            return True

        # Create a new connection for this worker
        worker_conn = psycopg2.connect(conn_str)
        try:
            with worker_conn.cursor() as cur:
                # Get the ID at the offset
                query = f"""
                SELECT id 
                FROM {config.source_table_name}
                {limit_query}
                ORDER BY id
                OFFSET {offset}
                LIMIT 1;
                """
                # Force index scan instead of sequential scan
                cur.execute("SET enable_seqscan = off;")
                cur.execute(query)
                result = cur.fetchone()
                id_at_offset = result[0] if result else None

                if id_at_offset is None:
                    return False

                # Build the COPY query
                copy_sql = f"""
                    COPY (
                        SELECT row_to_json(t)
                        FROM (
                            SELECT {columns}
                            FROM {config.source_table_name}
                            {limit_query}
                """

                if "where" in limit_query.lower():
                    copy_sql += f" AND id > '{id_at_offset}'"
                else:
                    copy_sql += f" WHERE id > '{id_at_offset}'"

                # If we're on the last chunk, we need to copy all the remaining rows
                if chunk_index == num_chunks:
                    copy_sql += " ORDER BY id) t) TO STDOUT;"
                else:
                    copy_sql += f" ORDER BY id LIMIT {chunk_size}) t) TO STDOUT;"

                # Execute the COPY command and write to file
                with open(chunk_file, "w") as f:
                    cur.copy_expert(copy_sql, f)

                return True
        except Exception as e:
            logger.error(f"Error processing chunk {chunk_index}: {str(e)}")
            return False
        finally:
            worker_conn.close()

    def load_dataset(self, data_dir: Path) -> Dataset:
        """
        Returns a HuggingFace dataset created from the JSONL files and casted to the correct features.

        Parameters
        ----------
        data_dir : Path
            The directory to store the JSONL files

        Returns
        -------
        Dataset: HuggingFace dataset created from the CSV files
        """

        dataset = load_dataset("json", data_files=str(data_dir / "*.jsonl"))

        if "species" in dataset["train"].column_names:

            def convert_species(batch):
                batch["species"] = [json.dumps(species) for species in batch["species"]]
                return batch

            dataset = dataset.map(
                convert_species,
                batched=True,
                num_proc=self.config.num_workers,
                desc="Converting species column to string",
            )

        for split in dataset.keys():
            dataset[split] = dataset[split].cast(
                features=self.features, num_proc=self.config.num_workers
            )

        return dataset
