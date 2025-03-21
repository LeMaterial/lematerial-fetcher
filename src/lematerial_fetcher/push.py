from pathlib import Path
from urllib.parse import urlparse

import psycopg2
from datasets import Features, Sequence, Value, load_dataset

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
            self.features = self._get_trajectories_features()
        elif self.data_type == "trajectories":
            self.features = self._get_optimade_features()
        elif self.data_type == "any":
            self.features = None

        self.debug = debug

        parsed = urlparse(self.config.source_db_conn_str)
        self.dbname = parsed.path[1:]
        self.user = parsed.username
        self.password = parsed.password
        self.host = parsed.hostname
        self.port = parsed.port or 5432

        if self.config.data_dir is None:
            self.data_dir = (
                get_cache_dir() / f"push/{self.dbname}/{self.config.source_table_name}"
            )
        else:
            self.data_dir = Path(self.config.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

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
                "id": Value("string"),
                "elements": Sequence(Value("string")),
                "nelements": Value("int8"),
                "elements_ratios": Sequence(Value("float64")),
                "nsites": Value("int32"),
                "cartesian_site_positions": Sequence(Sequence(Value("float64"))),
                "lattice_vectors": Sequence(Sequence(Value("float64"))),
                "species_at_sites": Sequence(Value("string")),
                "species": Sequence(Value("string")),
                "chemical_formula_anonymous": Value("string"),
                "chemical_formula_descriptive": Value("string"),
                "chemical_formula_reduced": Value("string"),
                "dimension_types": Sequence(Value("int8")),
                "nperiodic_dimensions": Value("int8"),
                "immutable_id": Value("string"),
                "last_modified": Value("string"),
                "stress_tensor": Sequence(Sequence(Value("float64"))),
                "energy": Value("float64"),
                "magnetic_moments": Sequence(Value("float64")),
                "forces": Sequence(Sequence(Value("float64"))),
                "total_magnetization": Value("float64"),
                "dos_ef": Value("float64"),
                "functional": Value("string"),
                "cross_compatibility": Value("bool"),
                "entalpic_fingerprint": Value("string"),
            }
        )

        return features

    def _get_trajectories_features(self) -> Features:
        """Get the features with the correct types for the trajectories data.
        This returns a Features object that can be used to create a HuggingFace dataset.

        Returns
        -------
        Features: The features for the trajectories data
        """
        features = self._get_optimade_features()

        features.update(
            {
                "relaxation_step": Value("int32"),
                "relaxation_number": Sequence(Value("int32")),
            }
        )
        # del features['total_magnetization']
        # del features['dos_ef']

        return features

    def push(self):
        """
        Entry point for the push operation.
        This function will download the database as CSV files and push them to the HuggingFace Repository.

        Parameters
        ----------
        hf_repo_id : str
            The ID of the HuggingFace Repository to push the data to
        """
        dataset = self.download_db_as_csv()

        dataset.push_to_hub(self.config.hf_repo_id, **self.push_kwargs)

    def download_db_as_csv(self):
        """
        Downloads the database directly as CSV files using PostgreSQL COPY command.

        Returns a HuggingFace dataset created from the CSV files and casted to the correct features.
        The dataset is stored in the data_dir attribute.

        Parameters
        ----------
        output_dir : str
            Directory where CSV files will be stored
        chunk_size : int
            Number of rows to process in each chunk

        Returns
        -------
        Dataset: HuggingFace dataset created from the CSV files
        """
        output_path = Path(self.data_dir)
        chunk_size = self.config.chunk_size

        conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )

        logger.info(
            f"Downloading {self.config.source_table_name} from {self.dbname}@{self.host}:{self.port}"
            f" Will save to {output_path}"
        )

        try:
            with conn.cursor("cursor") as cur:
                cur.execute(f"SELECT COUNT(id) FROM {self.config.source_table_name}")
                total_rows = cur.fetchone()[0]

                num_chunks = (total_rows + chunk_size - 1) // chunk_size

                for i in range(num_chunks):
                    offset = i * chunk_size
                    chunk_file = output_path / f"chunk_{i}.csv"

                    # Will copy all columns if data_type is "any"
                    if self.columns is None:
                        columns = "*"
                    else:
                        columns = ", ".join(self.columns)

                    copy_sql = f"""
                        COPY (
                            SELECT {columns} 
                            FROM {self.config.source_table_name}
                            ORDER BY id
                            LIMIT {chunk_size} OFFSET {offset}
                        ) TO STDOUT WITH CSV HEADER
                        """

                    with open(chunk_file, "w") as f:
                        cur.copy_expert(copy_sql, f)

        finally:
            conn.close()

        # Recast to the correct features if data_type is not "any"
        features = self.features if self.data_type != "any" else None
        dataset = load_dataset(
            "csv", data_files=str(output_path / "*.csv"), features=features
        )

        return dataset
