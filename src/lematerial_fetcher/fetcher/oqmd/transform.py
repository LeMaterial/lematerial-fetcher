# Copyright 2025 Entalpic
import ast
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from typing import Any, Optional, Type

from pymatgen.core import Structure

from lematerial_fetcher.database.mysql import MySQLDatabase
from lematerial_fetcher.database.postgres import StructuresDatabase
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import Functional, OptimadeStructure
from lematerial_fetcher.transform import BaseTransformer, TDatabase, TStructure
from lematerial_fetcher.utils.config import TransformerConfig
from lematerial_fetcher.utils.logging import logger
from lematerial_fetcher.utils.structure import (
    get_optimade_from_pymatgen,
    stress_matrix_from_voigt_6_stress,
)


def process_batch(
    batch_id: int,
    rows: list[RawStructure],
    task_table_name: Optional[str],
    config: TransformerConfig,
    database_class: Type[TDatabase],
    structure_class: Type[TStructure],
    transformer_class: Type["BaseTransformer[TDatabase, TStructure]"],
    manager_dict: dict,
) -> None:
    """
    Process a batch of rows in a worker process.

    Parameters
    ----------
    batch_id : int
        Identifier for the batch
    rows : list[RawStructure]
        List of raw structures to process
    task_table_name : Optional[str]
        Task table name to read targets or trajectories from.
        This is only used for Materials Project.
    config : TransformerConfig
        Configuration object
    database_class : Type[TDatabase]
        The class to use for the target database
    structure_class : Type[TStructure]
        The class to use for the transformed structures
    transformer_class : Type["BaseTransformer[TDatabase, TStructure]"]
        The transformer class to use for transformation
    manager_dict : dict
        Shared dictionary to signal critical errors across processes
    """
    try:
        # Create new database connections for this process
        source_db = MySQLDatabase(
            **config.mysql_config,
        )
        target_db = database_class(config.dest_db_conn_str, config.dest_table_name)

        # transform the rows into TStructure objects
        transformer = transformer_class(
            config=config,
            database_class=database_class,
            structure_class=structure_class,
        )

        for i, raw_structure in enumerate(rows, 1):
            try:
                structures = transformer.transform_row(
                    raw_structure, source_db=source_db, task_table_name=task_table_name
                )

                for structure in structures:
                    target_db.insert_data(structure)

                if (batch_id * len(rows) + i) % config.log_every == 0:
                    logger.info(f"Transformed {batch_id * len(rows) + i} records")

            except Exception as e:
                logger.warning(f"Error processing {raw_structure['id']} row: {str(e)}")
                # Check if this is a critical error
                if BaseTransformer.is_critical_error(e):
                    manager_dict["occurred"] = True  # shared across processes
                    return

    except Exception as e:
        logger.error(f"Process initialization error: {str(e)}")
        if BaseTransformer.is_critical_error(e):
            manager_dict["occurred"] = True  # shared across processes

    finally:
        source_db.close()
        target_db.close()


class BaseOQMDTransformer(BaseTransformer):
    """
    OQMD transformer implementation.
    Transforms raw OQMD data into OptimadeStructures.
    """

    def get_new_transform_version(self) -> str:
        """
        Get the new transform version based on the latest processed data.

        Returns
        -------
        str
            New transform version in YYYY-MM-DD format
        """
        try:
            with self.target_db.conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT MAX(last_modified::date)::text
                    FROM {self.config.dest_table_name}
                    """
                )
                latest_date = cur.fetchone()[0]
                return (
                    latest_date if latest_date else super().get_new_transform_version()
                )
        except Exception:
            return super().get_new_transform_version()

    def _process_rows(self) -> None:
        """
        Process rows from source database in parallel, transform them, and store in target database.
        Processes rows in batches to avoid memory issues. Uses a work-stealing approach where workers
        can grab new work immediately without waiting for other workers.

        Raises
        ------
        Exception
            If a critical error occurs during processing
        """
        batch_size = self.config.batch_size
        offset = 0
        total_processed = 0
        task_table_name = self.config.mp_task_table_name

        if self.debug:
            # Debug mode: process in main process
            while True:
                source_db = MySQLDatabase(
                    **self.config.mysql_config,
                )
                rows = source_db.fetch_items(
                    offset=offset, batch_size=batch_size, table_name="structures"
                )
                if not rows:
                    break

                total_processed += len(rows)
                logger.info(
                    f"Processing batch of {len(rows)} rows (total processed: {total_processed})"
                )

                # Process batch in main process
                process_batch(
                    offset // batch_size,  # batch_id
                    rows,
                    task_table_name,
                    self.config,
                    self._database_class,
                    self._structure_class,
                    self.__class__,
                    self.manager_dict,
                )

                offset += batch_size

            logger.info(f"Completed processing {total_processed} total rows")
            return

        # Normal mode: process in parallel with work stealing
        with ProcessPoolExecutor(max_workers=self.config.num_workers) as executor:
            futures = set()

            while True:
                # Check for completed futures and remove them
                done, futures = futures, set()
                for future in done:
                    try:
                        future.result()  # Check for any exceptions
                    except Exception as e:
                        logger.error(f"Critical error encountered: {str(e)}")
                        executor.shutdown(wait=False)
                        raise

                # Check for critical errors across processes
                if self.manager_dict.get("occurred", False):
                    logger.critical(
                        "Critical error detected, shutting down process pool"
                    )
                    executor.shutdown(wait=False)
                    raise RuntimeError("Critical error occurred during processing")

                # Get next batch of rows
                source_db = MySQLDatabase(
                    **self.config.mysql_config,
                )
                rows = source_db.fetch_items(
                    offset=offset, batch_size=batch_size, table_name="structures"
                )
                if not rows:
                    # Wait for remaining futures to complete
                    for future in futures:
                        try:
                            future.result()
                        except Exception as e:
                            logger.error(f"Critical error encountered: {str(e)}")
                            executor.shutdown(wait=False)
                            raise
                    break

                total_processed += len(rows)
                logger.info(
                    f"Processing batch of {len(rows)} rows (total processed: {total_processed})"
                )

                # Split the batch into smaller chunks for parallel processing
                chunk_size = max(1, len(rows) // self.config.num_workers)
                chunks = [
                    rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)
                ]

                # Submit new tasks for each chunk
                for chunk in chunks:
                    future = executor.submit(
                        process_batch,
                        offset // batch_size,  # batch_id
                        chunk,
                        task_table_name,
                        self.config,
                        self._database_class,
                        self._structure_class,
                        self.__class__,
                        self.manager_dict,
                    )
                    futures.add(future)

                offset += batch_size

            logger.info(f"Completed processing {total_processed} total rows")

    def _extract_structures_attributes(
        self, raw_structure: RawStructure
    ) -> dict[str, Any]:
        """
        Extract the base attributes of a raw OQMD structure.
        """
        pass


class OQMDTransformer(BaseOQMDTransformer):
    """
    OQMD transformer implementation.
    Transforms raw OQMD data into OptimadeStructures.
    """

    def transform_row(
        self,
        raw_structure: RawStructure | dict[str, Any],
        source_db: Optional[StructuresDatabase] = None,
        task_table_name: Optional[str] = None,
    ) -> list[OptimadeStructure]:
        """
        Transform a raw OQMD structure into OptimadeStructures.

        Parameters
        ----------
        raw_structure : RawStructure
            RawStructure object from the dumped database
        source_db : Optional[StructuresDatabase]
            Source database connection
        task_table_name : Optional[str]
            Task table name to read targets or trajectories from

        Returns
        -------
        list[OptimadeStructure]
            The transformed OptimadeStructure objects.
            If the list is empty, nothing from the structure should be included in the database.
        """

        stress_tensor_keys = ["sxx", "syy", "szz", "syz", "szx", "sxy"]
        stress_tensor = [raw_structure[key] for key in stress_tensor_keys]
        lattice_vectors_keys = [
            ["x1", "y1", "z1"],
            ["x2", "y2", "z2"],
            ["x3", "y3", "z3"],
        ]
        lattice_vectors = [
            [raw_structure[key] for key in keys] for keys in lattice_vectors_keys
        ]

        structure_mapping_keys = {
            "chemical_formula_descriptive": "composition_id",
            "nsites": "nsites",
            "nelements": "ntypes",
            "total_magnetization": "magmom",
        }

        values_dict = {}
        for key, value in structure_mapping_keys.items():
            values_dict[key] = raw_structure[value]
        values_dict["lattice_vectors"] = lattice_vectors
        values_dict["stress_tensor"] = stress_matrix_from_voigt_6_stress(stress_tensor)
        values_dict["immutable_id"] = f"oqmd-{raw_structure['id']}"

        custom_query = (
            f"SELECT * FROM calculations WHERE entry_id = {raw_structure['entry_id']}"
        )
        calculations = source_db.fetch_items(query=custom_query)
        static_calculation = next(
            (calc for calc in calculations if calc["label"] == "static"), None
        )
        if static_calculation is None:
            logger.warning(f"No static calculation found for {raw_structure['id']}")
            return []

        values_dict["energy"] = static_calculation["energy_pa"] * values_dict["nsites"]
        # TODO(msiron): Agree on band gap
        # values_dict["band_gap_indirect"] = static_calculation["band_gap"]

        atoms = source_db.fetch_items(
            query=f"SELECT * FROM atoms WHERE structure_id = {raw_structure['id']}"
        )
        species_at_sites, frac_coords, forces, charges = [], [], [], []
        for atom in atoms:
            species_at_sites.append(atom["element_id"])
            frac_coords.append([atom["x"], atom["y"], atom["z"]])
            forces.append([atom["fx"], atom["fy"], atom["fz"]])
            charges.append(atom["charge"])
        structure = Structure(
            species=species_at_sites,
            coords=frac_coords,
            lattice=lattice_vectors,
            coords_are_cartesian=False,
        )
        cartesian_site_positions = structure.cart_coords
        values_dict["species_at_sites"] = species_at_sites
        values_dict["cartesian_site_positions"] = cartesian_site_positions
        values_dict["forces"] = forces
        values_dict["charges"] = charges

        optimade_keys_from_structure = get_optimade_from_pymatgen(structure)
        keep_cols = [
            "chemical_formula_anonymous",
            "elements",
            "elements_ratios",
            "chemical_formula_reduced",
            "dimension_types",
            "nperiodic_dimensions",
            "species",
        ]
        for key in keep_cols:
            values_dict[key] = optimade_keys_from_structure[key]

        # Compatibility of the DFT settings
        # dict from string to dict
        settings = ast.literal_eval(static_calculation["settings"])
        if settings["ispin"] in ["1", 1]:
            values_dict["functional"] = Functional.PBE
        else:
            values_dict["functional"] = Functional.INCOMPATIBLE
        filter_out_elements = [
            "Yb",
            "W",
            "Tl",
            "Eu",
            "Ce",
            "Rh",
            "Ru",
            "Mo",
            "Mn",
            "Cr",
            "V",
            "Ti",
            "Ca",
        ]
        if any(element in values_dict["elements"] for element in filter_out_elements):
            logger.warning(
                f"Skipping {raw_structure['id']} because it contains filter_out_elements"
            )
            return []

        optimade_structure = OptimadeStructure(
            **values_dict,
            id=values_dict["immutable_id"],
            source="oqmd",
            # Couldn't find a way to get the last modified date from the source database
            last_modified=datetime.now().isoformat(),
        )

        return [optimade_structure]


class OQMDTrajectoryTransformer(BaseOQMDTransformer):
    """
    OQMD trajectory transformer implementation.
    Transforms raw OQMD data into Trajectory objects.
    """

    def transform_row(
        self,
        raw_structure: RawStructure | dict[str, Any],
        source_db: Optional[StructuresDatabase] = None,
        task_table_name: Optional[str] = None,
    ) -> list[OptimadeStructure]:
        pass
