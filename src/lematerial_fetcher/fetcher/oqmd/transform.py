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
    offset: int,
    batch_size: int,
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
    offset : int
        The offset to start fetching rows from
    batch_size : int
        The number of rows to fetch
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

        processed_count = 0
        rows = source_db.fetch_items(
            offset=offset, batch_size=batch_size, table_name="structures"
        )

        for raw_structure in rows:
            try:
                structures = transformer.transform_row(
                    raw_structure, source_db=source_db, task_table_name=task_table_name
                )

                for structure in structures:
                    target_db.insert_data(structure)

                processed_count += 1
                if processed_count % config.log_every == 0:
                    logger.info(
                        f"Transformed {batch_id * batch_size + processed_count} records"
                    )

            except Exception as e:
                logger.warning(
                    f"Error processing row oqmd-{raw_structure['id']}: {str(e)}"
                )
                # Check if this is a critical error
                import os
                import pickle

                os.makedirs("errors", exist_ok=True)
                with open(f"errors/error_{raw_structure['id']}.pkl", "wb") as f:
                    pickle.dump(e, f)
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
        offset = self.config.page_offset
        total_processed = 0
        task_table_name = self.config.mp_task_table_name

        if self.debug:
            # Debug mode: process in main process
            while True:
                # Process batch in main process
                process_batch(
                    offset // batch_size,  # batch_id
                    offset,
                    batch_size,
                    task_table_name,
                    self.config,
                    self._database_class,
                    self._structure_class,
                    self.__class__,
                    self.manager_dict,
                )

                # Check if we should continue
                source_db = MySQLDatabase(
                    **self.config.mysql_config,
                )
                rows = source_db.fetch_items(
                    offset=offset + batch_size, batch_size=1, table_name="structures"
                )
                source_db.close()
                if not rows:
                    break

                total_processed += batch_size
                logger.info(f"Total processed: {total_processed}")
                offset += batch_size

            logger.info(f"Completed processing {total_processed} total rows")
            return

        # Normal mode: process in parallel with work stealing
        with ProcessPoolExecutor(max_workers=self.config.num_workers) as executor:
            futures = set()

            # Submit initial batch of tasks
            for i in range(self.config.num_workers):
                future = executor.submit(
                    process_batch,
                    offset // batch_size,  # batch_id
                    offset + (i * batch_size),
                    batch_size,
                    task_table_name,
                    self.config,
                    self._database_class,
                    self._structure_class,
                    self.__class__,
                    self.manager_dict,
                )
                futures.add((offset + (i * batch_size), future))
                total_processed += batch_size

            offset += batch_size * self.config.num_workers
            more_data = True

            while futures and more_data:
                # Check for completed futures and remove them
                done_futures = set()
                for current_offset, future in futures:
                    if future.done():
                        try:
                            future.result()

                            if self.manager_dict.get("occurred", False):
                                logger.critical(
                                    "Critical error detected, shutting down process pool"
                                )
                                executor.shutdown(wait=False)
                                raise RuntimeError(
                                    "Critical error occurred during processing"
                                )

                            # Check if there might be more data
                            source_db = MySQLDatabase(
                                **self.config.mysql_config,
                            )
                            check_rows = source_db.fetch_items(
                                offset=offset, batch_size=1, table_name="structures"
                            )
                            source_db.close()

                            if check_rows:
                                # Submit new task
                                next_future = executor.submit(
                                    process_batch,
                                    offset // batch_size,  # batch_id
                                    offset,
                                    batch_size,
                                    task_table_name,
                                    self.config,
                                    self._database_class,
                                    self._structure_class,
                                    self.__class__,
                                    self.manager_dict,
                                )
                                futures.add((offset, next_future))
                                offset += batch_size
                            else:
                                more_data = False

                            logger.info(
                                f"Successfully processed batch at offset {current_offset}"
                            )

                        except Exception as e:
                            logger.error(f"Critical error encountered: {str(e)}")
                            executor.shutdown(wait=False)
                            raise

                        done_futures.add((current_offset, future))
                        break

                futures -= done_futures

            # Wait for remaining futures
            for current_offset, future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(
                        f"Error processing batch at offset {current_offset}: {str(e)}"
                    )

            logger.info(
                f"Completed processing approximately {total_processed} total rows"
            )

    @property
    def exclude_elements(self) -> list[str]:
        """
        Getter for excluded elements.
        """
        return [
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

    def _get_calculations(
        self,
        raw_structure: RawStructure,
        source_db: MySQLDatabase,
        filter_label: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        """
        Get the calculations for a raw OQMD structure.

        Parameters
        ----------
        raw_structure : RawStructure
            The raw OQMD structure to get the calculations for
        source_db : MySQLDatabase
            The source database to get the calculations from
        filter_label : Optional[list[str]]
            The labels to filter the calculations by

        Returns
        -------
        list[dict[str, Any]]
            The calculations for the raw OQMD structure
        """
        custom_query = (
            f"SELECT * FROM calculations WHERE entry_id = {raw_structure['entry_id']}"
        )
        calculations = source_db.fetch_items(query=custom_query)

        if filter_label:
            calculations = [
                calc for calc in calculations if calc["label"] in filter_label
            ]

        return calculations

    def _get_atoms_from_structure_id(
        self, structure_id: int, source_db: MySQLDatabase
    ) -> list[dict[str, Any]]:
        """
        Get the atoms from a structure ID.

        Parameters
        ----------
        structure_id : int
            The ID of the structure to get the atoms from
        source_db : MySQLDatabase
            The source database to get the atoms from

        Returns
        -------
        list[dict[str, Any]]
            The atoms from the structure ID
        """

        atoms = source_db.fetch_items(
            query=f"SELECT * FROM atoms WHERE structure_id = {structure_id}"
        )

        return atoms

    def _extract_atoms_attributes(
        self, atoms: list[dict[str, Any]]
    ) -> tuple[list[str], list[list[float]], list[list[float]], list[float]]:
        """
        Extract the attributes of the atoms from the atoms table.

        Parameters
        ----------
        atoms : list[dict[str, Any]]
            The atoms to extract the attributes from

        Returns
        -------
        species_at_sites : list[str]
            The species at sites
        frac_coords : list[list[float]]
            The fractional coordinates of the atoms
        forces : list[list[float]]
            The forces on the atoms
        charges : list[float]
            The charges on the atoms
        """

        species_at_sites, frac_coords, forces, charges = [], [], [], []
        for atom in atoms:
            species_at_sites.append(atom["element_id"])
            frac_coords.append([atom["x"], atom["y"], atom["z"]])
            forces.append([atom["fx"], atom["fy"], atom["fz"]])
            charges.append(atom["charge"])

        return species_at_sites, frac_coords, None, charges

    def _extract_structures_attributes(
        self, raw_structure: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Extract the base attributes of a raw OQMD structure from the structures table.

        Parameters
        ----------
        raw_structure : dict[str, Any]
            The raw OQMD structure to extract the attributes from

        Returns
        -------
        dict[str, Any]
            The base attributes of the raw OQMD structure
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

        return values_dict


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

        values_dict = self._extract_structures_attributes(raw_structure)

        calculations = self._get_calculations(
            raw_structure, source_db, filter_label=["static"]
        )
        if len(calculations) == 0:
            logger.warning(f"No static calculation found for {raw_structure['id']}")
            return []
        static_calculation = calculations[0]

        values_dict["energy"] = static_calculation["energy_pa"] * values_dict["nsites"]
        # TODO(msiron): Agree on band gap
        # values_dict["band_gap_indirect"] = static_calculation["band_gap"]

        atoms = self._get_atoms_from_structure_id(raw_structure["id"], source_db)
        species_at_sites, frac_coords, forces, charges = self._extract_atoms_attributes(
            atoms
        )
        structure = Structure(
            species=species_at_sites,
            coords=frac_coords,
            lattice=values_dict["lattice_vectors"],
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

        values_dict["functional"] = Functional.PBE
        # TODO(Ramlaoui): Check that it's only PBE in OQMD
        # Compatibility of the DFT settings
        # dict from string to dict
        settings = ast.literal_eval(static_calculation["settings"])
        if settings["ispin"] in ["1", 1]:
            values_dict["cross_compatibility"] = True
        else:
            values_dict["cross_compatibility"] = False

        if any(element in values_dict["elements"] for element in self.exclude_elements):
            logger.warning(
                f"Skipping oqmd-{raw_structure['id']} because it contains excluded elements"
            )
            return []

        optimade_structure = OptimadeStructure(
            **values_dict,
            id=values_dict["immutable_id"],
            source="oqmd",
            # Couldn't find a way to get the last modified date from the source database
            last_modified=datetime.now().isoformat(),
            functional=Functional.PBE,
        )

        return [optimade_structure]


class OQMDTrajectoryTransformer(BaseOQMDTransformer):
    """
    OQMD trajectory transformer implementation.
    Transforms raw OQMD data into Trajectory objects.
    """

    def _get_structure_from_structure_id(
        self, structure_id: int, source_db: MySQLDatabase
    ) -> dict[str, Any]:
        """
        Get a structure from a structure ID.
        """
        query = f"SELECT * FROM structures WHERE id = {structure_id}"
        raw_structure = source_db.fetch_items(query=query)[0]
        return raw_structure

    def transform_row(
        self,
        raw_structure: RawStructure | dict[str, Any],
        source_db: Optional[StructuresDatabase] = None,
        task_table_name: Optional[str] = None,
    ) -> list[OptimadeStructure]:
        raise NotImplementedError("OQMD trajectory transformation not implemented")
        calculations = self._get_calculations(
            raw_structure,
            source_db,
            filter_label=[
                "coarse_relax",
                "relaxation",
                "fine_relax",
            ],
        )

        for calculation in calculations:
            input_atoms = self._get_atoms_from_structure_id(
                calculation["input_id"], source_db
            )
            input_structure = self._get_structure_from_structure_id(
                input_atoms[0]["structure_id"], source_db
            )
            input_values_dict = self._extract_structures_attributes(input_structure)
            input_atoms_attributes = self._extract_atoms_attributes(input_atoms)
            input_values_dict = {
                **input_values_dict,
                "species_at_sites": input_atoms_attributes[0],
                "cartesian_site_positions": input_atoms_attributes[1],
                "forces": input_atoms_attributes[2],
                "charges": input_atoms_attributes[3],
            }
            breakpoint()

            output_atoms = self._get_atoms_from_structure_id(
                calculation["output_id"], source_db
            )
            output_structure = self._get_structure_from_structure_id(
                output_atoms[0]["structure_id"], source_db
            )

            output_values_dict = self._extract_structures_attributes(output_structure)

            return output_values_dict

        pass
