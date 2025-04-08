# Copyright 2025 Entalpic
import ast
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from typing import Any, Optional, Type

from pymatgen.core import Structure

from lematerial_fetcher.database.mysql import MySQLDatabase
from lematerial_fetcher.database.postgres import (
    OptimadeDatabase,
    StructuresDatabase,
    TrajectoriesDatabase,
)
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import Functional, OptimadeStructure
from lematerial_fetcher.models.trajectories import Trajectory, has_trajectory_converged
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
        if database_class == OptimadeDatabase:
            table_source = "structures"
        elif database_class == TrajectoriesDatabase:
            table_source = "entries"

        rows = source_db.fetch_items(
            offset=offset, batch_size=batch_size, table_name=table_source
        )

        structures = transformer.transform_row(
            rows, source_db=source_db, task_table_name=task_table_name
        )

        target_db.batch_insert_data(structures)

        processed_count += 1
        if processed_count % config.log_every == 0:
            logger.info(
                f"Transformed {batch_id * batch_size + processed_count} records"
            )

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
        table_name = (
            "structures" if self._database_class == OptimadeDatabase else "entries"
        )

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
                    offset=offset + batch_size, batch_size=1, table_name=table_name
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
                            # Check that there is more data
                            check_rows = source_db.fetch_items(
                                offset=offset, batch_size=1, table_name=table_name
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
        raw_structures: list[RawStructure | dict[str, Any]],
        source_db: MySQLDatabase,
        filter_label: Optional[list[str]] = None,
        entry_id_key: str = "entry_id",
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Get the calculations for a raw OQMD structure.

        Parameters
        ----------
        raw_structures : list[RawStructure | dict[str, Any]]
            The raw OQMD structures to get the calculations for
        source_db : MySQLDatabase
            The source database to get the calculations from
        filter_label : Optional[list[str]]
            The labels to filter the calculations by, they are also sorted by the label
            order
        entry_id_key : str
            The key to use to get the entry_id from the raw_structure.
            This is because we might pass either a list of objects from the `structures`
            table of OQMD or from the `entries` table of OQMD.

        Returns
        -------
        dict[str, list[dict[str, Any]]]
            The calculations for the raw OQMD structures for every structure as a
            dictionary id -> list of calculations, sorted, for every id, by the order of labels in
            filter_label if provided
        """
        structure_id_to_entry_id = {
            raw_structure["id"]: raw_structure[entry_id_key]
            for raw_structure in raw_structures
        }
        entry_ids = list(structure_id_to_entry_id.values())
        # Get a list of all the calculations for the entry_ids
        custom_query = f"SELECT * FROM calculations WHERE entry_id IN ({', '.join(map(str, entry_ids))})"
        fetched_calculations = source_db.fetch_items(query=custom_query)

        # We need to group the calculations by entry_id because different structures can have the same entry_id
        calculations_by_entry_id = defaultdict(list)
        for calculation in fetched_calculations:
            calculations_by_entry_id[calculation["entry_id"]].append(calculation)

        # Group the calculations by structure_id
        calculations = defaultdict(list)
        for structure_id in structure_id_to_entry_id.keys():
            calculations[structure_id] = calculations_by_entry_id[
                structure_id_to_entry_id[structure_id]
            ]

            if filter_label:
                # Filter and sort calculations based on label order
                calculations[structure_id] = sorted(
                    [
                        calc
                        for calc in calculations[structure_id]
                        if calc["label"] in filter_label
                    ],
                    key=lambda x: filter_label.index(x["label"]),
                )

        return calculations

    def _get_atoms_from_structure_id(
        self, structure_ids: list[int], source_db: MySQLDatabase
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Get the atoms for a list of structure IDs.

        Parameters
        ----------
        structure_ids : list[int]
            The IDs of the structures to get the atoms from
        source_db : MySQLDatabase
            The source database to get the atoms from

        Returns
        -------
        dict[str, list[dict[str, Any]]]
            The atoms from the structure IDs
        """

        atoms = source_db.fetch_items(
            query=f"SELECT * FROM atoms WHERE structure_id IN ({', '.join(map(str, structure_ids))})"
        )

        atoms_dict = defaultdict(list)
        for atom in atoms:
            atoms_dict[atom["structure_id"]].append(atom)

        return atoms_dict

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

        if any(any(f is None for f in force) for force in forces):
            forces = None

        return species_at_sites, frac_coords, forces, charges

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
        values_dict["energy"] = raw_structure["energy"]  # might be None

        return values_dict


class OQMDTransformer(
    BaseOQMDTransformer, BaseTransformer[OptimadeDatabase, OptimadeStructure]
):
    """
    OQMD transformer implementation.
    Transforms raw OQMD data into OptimadeStructures.
    """

    def transform_row(
        self,
        raw_structures: list[RawStructure | dict[str, Any]],
        source_db: Optional[StructuresDatabase] = None,
        task_table_name: Optional[str] = None,
    ) -> list[OptimadeStructure]:
        """
        Transform a list of raw OQMD structures into OptimadeStructures.
        Contrary to the name of the method, this does not only process a single
        structure but takes a list of structures as input to avoid latency
        with the fetching calls.

        Parameters
        ----------
        raw_structures : list[RawStructure | dict[str, Any]]
            List of RawStructure objects from the dumped database
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

        values_dict_dict = {
            raw_structure["id"]: self._extract_structures_attributes(raw_structure)
            for raw_structure in raw_structures
        }

        calculations_dict = self._get_calculations(
            raw_structures, source_db, filter_label=["static"]
        )  # same order as raw_structures

        atoms = self._get_atoms_from_structure_id(
            list(calculations_dict.keys()), source_db
        )

        optimade_structures = []
        for raw_structure, structure_id in zip(
            raw_structures, calculations_dict.keys()
        ):
            calculations = calculations_dict[structure_id]
            values_dict = values_dict_dict[structure_id]

            if len(calculations) == 0:
                logger.warning(f"No static calculation found for {structure_id}")
                continue
            static_calculation = calculations[0]

            values_dict["energy"] = (
                static_calculation["energy_pa"] * values_dict["nsites"]
            )
            # TODO(msiron): Agree on band gap
            # values_dict["band_gap_indirect"] = static_calculation["band_gap"]

            species_at_sites, frac_coords, forces, charges = (
                self._extract_atoms_attributes(atoms)
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
            # Compatibility of the DFT settings
            # dict from string to dict
            settings = ast.literal_eval(static_calculation["settings"])
            if settings["ispin"] in ["2", 2]:
                values_dict["cross_compatibility"] = True
            else:
                values_dict["cross_compatibility"] = False

            if any(
                element in values_dict["elements"] for element in self.exclude_elements
            ):
                # TODO(Ramlaoui): Do we just want to skip the structure or set cross_compatibility to False?
                values_dict["cross_compatibility"] = False

            optimade_structure = OptimadeStructure(
                **values_dict,
                id=values_dict["immutable_id"],
                source="oqmd",
                # Couldn't find a way to get the last modified date from the source database
                last_modified=datetime.now().isoformat(),
                functional=Functional.PBE,
            )
            optimade_structures.append(optimade_structure)

        return optimade_structures


class OQMDTrajectoryTransformer(
    BaseOQMDTransformer, BaseTransformer[TrajectoriesDatabase, Trajectory]
):
    """
    OQMD trajectory transformer implementation.
    Transforms raw OQMD data into Trajectory objects.
    """

    def __init__(self, *args, **kwargs):
        if "structure_class" in kwargs:
            del kwargs["structure_class"]
        if "database_class" in kwargs:
            del kwargs["database_class"]
        super().__init__(
            *args,
            **kwargs,
            structure_class=Trajectory,
            database_class=TrajectoriesDatabase,
        )

    def _get_structures_from_structure_ids(
        self, structure_ids: list[int], source_db: MySQLDatabase
    ) -> dict[str, dict[str, Any]]:
        """
        Get a structure from a structure ID.
        """
        query = f"SELECT * FROM structures WHERE id IN ({', '.join(map(str, structure_ids))})"
        raw_structures = source_db.fetch_items(query=query)
        return {raw_structure["id"]: raw_structure for raw_structure in raw_structures}

    def transform_row(
        self,
        raw_structures: list[RawStructure | dict[str, Any]],
        source_db: Optional[StructuresDatabase] = None,
        task_table_name: Optional[str] = None,
    ) -> list[Trajectory]:
        """
        Transform a list of raw OQMD structures into Trajectory objects.
        Contrary to the name of the method, this does not only process a single
        structure but takes a list of structures as input to avoid latency
        with the fetching calls.

        Parameters
        ----------
        raw_structures : list[RawStructure | dict[str, Any]]
            List of RawStructure objects from the dumped database
        source_db : Optional[StructuresDatabase]
            Source database connection
        task_table_name : Optional[str]
            Task table name to read targets or trajectories from
            Not used for OQMD.

        Returns
        -------
        list[Trajectory]
            The transformed Trajectory objects.
        """
        calculations_dict = self._get_calculations(
            raw_structures,
            source_db,
            filter_label=[
                "relaxation",
                "coarse_relax",
                "fine_relax",
            ],
            entry_id_key="id",  # We are iterating through entries not structures
        )  # calculations are sorted by the order of labels in filter_label

        def get_values_dict_dict_from_structure_id(
            structure_ids: list[int],
        ) -> dict[str, Any]:
            structures_dict = self._get_structures_from_structure_ids(
                structure_ids, source_db
            )

            values_dict_dict = {
                structure_id: self._extract_structures_attributes(structure)
                for structure_id, structure in structures_dict.items()
            }

            atoms_dict = self._get_atoms_from_structure_id(structure_ids, source_db)

            for structure_id, atoms in atoms_dict.items():
                values_dict = values_dict_dict[structure_id]

                species_at_sites, frac_coords, forces, charges = (
                    self._extract_atoms_attributes(atoms)
                )

                structure = Structure(
                    species=species_at_sites,
                    coords=frac_coords,
                    lattice=values_dict["lattice_vectors"],
                    coords_are_cartesian=False,
                )
                optimade_keys_from_structure = get_optimade_from_pymatgen(structure)

                values_dict = {
                    **values_dict,
                    **optimade_keys_from_structure,
                    "species_at_sites": species_at_sites,
                    "cartesian_site_positions": structure.cart_coords,
                    "forces": forces,
                    "charges": charges,
                }

                values_dict["functional"] = Functional.PBE

                values_dict_dict[structure_id] = values_dict

            return values_dict_dict

        # Get all the structure IDs from the calculations
        # some of them might be None so we need to filter them out to avoid errors
        flattened_structure_ids = [
            structure_id
            for _, calculations in calculations_dict.items()
            for calculation in calculations
            for structure_id in (calculation["input_id"], calculation["output_id"])
            if structure_id is not None
        ]
        entry_id_to_ignore = set(
            [
                entry_id
                for entry_id, calculations in calculations_dict.items()
                for calculation in calculations
                for structure_id in (calculation["input_id"], calculation["output_id"])
                if structure_id is None
            ]
        )
        flattened_structure_ids = [
            structure_id
            for structure_id in flattened_structure_ids
            if structure_id not in entry_id_to_ignore
        ]
        values_dict_dict = get_values_dict_dict_from_structure_id(
            flattened_structure_ids
        )

        trajectories = []

        for entry_id, calculations in calculations_dict.items():
            if len(calculations) == 0:
                logger.warning(f"No calculations found for entry {entry_id}")
                continue

            if entry_id in entry_id_to_ignore:
                continue
            current_relaxation_step, current_relaxation_number = 0, 0

            labels_order = [calculation["label"] for calculation in calculations]
            if len(set(labels_order)) != len(labels_order):
                logger.warning(f"Found {labels_order} for entry {entry_id}, skipping")
                continue

            entry_trajectories = []
            # The id of the trajectory will be the id of the final output structure
            # similar to how it is done with MP where we use the materials_id to name
            # the trajectory.
            trajectory_immutable_id = f"oqmd-{calculations[-1]['output_id']}"
            for calculation in calculations:
                input_values_dict = values_dict_dict[calculation["input_id"]]
                output_values_dict = values_dict_dict[calculation["output_id"]]

                if any(
                    element in input_values_dict["elements"]
                    for element in self.exclude_elements
                ):
                    cross_compatibility = False

                # Compatibility of the DFT settings
                # dict from string to dict
                settings = ast.literal_eval(calculation["settings"])
                if settings.get("ispin", None) in ["2", 2]:
                    cross_compatibility = True
                else:
                    cross_compatibility = False

                # No need to add the input relaxation step if its an intermediary relaxation number
                # because it was already the output of the previous relaxation number
                if current_relaxation_number == 0:
                    input_values_dict["immutable_id"] = trajectory_immutable_id
                    entry_trajectories.append(
                        Trajectory(
                            id=f"{trajectory_immutable_id}-{Functional.PBE}-{current_relaxation_number}",
                            source="oqmd",
                            last_modified=datetime.now().isoformat(),  # not available for OQMD
                            relaxation_number=current_relaxation_number,
                            relaxation_step=current_relaxation_step,
                            cross_compatibility=cross_compatibility,
                            **input_values_dict,
                        )
                    )

                output_relaxation_step = current_relaxation_step + calculation["nsteps"]
                output_values_dict["immutable_id"] = trajectory_immutable_id
                entry_trajectories.append(
                    Trajectory(
                        id=f"{trajectory_immutable_id}-{Functional.PBE}-{output_relaxation_step}",
                        source="oqmd",
                        last_modified=datetime.now().isoformat(),  # not available for OQMD
                        relaxation_number=current_relaxation_number,
                        relaxation_step=output_relaxation_step,
                        cross_compatibility=cross_compatibility,
                        **output_values_dict,
                    )
                )

                # TODO(Ramlaoui): No relaxation sometimes
                current_relaxation_step += calculation["nsteps"]
                current_relaxation_number += 1

            if len(calculations) > 1 and not calculations[-1]["converged"]:
                logger.warning(f"Entry {entry_id} did not converge, skipping")
                continue

            # We only check that the forces in the last step are not small
            # because we don't have all the steps
            if not has_trajectory_converged(entry_trajectories, energy_threshold=None):
                continue

            trajectories.extend(entry_trajectories)

        return trajectories
