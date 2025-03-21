# Copyright 2025 Entalpic
from typing import Any, Optional

import numpy as np
from pymatgen.core import Structure

from lematerial_fetcher.database.postgres import (
    OptimadeDatabase,
    StructuresDatabase,
    TrajectoriesDatabase,
)
from lematerial_fetcher.fetcher.mp.utils import (
    extract_structure_optimization_tasks,
    map_tasks_to_functionals,
)
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import Functional, OptimadeStructure
from lematerial_fetcher.models.trajectories import Trajectory
from lematerial_fetcher.transform import BaseTransformer
from lematerial_fetcher.utils.logging import logger


class BaseMPTransformer:
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

    def _transform_structure(
        self,
        raw_structure: RawStructure,
        mp_structure: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Take a raw Materials Project structure and return a dictionary of fields
        that can be used to construct an OptimadeStructure.

        Parameters
        ----------
        mp_structure : dict[str, Any]
            The raw Materials Project structure to transform.

        Returns
        -------
        dict[str, Any]
            The transformed Materials Project structure.
        """

        pmg_structure = Structure.from_dict(mp_structure)

        # TODO(ramlaoui): This does not handle with disordered structures

        species_at_sites = [str(site.specie) for site in pmg_structure.sites]
        cartesian_site_positions = pmg_structure.cart_coords.tolist()
        lattice_vectors = pmg_structure.lattice.matrix.tolist()

        chemical_formula_reduced_dict = raw_structure.attributes["composition_reduced"]
        chemical_formula_reduced_elements = list(chemical_formula_reduced_dict.keys())
        chemical_formula_reduced_ratios = list(chemical_formula_reduced_dict.values())
        chemical_formula_reduced_ratios = [
            str(ratio) if ratio != 1 else ""
            for ratio in chemical_formula_reduced_ratios
        ]
        chemical_formula_reduced_elements_alphabet_sorted = np.argsort(
            np.array(chemical_formula_reduced_elements)
        )
        chemical_formula_reduced = "".join(
            [
                chemical_formula_reduced_elements[i]
                + str(chemical_formula_reduced_ratios[i])
                for i in chemical_formula_reduced_elements_alphabet_sorted
            ]
        )

        chemical_formula_reduced_ratios = list(chemical_formula_reduced_dict.values())
        element_ratios = [
            chemical_formula_reduced_ratios[i] / sum(chemical_formula_reduced_ratios)
            for i in chemical_formula_reduced_elements_alphabet_sorted
        ]

        species = [
            {
                "mass": None,
                "name": element,
                "attached": None,
                "nattached": None,
                "concentration": [1],
                "original_name": None,
                "chemical_symbols": [element],
            }
            for element in raw_structure.attributes["elements"]
        ]

        return {
            # Structural fields
            "elements": raw_structure.attributes["elements"],
            "nelements": raw_structure.attributes["nelements"],
            "elements_ratios": element_ratios,
            # sites
            "nsites": raw_structure.attributes["nsites"],
            "cartesian_site_positions": cartesian_site_positions,
            "species_at_sites": species_at_sites,
            "species": species,
            # chemistry
            "chemical_formula_anonymous": raw_structure.attributes["formula_anonymous"],
            "chemical_formula_descriptive": raw_structure.attributes["formula_pretty"],
            "chemical_formula_reduced": chemical_formula_reduced,
            # dimensionality
            "dimension_types": [1, 1, 1],
            "nperiodic_dimensions": 3,
            "lattice_vectors": lattice_vectors,
        }

    def _get_calc_targets(
        self, calc_output: dict[str, Any], composition_reduced: dict[str, float]
    ) -> dict[str, Any]:
        """
        Get the targets of a calculation.
        These targets include:
        - energy
        - forces
        - stress tensor
        - magnetic moments
        - total magnetization

        Parameters
        ----------
        calc_output : dict[str, Any]
            The output of an MP task calculation.
        composition_reduced : dict[str, float]
            The composition of the material in reduced form.

        Returns
        -------
        dict[str, Any]
            The targets of the calculation.
        """

        targets = {}
        targets["energy"] = calc_output["energy"]
        try:
            targets["magnetic_moments"] = [
                site["properties"]["magmom"]
                for site in calc_output["structure"]["sites"]
            ]
        except (TypeError, KeyError):
            logger.warning("No magnetic moments")
            targets["magnetic_moments"] = None
        targets["forces"] = calc_output["ionic_steps"][-1]["forces"]
        # TODO(ramlaoui): Check if these are correct
        targets["dos_ef"] = calc_output.get("efermi", None)  # dos_ef
        targets["total_magnetization"] = calc_output.get("magnetization", {}).get(
            "total_magnetization", None
        )
        try:
            targets["stress_tensor"] = calc_output["stress"]
        except KeyError:
            logger.warning("No stress tensor")
            targets["stress_tensor"] = None

        targets["cross_compatible"] = True
        non_compatible_elements = ["V", "Cs"]
        # TODO(msiron): What about Yb?
        for element in non_compatible_elements:
            if element in composition_reduced.keys():
                targets["cross_compatible"] = False

        return targets

    def _get_ionic_step_targets(self, ionic_step: dict[str, Any]) -> dict[str, Any]:
        """
        Get the targets of an ionic step.
        These targets include:
        - forces
        - stress tensor
        - energy

        Parameters
        ----------
        ionic_step : dict[str, Any]
            The ionic step to get the targets from.

        Returns
        -------
        dict[str, Any]
            The targets of the ionic step.
        """
        targets = {}
        targets["forces"] = ionic_step["forces"]
        targets["stress_tensor"] = ionic_step["stress"]
        targets["energy"] = ionic_step["e_fr_energy"]

        return targets

    def _get_task_targets(
        self, task: RawStructure, material_id: str, functional: Functional
    ) -> dict[str, Any]:
        """
        Get the target outputs of a task.
        These outputs include:
        - energy
        - forces
        - stress tensor
        - magnetic moments
        - total magnetization
        - cross-compatibility
        - dos_ef

        Parameters
        ----------
        task : RawStructure
            The task to get the targets from.

        Returns
        -------
        dict[str, Any]
            The target parameters of the task.
        """
        try:
            targets = self._get_calc_targets(
                task.attributes["output"], task.attributes["composition_reduced"]
            )
        except KeyError as e:
            logger.warning(
                f"Error getting targets for {material_id} with functional {functional}: {e}"
            )
            return {}

        return targets


class MPTransformer(
    BaseMPTransformer, BaseTransformer[OptimadeDatabase, OptimadeStructure]
):
    """
    Materials Project transformer implementation.
    Transforms raw Materials Project data into OptimadeStructures.
    """

    def transform_row(
        self,
        raw_structure: RawStructure,
        source_db: StructuresDatabase,
        task_table_name: Optional[str] = None,
    ) -> list[OptimadeStructure]:
        """
        Transform a raw Materials Project structure into OptimadeStructures.

        Parameters
        ----------
        raw_structure : RawStructure
            RawStructure object from the dumped database
        source_db : StructuresDatabase
            Source database connection
        task_table_name : Optional[str]
            Task table name to read targets or trajectories from

        Returns
        -------
        list[OptimadeStructure]
            The transformed OptimadeStructure objects.
            If the list is empty, nothing from the structure should be included in the database.
        """
        tasks, calc_types = extract_structure_optimization_tasks(
            raw_structure, source_db, task_table_name
        )
        functionals = map_tasks_to_functionals(tasks, calc_types)

        if not functionals:
            return []

        targets_functionals = {
            functional: self._get_task_targets(task, raw_structure.id, functional)
            for functional, task in functionals.items()
        }

        input_structure_fields = self._transform_structure(
            raw_structure.attributes["structure"]
        )

        optimade_structures = []
        for functional in functionals.keys():
            targets = targets_functionals[functional]
            optimade_structure = OptimadeStructure(
                id=f"{raw_structure.attributes['material_id']}-{functional}",
                source="mp",
                # Basic fields
                immutable_id=raw_structure.attributes["material_id"],
                **input_structure_fields,
                # misc
                last_modified=raw_structure.attributes["builder_meta"]["build_date"][
                    "$date"
                ],
                functional=functional,
                # targets
                **targets,
            )

            optimade_structures.append(optimade_structure)

        return optimade_structures


class MPTrajectoryTransformer(
    BaseMPTransformer, BaseTransformer[TrajectoriesDatabase, Trajectory]
):
    """
    Materials Project transformer implementation for trajectories.
    Transforms raw Materials Project data into OptimadeTrajectories.
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

    def transform_tasks(
        self, task: RawStructure, functional: Functional, material_id: str
    ) -> list[Trajectory]:
        """
        Transform a raw Materials Project structure into Trajectory objects.

        Parameters
        ----------
        task : RawStructure
            The task to extract the trajectories from.
        functional : Functional
            The functional to use for the transformation.
        material_id : str
            The material id of the task.

        Returns
        -------
        list[Trajectory]
            The transformed Trajectory objects.
        """

        trajectories = []

        relaxation_step = 0
        for i, calc in enumerate(task.attributes["calcs_reversed"]):
            # TODO(ramlaoui): What about this input?
            # input_structure_fields = self._transform_structure(raw_structure, calc["input"]["structure"])

            # ionic steps are stored in normal order (first step first)
            for ionic_step in calc["output"]["ionic_steps"]:
                input_structure_fields = self._transform_structure(
                    task, ionic_step["structure"]
                )
                output_targets = self._get_ionic_step_targets(ionic_step)

                trajectory = Trajectory(
                    id=f"{material_id}-{functional}-{relaxation_step}",
                    source="mp",
                    immutable_id=material_id,
                    **input_structure_fields,
                    **output_targets,
                    functional=functional,
                    last_modified=task.attributes["last_updated"]["$date"],
                    relaxation_step=relaxation_step,
                    relaxation_number=i,
                )

                trajectories.append(trajectory)
                relaxation_step += 1

        return trajectories

    def transform_row(
        self,
        raw_structure: RawStructure,
        source_db: StructuresDatabase,
        task_table_name: Optional[str] = None,
    ) -> list[Trajectory]:
        """
        Transform a raw Materials Project structure into Trajectory objects.

        Parameters
        ----------
        raw_structure : RawStructure
            RawStructure object from the dumped database.
            In this case, we expect the raw_structure to be an MP task
            which already contains the trajectories.
        source_db : StructuresDatabase
            Source database connection
        task_table_name : Optional[str]
            Task table name to read targets or trajectories from.
            This is not used here since we expect the raw_structure to be an MP task
            which already contains the trajectories.

        Returns
        -------
        list[Trajectory]
            The transformed Trajectory objects.
        """

        tasks, calc_types = extract_structure_optimization_tasks(
            raw_structure, source_db, task_table_name
        )
        functionals = map_tasks_to_functionals(tasks, calc_types)

        # Only keep tasks with a BY-C license
        license = raw_structure.attributes["builder_meta"]["license"]
        if license != "BY-C":
            logger.warning(
                f"Material {raw_structure.id} has a license of {license}, skipping"
            )
            return []

        if not functionals:
            logger.warning(f"Material {raw_structure.id} has no functionals, skipping")
            return []

        trajectories = []
        for functional, task in functionals.items():
            trajectories.extend(
                self.transform_tasks(task, functional, raw_structure.id)
            )

        return trajectories
