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
    extract_static_structure_optimization_tasks,
    map_tasks_to_functionals,
)
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import Functional, OptimadeStructure
from lematerial_fetcher.models.trajectories import (
    Trajectory,
    close_to_primary_task,
    has_trajectory_converged,
)
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

        chemical_formula_reduced_dict = raw_structure.attributes["composition_reduced"]
        chemical_formula_reduced_elements = list(chemical_formula_reduced_dict.keys())
        chemical_formula_reduced_ratios = list(chemical_formula_reduced_dict.values())
        chemical_formula_reduced_ratios = [
            str(int(ratio)) if ratio != 1 else ""
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
            # chemistry
            "chemical_formula_anonymous": raw_structure.attributes["formula_anonymous"],
            "chemical_formula_descriptive": str(pmg_structure.composition),
            "chemical_formula_reduced": chemical_formula_reduced,
            "species": species,
            # dimensionality
            "dimension_types": [1, 1, 1],
            "nperiodic_dimensions": 3,
        }

    def _get_calc_targets(self, calc_output: dict[str, Any]) -> dict[str, Any]:
        """
        Get the targets of a calculation. These are extracted from a task and are then
        either associated to a material or a trajectory.

        These targets include:
        - cartesian_site_positions
        - species_at_sites
        - nsites
        - energy
        - forces
        - stress tensor
        - magnetic moments
        - total magnetization

        Parameters
        ----------
        calc_output : dict[str, Any]
            The output of an MP task calculation.
            (task -> output)
        composition_reduced : dict[str, float]
            The composition of the material in reduced form.

        Returns
        -------
        dict[str, Any]
            The targets of the calculation.
        """

        targets = {}

        pmg_structure = Structure.from_dict(calc_output["structure"])
        targets["lattice_vectors"] = pmg_structure.lattice.matrix.tolist()
        targets["cartesian_site_positions"] = pmg_structure.cart_coords.tolist()
        # For some calculations, the unit cell contains less species than other for the same material ID
        # So we need to determine them from the output structure of the calculation.
        targets["species_at_sites"] = [str(site.specie) for site in pmg_structure.sites]
        targets["nsites"] = len(targets["species_at_sites"])

        targets["energy"] = calc_output["energy"]

        try:
            targets["magnetic_moments"] = [
                site["properties"]["magmom"]
                for site in calc_output["structure"]["sites"]
            ]
        except (TypeError, KeyError):
            targets["magnetic_moments"] = None

        targets["forces"] = calc_output["forces"]
        targets["band_gap_indirect"] = calc_output["bandgap"]
        # MP Charges are stored in an external file
        targets["charges"] = None

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

        return targets

    def _get_cross_compatibility_from_composition(
        self, composition_reduced: dict[str, float]
    ) -> bool:
        """
        Get the cross-compatibility of a material from its composition.
        This is based on the fact that some elements are not cross-compatible
        because of the pseudopotential used with the rest of the database.

        Parameters
        ----------
        composition_reduced : dict[str, float]
            The composition of the material in reduced form.

        Returns
        -------
        bool
            True if the material is cross-compatible, False otherwise.
        """

        cross_compatible = True
        non_compatible_elements = ["V", "Cs"]
        # NB: We keep Yb for Materials Project since Yb_3 is now used
        for element in non_compatible_elements:
            if element in composition_reduced.keys():
                cross_compatible = False

        return cross_compatible

    def _get_ionic_step_targets(
        self, ionic_step: dict[str, Any], NELM: int
    ) -> dict[str, Any]:
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
        NELM : int
            The number of electronic steps as parameter of the task.
            This is used to determine if the ionic step is converged.

        Returns
        -------
        dict[str, Any]
            The targets of the ionic step.
        """
        targets = {}
        targets["forces"] = ionic_step["forces"]
        targets["stress_tensor"] = ionic_step["stress"]
        targets["energy"] = ionic_step["e_fr_energy"]

        pmg_structure = Structure.from_dict(ionic_step["structure"])
        targets["lattice_vectors"] = pmg_structure.lattice.matrix.tolist()
        targets["cartesian_site_positions"] = pmg_structure.cart_coords.tolist()
        targets["species_at_sites"] = [str(site.specie) for site in pmg_structure.sites]
        targets["nsites"] = len(targets["species_at_sites"])

        if NELM is not None and len(ionic_step["electronic_steps"]) == NELM:
            raise ValueError(
                f"Ionic step has {len(ionic_step['electronic_steps'])} electronic steps, expected {NELM}"
            )

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
            targets = self._get_calc_targets(task.attributes["output"])
        except KeyError as e:
            logger.warning(
                f"Error getting targets for {material_id} with functional {functional}: {e}"
            )
            return {}

        last_ionic_step = task.attributes["calcs_reversed"][-1]["output"][
            "ionic_steps"
        ][-1]
        NELM = task.attributes["input"]["parameters"]["NELM"]
        if len(last_ionic_step["electronic_steps"]) == NELM:
            raise ValueError(
                f"Last ionic step has {len(last_ionic_step['electronic_steps'])} electronic steps, expected {NELM}"
            )

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
        tasks, calc_types = extract_static_structure_optimization_tasks(
            raw_structure, source_db, task_table_name
        )
        functionals = map_tasks_to_functionals(
            tasks, calc_types, keep_all_calculations=False
        )

        if not functionals:
            return []

        targets_functionals = {
            functional: self._get_task_targets(task, raw_structure.id, functional)
            for functional, task in functionals.items()
        }

        cross_compatibility = self._get_cross_compatibility_from_composition(
            raw_structure.attributes["composition_reduced"]
        )

        input_structure_fields = self._transform_structure(
            raw_structure, raw_structure.attributes["structure"]
        )

        optimade_structures = []
        for functional in functionals.keys():
            targets = targets_functionals[functional]
            optimade_structure = OptimadeStructure(
                id=f"{raw_structure.attributes['material_id']}-{functional.value}",
                source="mp",
                # Basic fields
                immutable_id=raw_structure.attributes["material_id"],
                **input_structure_fields,
                # misc
                last_modified=raw_structure.attributes["builder_meta"]["build_date"][
                    "$date"
                ],
                functional=functional,
                cross_compatibility=cross_compatibility,
                # targets
                **targets,
                compute_space_group=True,
                compute_bawl_hash=True,
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
        self,
        task: RawStructure,
        functional: Functional,
        material_id: str,
        trajectory_number: int = 0,
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
        trajectory_number : int
            The number of the trajectory to use for the transformation.

        Returns
        -------
        list[Trajectory]
            The transformed Trajectory objects.
        """

        trajectories = []

        relaxation_step = 0
        energy_correction = None
        for i, calc in enumerate(task.attributes["calcs_reversed"]):
            # TODO(ramlaoui): What about this input?
            # input_structure_fields = self._transform_structure(raw_structure, calc["input"]["structure"])

            # ionic steps are stored in normal order (first step first)
            NELM = task.attributes["input"].get("parameters", {}).get("NELM", None)
            for ionic_step in calc["output"]["ionic_steps"]:
                input_structure_fields = self._transform_structure(
                    task, ionic_step["structure"]
                )
                output_targets = self._get_ionic_step_targets(ionic_step, NELM)

                cross_compatibility = self._get_cross_compatibility_from_composition(
                    task.attributes["composition_reduced"]
                )

                trajectory = Trajectory(
                    # For one material_id, there can be multiple trajectories even for the same functional
                    # So we need to add a number to the trajectory id to differentiate them
                    id=f"{material_id}-{trajectory_number}-{functional.value}-{relaxation_step}",
                    source="mp",
                    immutable_id=f"{material_id}-{trajectory_number}",
                    **input_structure_fields,
                    **output_targets,
                    functional=functional,
                    last_modified=task.attributes["last_updated"]["$date"],
                    relaxation_step=relaxation_step,
                    relaxation_number=i,
                    cross_compatibility=cross_compatibility,
                    energy_corrected=(
                        output_targets["energy"] + energy_correction
                        if output_targets["energy"] is not None
                        and energy_correction is not None
                        else None
                    ),
                )
                # avoid having to recompute the energy correction
                # for every snapshot of the trajectory
                energy_correction = (
                    trajectory.energy_corrected - trajectory.energy
                    if trajectory.energy is not None
                    and trajectory.energy_corrected is not None
                    else None
                )

                trajectories.append(trajectory)
                relaxation_step += 1

        trajectories = has_trajectory_converged(trajectories)

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

        tasks, calc_types = extract_static_structure_optimization_tasks(
            raw_structure, source_db, task_table_name, extract_static=False
        )
        functionals = map_tasks_to_functionals(
            tasks, calc_types, keep_all_calculations=True
        )

        # Only keep tasks with a BY-C license
        license = raw_structure.attributes["builder_meta"]["license"]
        if license != "BY-C":
            logger.warning(
                f"Material {raw_structure.id} has a license of {license}, skipping"
            )
            return []

        if not functionals:
            logger.warning(
                f"Material {raw_structure.id} has no task found in your tasks database, will be skipped"
            )
            return []

        trajectories = []
        for functional, tasks_list in functionals.items():
            all_functional_trajectories = [
                self.transform_tasks(
                    task, functional, raw_structure.id, trajectory_number
                )
                for trajectory_number, task in enumerate(tasks_list)
            ]
            if len(all_functional_trajectories) == 0:
                continue

            for trajectory in all_functional_trajectories[1:]:
                if close_to_primary_task(all_functional_trajectories[0], trajectory):
                    trajectories.extend(trajectory)

        return trajectories
