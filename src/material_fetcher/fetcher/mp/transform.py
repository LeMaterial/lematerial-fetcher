# Copyright 2025 Entalpic
from enum import Enum
from typing import Any, Optional

import numpy as np
from pymatgen.core import Structure

from material_fetcher.fetcher.mp.utils import (
    extract_structure_optimization_tasks,
    map_tasks_to_functionals,
)
from material_fetcher.model.models import RawStructure
from material_fetcher.model.optimade import Functional, OptimadeStructure
from material_fetcher.transform import BaseTransformer
from material_fetcher.utils.logging import logger


class TaskType(Enum):
    STRUCTURE_OPTIMIZATION = "Structure Optimization"
    DEPRECATED = "Deprecated"


class MPTransformer(BaseTransformer):
    """
    Materials Project transformer implementation.
    Transforms raw Materials Project data into OptimadeStructures.
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

    def transform_row(
        self,
        raw_structure: RawStructure,
        task_table_name: Optional[str] = None,
    ) -> list[OptimadeStructure]:
        """
        Transform a raw Materials Project structure into OptimadeStructures.

        Parameters
        ----------
        raw_structure : RawStructure
            RawStructure object from the dumped database
        task_table_name : Optional[str]
            Task table name to read targets or trajectories from

        Returns
        -------
        list[OptimadeStructure]
            The transformed OptimadeStructure objects.
            If the list is empty, nothing from the structure should be included in the database.
        """
        tasks, calc_types = extract_structure_optimization_tasks(
            raw_structure, self.source_db, task_table_name
        )
        functionals = map_tasks_to_functionals(tasks, calc_types)

        if not functionals:
            return []

        targets_functionals = {
            functional: self._get_task_targets(task, raw_structure.id, functional)
            for functional, task in functionals.items()
        }

        pmg_structure = Structure.from_dict(raw_structure.attributes["structure"])

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

        optimade_structures = []
        for functional in functionals.keys():
            targets = targets_functionals[functional]
            optimade_structure = OptimadeStructure(
                # Basic fields
                id=raw_structure.attributes["material_id"],
                source="mp",
                immutable_id=raw_structure.attributes["material_id"],
                # Structural fields
                elements=raw_structure.attributes["elements"],
                nelements=raw_structure.attributes["nelements"],
                elements_ratios=element_ratios,
                # sites
                nsites=raw_structure.attributes["nsites"],
                cartesian_site_positions=cartesian_site_positions,
                species_at_sites=species_at_sites,
                species=species,
                # chemistry
                chemical_formula_anonymous=raw_structure.attributes[
                    "formula_anonymous"
                ],
                chemical_formula_descriptive=raw_structure.attributes["formula_pretty"],
                chemical_formula_reduced=chemical_formula_reduced,
                # dimensionality
                dimension_types=[1, 1, 1],
                nperiodic_dimensions=3,
                lattice_vectors=lattice_vectors,
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

    def _get_task_targets(
        self, task: RawStructure, material_id: str, functional: Functional
    ) -> dict[str, Any]:
        """
        Get the target parameters of a task.

        Parameters
        ----------
        task : RawStructure
            The task to get the targets from.

        Returns
        -------
        dict[str, Any]
            The target parameters of the task.
        """
        targets = {}
        targets["energy"] = task.attributes["output"]["energy"]
        try:
            targets["magnetic_moments"] = [
                site["properties"]["magmom"]
                for site in task.attributes["output"]["structure"]["sites"]
            ]
        except (TypeError, KeyError):
            logger.warning(
                f"No magnetic moments for {material_id} with functional {functional}"
            )
            targets["magnetic_moments"] = None
        targets["forces"] = task.attributes["calcs_reversed"][0]["output"][
            "ionic_steps"
        ][-1]["forces"]
        # TODO(ramlaoui): Check if these are correct
        targets["dos_ef"] = task.attributes["output"].get("efermi", None)  # dos_ef
        targets["total_magnetization"] = (
            task.attributes["output"]
            .get("magnetization", {})
            .get("total_magnetization", None)
        )
        try:
            targets["stress_tensor"] = task.attributes["output"]["stress"]
        except KeyError:
            logger.warning(
                f"No stress tensor for {material_id} with functional {functional}"
            )
            targets["stress_tensor"] = None

        targets["cross_compatible"] = True
        non_compatible_elements = ["V", "Cs"]
        # TODO(msiron): What about Yb?
        for element in non_compatible_elements:
            if element in task.attributes["composition_reduced"].keys():
                targets["cross_compatible"] = False

        return targets
