# Copyright 2025 Entalpic
from typing import Optional

from pymatgen.core import Structure

from lematerial_fetcher.database.postgres import (
    StructuresDatabase,
    TrajectoriesDatabase,
)
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import Functional, OptimadeStructure
from lematerial_fetcher.models.trajectories import Trajectory, has_trajectory_converged
from lematerial_fetcher.transform import BaseTransformer
from lematerial_fetcher.utils.structure import get_optimade_from_pymatgen


class AlexandriaTransformer(BaseTransformer):
    """
    Alexandria transformer implementation.
    Transforms raw Alexandria data into OptimadeStructures.
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
        source_db: Optional[StructuresDatabase] = None,
        task_table_name: Optional[str] = None,
    ) -> list[OptimadeStructure]:
        """
        Transform a raw Alexandria structure into OptimadeStructures.

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
        key_mapping = {
            "immutable_id": "immutable_id",
            "chemical_formula_reduced": "chemical_formula_reduced",
            "chemical_formula_anonymous": "chemical_formula_anonymous",
            "chemical_formula_descriptive": "chemical_formula_descriptive",
            "cartesian_site_positions": "cartesian_site_positions",
            "elements": "elements",
            "elements_ratios": "elements_ratios",
            "nelements": "nelements",
            "nsites": "nsites",
            "species_at_sites": "species_at_sites",
            "species": "species",
            "nperiodic_dimensions": "nperiodic_dimensions",
            "dimension_types": "dimension_types",
            "last_modified": "last_modified",
            "lattice_vectors": "lattice_vectors",
            "_alexandria_forces": "forces",
            "_alexandria_stress_tensor": "stress_tensor",
            "_alexandria_dos_ef": "dos_ef",
            "_alexandria_energy": "energy",
            "_alexandria_magnetic_moments": "magnetic_moments",
            "_alexandria_magnetization": "total_magnetization",
        }

        values_dict = {}
        for key, value in key_mapping.items():
            values_dict[value] = raw_structure.attributes[key]

        optimade_structure = OptimadeStructure(
            **values_dict,
            id=raw_structure.id,  # problem, this is empty
            source="alexandria",
            functional=self._alexandria_functional(raw_structure),
            cross_compatibility=True,  # All Alexandria structures have compatible parameters
        )

        return [optimade_structure]

    def _alexandria_functional(self, raw_structure: RawStructure) -> Functional:
        """
        Get the functional from the raw Alexandria structure.
        """
        if "pbe" in raw_structure.attributes["_alexandria_xc_functional"].lower():
            return Functional.PBE
        elif "pbesol" in raw_structure.attributes["_alexandria_xc_functional"].lower():
            return Functional.PBESOL
        elif "scan" in raw_structure.attributes["_alexandria_xc_functional"].lower():
            return Functional.SCAN
        else:
            raise ValueError(
                f"Unknown functional: {raw_structure.attributes['_alexandria_xc_functional']}"
            )


class AlexandriaTrajectoryTransformer(BaseTransformer):
    """
    Alexandria trajectory transformer implementation.
    Transforms raw Alexandria trajectory data into Trajectory objects.
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
        source_db: StructuresDatabase,
        task_table_name: Optional[str] = None,
    ) -> list[Trajectory]:
        """
        Transform a raw Alexandria structure into OptimadeStructures.

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

        trajectories = []

        current_relaxation_number = 0
        for relaxation_number, calc in enumerate(raw_structure.attributes):
            relaxation_steps = calc["steps"]
            for relaxation_step, relaxation_step_dict in enumerate(relaxation_steps):
                structure = Structure.from_dict(relaxation_step_dict["structure"])
                optimade_structure_dict = get_optimade_from_pymatgen(structure)

                targets = {
                    "energy": relaxation_step_dict["energy"],
                    "forces": relaxation_step_dict["forces"],
                    "stress_tensor": relaxation_step_dict["stress"],
                }

                # Avoids errors when one component of the force is None
                # which makes the calculation obsolete?
                if any(any(f is None for f in force) for force in targets["forces"]):
                    targets["forces"] = None

                trajectory = Trajectory(
                    immutable_id=raw_structure.id,
                    id=f"{raw_structure.id}-{calc['functional']}-{current_relaxation_number}",
                    source="alexandria",
                    last_modified=raw_structure.last_modified,
                    **optimade_structure_dict,
                    **targets,
                    relaxation_number=relaxation_number,
                    relaxation_step=relaxation_step,
                    functional=Functional(calc["functional"].lower()),
                    cross_compatibility=True,
                )

                trajectories.append(trajectory)
                current_relaxation_number += 1

        if not has_trajectory_converged(trajectories):
            return []

        return trajectories
