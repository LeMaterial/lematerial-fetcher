# Copyright 2025 Entalpic
from typing import Optional

from lematerial_fetcher.database.postgres import StructuresDatabase
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import OptimadeStructure
from lematerial_fetcher.transform import BaseTransformer


class OQMDFetcher(BaseTransformer):
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

    def transform_row(
        self,
        raw_structure: RawStructure,
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
        key_mapping = {
            "immutable_id": "_oqmd_entry_id",
            "chemical_formula_reduced": "chemical_formula_reduced",
            "chemical_formula_anonymous": "chemical_formula_anonymous",
            "chemical_formula_descriptive": "chemical_formula_descriptive",
            "cartesian_site_positions": "cartesian_site_positions",
            "elements": "elements",
            # "elements_ratios": "elements_ratios",
            "nelements": "nelements",
            "nsites": "nsites",
            "species_at_sites": "species_at_sites",
            # "species": "species",
            "nperiodic_dimensions": "nperiodic_dimensions",
            # "dimension_types": "dimension_types",
            "last_modified": "last_modified",
            "lattice_vectors": "lattice_vectors",
            "_oqmd_band_gap": "band_gap",
            "_oqmd_delta_e": "energy",
        }

        values_dict = {}
        for key, value in key_mapping.items():
            values_dict[value] = raw_structure.attributes[key]

        values_dict["immutable_id"] = f"oqmd-{raw_structure.id}"

        optimade_structure = OptimadeStructure(
            **values_dict,
            id=values_dict["immutable_id"],
            source="oqmd",
            # functional=self._oqmd_functional(raw_structure),
        )

        return [optimade_structure]

    # def _oqmd_functional(self, raw_structure: RawStructure) -> Functional:
    #     """
    #     Get the functional from the raw OQMD structure.
    #     """
    #     if "pbe" in raw_structure.attributes["_oqmd_xc_functional"].lower():
    #         return Functional.PBE
    #     elif "pbesol" in raw_structure.attributes["_alexandria_xc_functional"].lower():
    #         return Functional.PBESOL
    #     elif "scan" in raw_structure.attributes["_alexandria_xc_functional"].lower():
    #         return Functional.SCAN
    #     else:
    #         raise ValueError(
    #             f"Unknown functional: {raw_structure.attributes['_alexandria_xc_functional']}"
    #         )
