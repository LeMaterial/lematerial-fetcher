# Copyright 2025 Entalpic
"""LeMatRho transformer: raw structures to OPTIMADE format.

Converts raw LeMatRho structures (with compressed charge densities from the
fetch step) into ``OptimadeStructure`` objects. Optionally downloads VASP
files from S3 and runs Bader and DDEC6 charge analysis via shared helpers
in ``utils``.
"""
import os
import shutil
from datetime import datetime
from typing import Optional

from pymatgen.core import Structure

from lematerial_fetcher.database.postgres import OptimadeDatabase, StructuresDatabase
from lematerial_fetcher.fetcher.lematrho.utils import (
    STATIC_CALC_TYPE,
    download_gz_file_from_s3,
    run_bader_from_bytes,
    run_ddec6_from_bytes,
)
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import Functional, OptimadeStructure
from lematerial_fetcher.transform import BaseTransformer
from lematerial_fetcher.utils.aws import get_authenticated_aws_client
from lematerial_fetcher.utils.logging import logger
from lematerial_fetcher.utils.structure import get_optimade_from_pymatgen


def get_cross_compatibility(elements: list[str]) -> bool:
    """Determine cross-compatibility for LeMatRho structures.

    All LeMatRho structures are cross-compatible (no element exclusions).

    Args:
        elements: List of element symbols in the structure.

    Returns:
        Always ``True``.
    """
    return True


class LeMatRhoTransformer(BaseTransformer):
    """Transformer for LeMatRho charge density data.

    Transforms raw structures (with compressed charge densities from the fetch step)
    into ``OptimadeStructure`` objects. Optionally runs Bader and DDEC6 charge
    analysis using pymatgen wrappers around external tools.

    External tool requirements:
        - ``bader``: Bader charge analysis executable
        - ``chargemol``: DDEC6 charge partitioning executable
        - ``PMG_VASP_PSP_DIR``: Env var for POTCAR generation
        - atomic densities directory: For DDEC6/chargemol analysis

    Args:
        config: Transformer configuration.
        database_class: Database class for storing results.
        structure_class: Pydantic model class for validated structures.
        debug: If ``True``, process sequentially for debugging.
    """

    def __init__(
        self,
        config=None,
        database_class=OptimadeDatabase,
        structure_class=OptimadeStructure,
        debug=False,
    ):
        super().__init__(config, database_class, structure_class, debug)
        self._aws_client = None
        self._bader_path = None
        self._chargemol_path = None
        self._atomic_densities_path = None
        self._can_generate_potcar = False
        self._validate_tools()

    def _validate_tools(self) -> None:
        """Check availability of external tools and log warnings for missing ones.

        Sets instance attributes ``_bader_path``, ``_chargemol_path``,
        ``_atomic_densities_path``, and ``_can_generate_potcar``.
        """
        self._bader_path = getattr(self.config, "bader_path", None) or shutil.which(
            "bader"
        )
        if not self._bader_path:
            logger.warning(
                "bader executable not found. Bader charges will not be computed."
            )

        self._chargemol_path = getattr(
            self.config, "chargemol_path", None
        ) or shutil.which("Chargemol_09_26_2017_linux_serial")
        if not self._chargemol_path:
            self._chargemol_path = shutil.which("chargemol")
        if not self._chargemol_path:
            logger.warning(
                "chargemol executable not found. DDEC6 charges will not be computed."
            )

        self._atomic_densities_path = getattr(
            self.config, "atomic_densities_path", None
        )
        if self._atomic_densities_path and not os.path.isdir(
            self._atomic_densities_path
        ):
            logger.warning(
                f"Atomic densities directory not found: {self._atomic_densities_path}. "
                "DDEC6 analysis requires this directory."
            )
            self._atomic_densities_path = None

        if not os.environ.get("PMG_VASP_PSP_DIR"):
            logger.warning(
                "PMG_VASP_PSP_DIR not set. POTCAR generation will fail. "
                "Bader and DDEC6 analysis require POTCAR."
            )
            self._can_generate_potcar = False
        else:
            self._can_generate_potcar = True

    @property
    def aws_client(self):
        """Lazy-initialized authenticated S3 client."""
        if self._aws_client is None:
            self._aws_client = get_authenticated_aws_client()
        return self._aws_client

    @property
    def can_run_bader(self) -> bool:
        """Check if all Bader analysis prerequisites are met."""
        return bool(self._bader_path and self._can_generate_potcar)

    @property
    def can_run_ddec6(self) -> bool:
        """Check if all DDEC6 analysis prerequisites are met."""
        return bool(
            self._chargemol_path
            and self._atomic_densities_path
            and self._can_generate_potcar
        )

    def transform_row(
        self,
        raw_structure: RawStructure,
        source_db: Optional[StructuresDatabase] = None,
        task_table_name: Optional[str] = None,
    ) -> list[OptimadeStructure]:
        """Transform a raw LeMatRho structure into an OptimadeStructure.

        Args:
            raw_structure: Raw structure from the fetch step, with charge
                density data in its attributes dict.
            source_db: Not used for LeMatRho.
            task_table_name: Not used for LeMatRho.

        Returns:
            Single-element list containing the transformed ``OptimadeStructure``.
        """
        attrs = raw_structure.attributes
        material_id = raw_structure.id

        # Extract pymatgen Structure from raw data
        structure = Structure.from_dict(attrs["structure"])

        # Get base OPTIMADE fields from structure
        optimade_dict = get_optimade_from_pymatgen(structure)

        # Extract pre-computed compressed grids from fetch step
        compressed_charge_density = attrs.get("compressed_charge_density")
        compressed_aeccar0 = attrs.get("compressed_aeccar0")
        compressed_aeccar1 = attrs.get("compressed_aeccar1")
        compressed_aeccar2 = attrs.get("compressed_aeccar2")
        grid_shape = attrs.get("grid_shape")
        s3_prefix = attrs.get("s3_prefix")

        # Cross-compatibility (no element exclusions for LeMatRho)
        cross_compatibility = get_cross_compatibility(optimade_dict["elements"])

        # Bader analysis (independent from DDEC6)
        bader_charges = None
        bader_atomic_volume = None
        if self.can_run_bader and s3_prefix:
            bader_charges, bader_atomic_volume = self._run_bader_analysis(
                structure, s3_prefix, material_id
            )

        # DDEC6 analysis (independent from Bader)
        ddec6_charges = None
        if self.can_run_ddec6 and s3_prefix:
            ddec6_charges = self._run_ddec6_analysis(
                structure, s3_prefix, material_id
            )

        optimade_structure = OptimadeStructure(
            id=material_id,
            source="lematrho",
            immutable_id=material_id,
            last_modified=raw_structure.last_modified or datetime.now(),
            **optimade_dict,
            functional=Functional.PBE,
            cross_compatibility=cross_compatibility,
            compressed_charge_density=compressed_charge_density,
            compressed_aeccar0=compressed_aeccar0,
            compressed_aeccar1=compressed_aeccar1,
            compressed_aeccar2=compressed_aeccar2,
            charge_density_grid_shape=grid_shape,
            bader_charges=bader_charges,
            bader_atomic_volume=bader_atomic_volume,
            ddec6_charges=ddec6_charges,
            compute_space_group=True,
            compute_bawl_hash=True,
        )

        return [optimade_structure]

    def _run_bader_analysis(
        self, structure: Structure, s3_prefix: str, material_id: str
    ) -> tuple[Optional[list[float]], Optional[list[float]]]:
        """Download CHGCAR/AECCAR files from S3 and run Bader charge analysis.

        Args:
            structure: Pymatgen Structure for POTCAR generation.
            s3_prefix: S3 folder prefix for this material.
            material_id: Material identifier, used for logging.

        Returns:
            Tuple of ``(net_charges, atomic_volumes)`` or ``(None, None)``
            on failure.
        """
        try:
            bucket = self.config.lematrho_bucket_name
            raw_files = {}
            for filename in ["CHGCAR.gz", "AECCAR0.gz", "AECCAR2.gz"]:
                key = f"{s3_prefix}/{STATIC_CALC_TYPE}/{filename}"
                data = download_gz_file_from_s3(self.aws_client, bucket, key)
                raw_files[filename.replace(".gz", "")] = data
                del data

            return run_bader_from_bytes(
                structure, raw_files, self._bader_path, material_id
            )
        except Exception as e:
            logger.warning(f"Bader S3 download failed for {material_id}: {e}")
            return None, None

    def _run_ddec6_analysis(
        self, structure: Structure, s3_prefix: str, material_id: str
    ) -> Optional[list[float]]:
        """Download CHGCAR from S3 and run DDEC6 charge analysis.

        Args:
            structure: Pymatgen Structure for POTCAR generation.
            s3_prefix: S3 folder prefix for this material.
            material_id: Material identifier, used for logging.

        Returns:
            DDEC6 net charges per site, or ``None`` on failure.
        """
        try:
            bucket = self.config.lematrho_bucket_name
            key = f"{s3_prefix}/{STATIC_CALC_TYPE}/CHGCAR.gz"
            data = download_gz_file_from_s3(self.aws_client, bucket, key)
            raw_files = {"CHGCAR": data}
            del data

            return run_ddec6_from_bytes(
                structure,
                raw_files,
                self._chargemol_path,
                self._atomic_densities_path,
                material_id,
            )
        except Exception as e:
            logger.warning(f"DDEC6 S3 download failed for {material_id}: {e}")
            return None
