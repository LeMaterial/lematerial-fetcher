# Copyright 2025 Entalpic
import os
import shutil
import subprocess
import tempfile
from datetime import datetime
from typing import Optional

from pymatgen.core import Structure

from lematerial_fetcher.database.postgres import OptimadeDatabase, StructuresDatabase
from lematerial_fetcher.fetcher.lematrho.utils import download_gz_file_from_s3
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import Functional, OptimadeStructure
from lematerial_fetcher.transform import BaseTransformer
from lematerial_fetcher.utils.aws import get_authenticated_aws_client
from lematerial_fetcher.utils.logging import logger
from lematerial_fetcher.utils.structure import get_optimade_from_pymatgen

STATIC_CALC_TYPE = "LeMatRhoStaticMaker"

BADER_TIMEOUT = 600  # seconds
CHGSUM_TIMEOUT = 300  # seconds
CHARGEMOL_TIMEOUT = 600  # seconds


def get_cross_compatibility(elements: list[str]) -> bool:
    """Determine cross-compatibility for LeMatRho structures.

    Currently, Yb-containing structures are not cross-compatible
    (same policy as Alexandria).
    """
    return "Yb" not in elements


def parse_acf_dat(filepath: str) -> tuple[list[float], list[float]]:
    """Parse Bader ACF.dat file for electron counts and atomic volumes.

    Parameters
    ----------
    filepath : str
        Path to ACF.dat file

    Returns
    -------
    tuple[list[float], list[float]]
        (electron_counts, atomic_volumes) per atom
    """
    electron_counts = []
    atomic_volumes = []
    with open(filepath) as f:
        lines = f.readlines()

    # Skip header (first 2 lines), parse data rows until separator line
    for line in lines[2:]:
        stripped = line.strip()
        if stripped.startswith("-") or not stripped:
            break
        parts = stripped.split()
        if len(parts) >= 7:
            electron_counts.append(float(parts[4]))  # CHARGE column
            atomic_volumes.append(float(parts[6]))  # ATOMIC VOL column

    return electron_counts, atomic_volumes


def read_potcar_zval(filepath: str) -> dict[str, float]:
    """Read valence electron counts from a POTCAR file.

    Parses TITEL and ZVAL lines to build an element -> valence electrons mapping.

    Parameters
    ----------
    filepath : str
        Path to POTCAR file

    Returns
    -------
    dict[str, float]
        Element symbol -> number of valence electrons
    """
    zval = {}
    current_element = None
    with open(filepath) as f:
        for line in f:
            if "TITEL" in line:
                parts = line.split()
                if len(parts) >= 4:
                    # Handle element names like 'Si_d' -> 'Si'
                    current_element = parts[3].split("_")[0]
            elif "ZVAL" in line and current_element:
                parts = line.split("=")
                if len(parts) >= 2:
                    try:
                        zval[current_element] = float(parts[1].split()[0])
                        current_element = None
                    except (ValueError, IndexError):
                        pass
    return zval


def parse_ddec6_charges(tmpdir: str) -> list[float]:
    """Parse DDEC6 net atomic charges from chargemol output.

    Parameters
    ----------
    tmpdir : str
        Directory containing chargemol output files

    Returns
    -------
    list[float]
        Net DDEC6 charges per atom
    """
    filepath = os.path.join(tmpdir, "DDEC6_even_tempered_net_atomic_charges.xyz")
    charges = []
    with open(filepath) as f:
        lines = f.readlines()

    n_atoms = int(lines[0].strip())
    for line in lines[2 : 2 + n_atoms]:
        parts = line.split()
        if len(parts) >= 5:
            charges.append(float(parts[4]))

    return charges


class LeMatRhoTransformer(BaseTransformer):
    """Transformer for LeMatRho charge density data.

    Transforms raw structures (with compressed charge densities from the fetch step)
    into OptimadeStructure objects. Optionally runs Bader and DDEC6 charge analysis
    using external tools.

    External tool requirements:
    - bader: Bader charge analysis executable
    - perl + chgsum.pl: For summing AECCAR0 + AECCAR2 reference charge density
    - chargemol: DDEC6 charge partitioning executable
    - PMG_VASP_PSP_DIR: Environment variable for POTCAR generation
    - atomic_densities directory: For DDEC6/chargemol analysis
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
        self._chgsum_script_path = None
        self._perl_path = None
        self._atomic_densities_path = None
        self._can_generate_potcar = False
        self._validate_tools()

    def _validate_tools(self):
        """Check availability of external tools and log warnings for missing ones."""
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

        self._chgsum_script_path = getattr(
            self.config, "chgsum_script_path", None
        )
        if self._chgsum_script_path and not os.path.isfile(self._chgsum_script_path):
            logger.warning(
                f"chgsum.pl not found at {self._chgsum_script_path}. "
                "Bader analysis requires this script."
            )
            self._chgsum_script_path = None

        self._perl_path = shutil.which("perl")
        if not self._perl_path:
            logger.warning(
                "perl not found. Bader analysis requires perl for chgsum.pl."
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
        return bool(
            self._bader_path
            and self._chgsum_script_path
            and self._perl_path
            and self._can_generate_potcar
        )

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

        Parameters
        ----------
        raw_structure : RawStructure
            Raw structure from the fetch step, with charge density data in attributes
        source_db : Optional[StructuresDatabase]
            Not used for LeMatRho
        task_table_name : Optional[str]
            Not used for LeMatRho

        Returns
        -------
        list[OptimadeStructure]
            Single-element list with the transformed structure
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

        # Cross-compatibility (exclude Yb, same policy as Alexandria)
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
        """Run Bader charge analysis.

        Downloads CHGCAR, AECCAR0, AECCAR2 from S3, runs chgsum.pl to create
        the reference charge density (CHGCAR_sum), then runs bader and parses results.

        Returns
        -------
        tuple[Optional[list[float]], Optional[list[float]]]
            (net_charges, atomic_volumes) or (None, None) on failure
        """
        try:
            bucket = self.config.lematrho_bucket_name
            with tempfile.TemporaryDirectory() as tmpdir:
                # Download raw VASP charge density files
                for filename in ["CHGCAR.gz", "AECCAR0.gz", "AECCAR2.gz"]:
                    key = f"{s3_prefix}/{STATIC_CALC_TYPE}/{filename}"
                    data = download_gz_file_from_s3(self.aws_client, bucket, key)
                    outname = filename.replace(".gz", "")
                    with open(os.path.join(tmpdir, outname), "wb") as f:
                        f.write(data)
                    del data

                # Generate POTCAR
                self._write_potcar(structure, tmpdir)

                # Sum AECCAR0 + AECCAR2 -> CHGCAR_sum
                subprocess.run(
                    [
                        self._perl_path,
                        self._chgsum_script_path,
                        "AECCAR0",
                        "AECCAR2",
                    ],
                    cwd=tmpdir,
                    timeout=CHGSUM_TIMEOUT,
                    check=True,
                    capture_output=True,
                )

                # Run Bader analysis with reference charge density
                subprocess.run(
                    [self._bader_path, "CHGCAR", "-ref", "CHGCAR_sum"],
                    cwd=tmpdir,
                    timeout=BADER_TIMEOUT,
                    check=True,
                    capture_output=True,
                )

                # Parse ACF.dat for electron counts and atomic volumes
                electron_counts, atomic_volumes = parse_acf_dat(
                    os.path.join(tmpdir, "ACF.dat")
                )

                # Compute net charges: valence_electrons - bader_electron_count
                zval = read_potcar_zval(os.path.join(tmpdir, "POTCAR"))
                net_charges = []
                for site, electron_count in zip(structure.sites, electron_counts):
                    element = str(site.specie)
                    valence = zval.get(element, 0)
                    net_charges.append(valence - electron_count)

                return net_charges, atomic_volumes

        except subprocess.TimeoutExpired:
            logger.warning(f"Bader analysis timed out for {material_id}")
            return None, None
        except subprocess.CalledProcessError as e:
            logger.warning(
                f"Bader subprocess failed for {material_id}: "
                f"exit code {e.returncode}, stderr: {e.stderr}"
            )
            return None, None
        except Exception as e:
            logger.warning(f"Bader analysis failed for {material_id}: {e}")
            return None, None

    def _run_ddec6_analysis(
        self, structure: Structure, s3_prefix: str, material_id: str
    ) -> Optional[list[float]]:
        """Run DDEC6 charge analysis via chargemol.

        Downloads CHGCAR from S3, generates POTCAR, writes chargemol config,
        runs chargemol, and parses DDEC6 net charges.

        Returns
        -------
        Optional[list[float]]
            DDEC6 net charges per site, or None on failure
        """
        try:
            bucket = self.config.lematrho_bucket_name
            with tempfile.TemporaryDirectory() as tmpdir:
                # Download CHGCAR
                key = f"{s3_prefix}/{STATIC_CALC_TYPE}/CHGCAR.gz"
                data = download_gz_file_from_s3(self.aws_client, bucket, key)
                with open(os.path.join(tmpdir, "CHGCAR"), "wb") as f:
                    f.write(data)
                del data

                # Generate POTCAR
                self._write_potcar(structure, tmpdir)

                # Write chargemol job control file
                self._write_chargemol_config(tmpdir)

                # Run chargemol
                env = os.environ.copy()
                env["DDEC6_ATOMIC_DENSITIES_DIR"] = self._atomic_densities_path
                subprocess.run(
                    [self._chargemol_path],
                    cwd=tmpdir,
                    timeout=CHARGEMOL_TIMEOUT,
                    check=True,
                    capture_output=True,
                    env=env,
                )

                return parse_ddec6_charges(tmpdir)

        except subprocess.TimeoutExpired:
            logger.warning(f"DDEC6 analysis timed out for {material_id}")
            return None
        except subprocess.CalledProcessError as e:
            logger.warning(
                f"DDEC6 subprocess failed for {material_id}: "
                f"exit code {e.returncode}, stderr: {e.stderr}"
            )
            return None
        except Exception as e:
            logger.warning(f"DDEC6 analysis failed for {material_id}: {e}")
            return None

    def _write_potcar(self, structure: Structure, tmpdir: str) -> None:
        """Generate POTCAR for the given structure.

        Requires PMG_VASP_PSP_DIR environment variable to be set.
        """
        from pymatgen.io.vasp.sets import MPRelaxSet

        input_set = MPRelaxSet(structure)
        input_set.potcar.write_file(os.path.join(tmpdir, "POTCAR"))

    def _write_chargemol_config(self, tmpdir: str) -> None:
        """Write job_control.txt for chargemol DDEC6 analysis."""
        config_content = (
            "<net charge>\n"
            "0.0\n"
            "</net charge>\n"
            "<periodicity along A, B, and C vectors>\n"
            ".true.\n"
            ".true.\n"
            ".true.\n"
            "</periodicity along A, B, and C vectors>\n"
            "<atomic densities directory complete path>\n"
            f"{self._atomic_densities_path}\n"
            "</atomic densities directory complete path>\n"
            "<charge type>\n"
            "DDEC6\n"
            "</charge type>\n"
            "<input filename>\n"
            "CHGCAR\n"
            "</input filename>\n"
        )
        with open(os.path.join(tmpdir, "job_control.txt"), "w") as f:
            f.write(config_content)
