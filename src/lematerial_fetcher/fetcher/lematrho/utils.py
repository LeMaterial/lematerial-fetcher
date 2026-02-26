# Copyright 2025 Entalpic
import gzip
import io
import os
from datetime import datetime
from typing import Any, Optional

from pymatgen.core import Structure
from pymatgen.io.vasp import Chgcar, Vasprun

from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.utils.logging import logger

# ── S3 folder structure constants ──────────────────────────────────────────────
STATIC_CALC_TYPE = "LeMatRhoStaticMaker"
RELAX_CALC_TYPE = "LeMatRhoRelaxMaker_1"
STATIC_FILES = ["CHGCAR.gz", "AECCAR0.gz", "AECCAR1.gz", "AECCAR2.gz"]
RELAX_FILES = ["vasprun.xml.gz"]

# Only process materials with these ID prefixes
VALID_PREFIXES = ("oqmd-", "mp-", "agm")

# Conservative default due to high memory usage per CHGCAR (~hundreds of MB)
DEFAULT_MAX_WORKERS = 4

# Map from S3 filename to compressed grid key name
GRID_KEY_MAP = {
    "CHGCAR.gz": "charge_density",
    "AECCAR0.gz": "aeccar0",
    "AECCAR1.gz": "aeccar1",
    "AECCAR2.gz": "aeccar2",
}

# Subprocess timeout constants (seconds)
BADER_TIMEOUT = 600
CHGSUM_TIMEOUT = 300
CHARGEMOL_TIMEOUT = 600


def download_gz_file_from_s3(client: Any, bucket: str, key: str) -> bytes:
    """Download and decompress a gzipped file from S3.

    Args:
        client: Boto3 S3 client.
        bucket: S3 bucket name.
        key: S3 object key.

    Returns:
        Decompressed file contents as raw bytes.
    """
    response = client.get_object(Bucket=bucket, Key=key)
    compressed = response["Body"].read()
    return gzip.decompress(compressed)


def parse_vasprun_structure(vasprun_bytes: bytes) -> Structure:
    """Parse a vasprun.xml to extract the final relaxed structure.

    Args:
        vasprun_bytes: Raw vasprun.xml content.

    Returns:
        The final relaxed pymatgen Structure.
    """
    vasprun = Vasprun(
        io.BytesIO(vasprun_bytes),
        parse_dos=False,
        parse_eigen=False,
        parse_potcar_file=False,
    )
    return vasprun.final_structure


def compress_chgcar(chgcar_bytes: bytes, grid_shape: tuple[int, int, int]) -> list:
    """Parse a CHGCAR file and compress its charge density using pyrho.

    Args:
        chgcar_bytes: Raw CHGCAR file content (uncompressed VASP format).
        grid_shape: Target grid shape for lossy compression, e.g. ``(15, 15, 15)``.

    Returns:
        Compressed charge density grid as a nested Python list.
    """
    from pyrho.charge_density import ChargeDensity

    chgcar = Chgcar.from_file(io.BytesIO(chgcar_bytes))
    charge_density = ChargeDensity.from_pmg(chgcar)
    compressed = charge_density.pgrids["total"].lossy_smooth_compression(grid_shape)
    result = compressed.tolist()
    del chgcar, charge_density, compressed
    return result


def build_raw_structure(
    material_id: str,
    structure: Structure,
    compressed_grids: dict[str, Optional[list]],
    grid_shape: tuple[int, int, int],
    s3_prefix: str,
) -> RawStructure:
    """Build a RawStructure from parsed charge density data.

    Args:
        material_id: Material identifier, e.g. ``"agm000001"``.
        structure: Pymatgen Structure parsed from vasprun.xml.
        compressed_grids: Dict mapping grid names (``"charge_density"``,
            ``"aeccar0"``, ``"aeccar1"``, ``"aeccar2"``) to compressed
            grid lists or ``None``.
        grid_shape: Grid shape used for compression.
        s3_prefix: S3 prefix path for the material folder.

    Returns:
        A ``RawStructure`` ready for database insertion.
    """
    attributes = {
        "structure": structure.as_dict(),
        "compressed_charge_density": compressed_grids.get("charge_density"),
        "compressed_aeccar0": compressed_grids.get("aeccar0"),
        "compressed_aeccar1": compressed_grids.get("aeccar1"),
        "compressed_aeccar2": compressed_grids.get("aeccar2"),
        "grid_shape": list(grid_shape),
        "s3_prefix": s3_prefix,
    }

    return RawStructure(
        id=material_id,
        type="lematrho",
        attributes=attributes,
        last_modified=datetime.now(),
    )


def write_potcar(structure: Structure, tmpdir: str) -> None:
    """Generate a POTCAR file for the given structure.

    Uses ``MatPESStaticSet`` to select pseudopotentials consistent with
    Materials Project settings and writes the resulting POTCAR to *tmpdir*.

    Args:
        structure: Pymatgen Structure for which to generate the POTCAR.
        tmpdir: Directory where ``POTCAR`` will be written.

    Raises:
        OSError: If ``PMG_VASP_PSP_DIR`` is not set or the pseudopotential
            files cannot be found.
    """
    from pymatgen.io.vasp.sets import MatPESStaticSet

    input_set = MatPESStaticSet(structure)
    input_set.potcar.write_file(os.path.join(tmpdir, "POTCAR"))
