# Copyright 2025 Entalpic
import gzip
import io
from datetime import datetime
from typing import Any, Optional

from pymatgen.core import Structure
from pymatgen.io.vasp import Chgcar, Vasprun

from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.utils.logging import logger


def download_gz_file_from_s3(client: Any, bucket: str, key: str) -> bytes:
    """Download and decompress a gzipped file from S3.

    Parameters
    ----------
    client : Any
        Boto3 S3 client
    bucket : str
        S3 bucket name
    key : str
        S3 object key

    Returns
    -------
    bytes
        Decompressed file contents
    """
    response = client.get_object(Bucket=bucket, Key=key)
    compressed = response["Body"].read()
    return gzip.decompress(compressed)


def parse_vasprun_structure(vasprun_bytes: bytes) -> Structure:
    """Parse a vasprun.xml to extract the final relaxed structure.

    Parameters
    ----------
    vasprun_bytes : bytes
        Raw vasprun.xml content

    Returns
    -------
    Structure
        The final relaxed pymatgen Structure
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

    Parameters
    ----------
    chgcar_bytes : bytes
        Raw CHGCAR file content
    grid_shape : tuple[int, int, int]
        Target grid shape for lossy compression (e.g. (15, 15, 15))

    Returns
    -------
    list
        Compressed charge density grid as nested list
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

    Parameters
    ----------
    material_id : str
        Material identifier (e.g. "agm000001")
    structure : Structure
        Pymatgen Structure from vasprun.xml
    compressed_grids : dict
        Dict with keys "charge_density", "aeccar0", "aeccar1", "aeccar2",
        values are compressed grid lists or None
    grid_shape : tuple[int, int, int]
        Grid shape used for compression
    s3_prefix : str
        S3 prefix path for the material folder

    Returns
    -------
    RawStructure
        Structure ready for database insertion
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
