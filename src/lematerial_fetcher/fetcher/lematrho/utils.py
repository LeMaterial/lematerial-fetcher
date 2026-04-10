# Copyright 2025 Entalpic
"""Utilities for the LeMatRho fetcher and transformer.

Provides S3 download helpers, VASP file parsing (vasprun.xml, CHGCAR),
lossy charge-density compression via pyrho, and shared Bader/DDEC6
charge-analysis wrappers built on pymatgen's ``BaderAnalysis`` and
``ChargemolAnalysis``.
"""

import gzip
import os
import tempfile
from typing import Any, Optional

import numpy as np
from pymatgen.command_line.bader_caller import BaderAnalysis
from pymatgen.command_line.chargemol_caller import ChargemolAnalysis
from pymatgen.core import Structure
from pymatgen.io.vasp import Chgcar, Vasprun

from lematerial_fetcher.utils.logging import logger

# ── S3 folder structure constants ──────────────────────────────────────────────
STATIC_CALC_TYPE = "LeMatRhoStaticMaker"
STATIC_FILES = ["vasprun.xml.gz", "CHGCAR.gz", "AECCAR0.gz", "AECCAR1.gz", "AECCAR2.gz"]

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


def get_cross_compatibility(elements: list[str]) -> bool:
    """Determine cross-compatibility for LeMatRho structures.

    All LeMatRho structures are cross-compatible (no element exclusions).

    Args:
        elements: List of element symbols in the structure.

    Returns:
        Always ``True``.
    """
    return True


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
    body = response["Body"]
    try:
        compressed = body.read()
        decompressed = gzip.decompress(compressed)
        del compressed
        return decompressed
    finally:
        body.close()


def parse_vasprun_output(
    vasprun_bytes: bytes,
) -> tuple[Structure, Optional[list[list[float]]], Optional[list[list[float]]], Optional[float]]:
    """Parse a vasprun.xml: final structure, last ionic-step forces/stress, and total energy.

    Intended for the ``LeMatRhoStaticMaker`` vasprun (``NSW=0``), where the structure
    is already fully relaxed and ``ionic_steps[-1]`` holds the residual forces and
    stress at the relaxed geometry.  Writes bytes to a temporary file because
    pymatgen's ``Vasprun`` requires a filesystem path, not a file-like object.

    Args:
        vasprun_bytes: Raw vasprun.xml content.

    Returns:
        Tuple of (final_structure, forces, stress_tensor, energy) where:
        - final_structure: pymatgen Structure
        - forces: nsites × 3 (eV/Å), or None if absent
        - stress_tensor: 3 × 3 (kBar), or None if absent
        - energy: total energy in eV from ``Vasprun.final_energy``, or None if absent
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "vasprun.xml")
        with open(path, "wb") as f:
            f.write(vasprun_bytes)
        vasprun = Vasprun(
            path,
            parse_dos=False,
            parse_eigen=False,
            parse_potcar_file=False,
        )

        structure = vasprun.final_structure
        forces_out: Optional[list[list[float]]] = None
        stress_out: Optional[list[list[float]]] = None
        energy_out: Optional[float] = None

        if vasprun.ionic_steps:
            final = vasprun.ionic_steps[-1]
            frc = final.get("forces")  # Forces in nsites × 3 (eV/Å)
            if frc is not None:
                forces_out = np.asarray(frc, dtype=float).reshape(-1, 3).tolist()

            strs = final.get("stress")  # Stress tensor in 3 × 3 (kBar)
            if strs is not None:
                s = np.asarray(strs, dtype=float)
                if s.shape == (3, 3):
                    stress_out = s.tolist()
                elif s.size == 9:
                    stress_out = s.reshape(3, 3).tolist()
                elif s.size == 6:
                    xx, yy, zz, xy, yz, xz = (float(x) for x in s.flat[:6])
                    stress_out = [[xx, xy, xz], [xy, yy, yz], [xz, yz, zz]]

        try:
            energy_out = float(vasprun.final_energy)
        except Exception:
            pass

        return structure, forces_out, stress_out, energy_out


def compress_chgcar(chgcar_bytes: bytes, grid_shape: tuple[int, int, int]) -> list:
    """Parse a CHGCAR file and compress its charge density using pyrho.

    Writes bytes to a temporary file because pymatgen's ``Chgcar.from_file``
    requires a filesystem path, not a file-like object.

    Args:
        chgcar_bytes: Raw CHGCAR file content (uncompressed VASP format).
        grid_shape: Target grid shape for lossy compression, e.g. ``(15, 15, 15)``.

    Returns:
        Compressed charge density grid as a nested Python list.
    """
    from pyrho.charge_density import ChargeDensity

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "CHGCAR")
        with open(path, "wb") as f:
            f.write(chgcar_bytes)
        chgcar = Chgcar.from_file(path)
    charge_density = ChargeDensity.from_pmg(chgcar)
    compressed = charge_density.pgrids["total"].lossy_smooth_compression(grid_shape)
    result = compressed.tolist()
    del chgcar, charge_density, compressed
    return result


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


def run_bader_from_bytes(
    structure: Structure,
    raw_files: dict[str, bytes],
    bader_path: str,
    material_id: str,
) -> tuple[Optional[list[float]], Optional[list[float]]]:
    """Run Bader charge analysis from raw decompressed VASP file bytes.

    Writes CHGCAR, AECCAR0, AECCAR2, and POTCAR to a temp directory, sums
    AECCAR0 + AECCAR2 using pymatgen ``Chgcar`` arithmetic, then delegates to
    ``BaderAnalysis`` which runs the bader executable and parses results.

    Args:
        structure: Pymatgen Structure for POTCAR generation.
        raw_files: Mapping of VASP filenames to their raw bytes,
            e.g. ``{"CHGCAR": b"...", "AECCAR0": b"...", "AECCAR2": b"..."}``.
        bader_path: Path to the bader executable.
        material_id: Material identifier, used for logging.

    Returns:
        Tuple of ``(net_charges, atomic_volumes)`` or ``(None, None)`` on failure.
    """
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            for name in ["CHGCAR", "AECCAR0", "AECCAR2"]:
                with open(os.path.join(tmpdir, name), "wb") as f:
                    f.write(raw_files[name])

            write_potcar(structure, tmpdir)

            # Sum AECCAR0 + AECCAR2 using pymatgen Chgcar arithmetic
            aeccar0 = Chgcar.from_file(os.path.join(tmpdir, "AECCAR0"))
            aeccar2 = Chgcar.from_file(os.path.join(tmpdir, "AECCAR2"))
            chgcar_sum = aeccar0 + aeccar2
            chgcar_sum.write_file(os.path.join(tmpdir, "CHGCAR_sum"))
            del aeccar0, aeccar2, chgcar_sum

            ba = BaderAnalysis(
                chgcar_filename=os.path.join(tmpdir, "CHGCAR"),
                potcar_filename=os.path.join(tmpdir, "POTCAR"),
                chgref_filename=os.path.join(tmpdir, "CHGCAR_sum"),
                bader_path=bader_path,
            )

            # charge_transfer = electron_count - valence (positive = gained electrons)
            # Negate to get valence - electron_count (positive = cationic)
            net_charges = [-ct for ct in ba.summary["charge_transfer"]]
            atomic_volumes = list(ba.summary["atomic_volume"])

            return net_charges, atomic_volumes

    except Exception as e:
        logger.warning(f"Bader analysis failed for {material_id}: {e}")
        return None, None


def run_ddec6_from_bytes(
    structure: Structure,
    raw_files: dict[str, bytes],
    chargemol_path: str,
    atomic_densities_path: str,
    material_id: str,
) -> Optional[list[float]]:
    """Run DDEC6 charge analysis from raw decompressed VASP file bytes.

    Writes CHGCAR and POTCAR to a temp directory, then delegates to
    ``ChargemolAnalysis`` which runs chargemol and parses DDEC6 charges.

    Note: Temporarily sets the ``CHARGEMOL_COMMAND`` env var for pymatgen.
    This is process-safe (``ProcessPoolExecutor`` gives each worker its own
    env) but NOT thread-safe — do not call from multiple threads.

    Args:
        structure: Pymatgen Structure for POTCAR generation.
        raw_files: Mapping with at least ``{"CHGCAR": b"..."}``.
        chargemol_path: Path to the chargemol executable.
        atomic_densities_path: Path to atomic densities directory.
        material_id: Material identifier, used for logging.

    Returns:
        DDEC6 net charges per site, or ``None`` on failure.
    """
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "CHGCAR"), "wb") as f:
                f.write(raw_files["CHGCAR"])

            write_potcar(structure, tmpdir)

            # pymatgen's ChargemolAnalysis reads the chargemol binary path
            # exclusively from the CHARGEMOL_COMMAND env var — there is no
            # constructor parameter to pass it directly. We save/restore the
            # original value so this function doesn't leak side effects.
            orig_chargemol_cmd = os.environ.get("CHARGEMOL_COMMAND")
            os.environ["CHARGEMOL_COMMAND"] = chargemol_path
            try:
                ca = ChargemolAnalysis(
                    path=tmpdir,
                    atomic_densities_path=atomic_densities_path,
                    run_chargemol=True,
                )
                return list(ca.ddec_charges)
            finally:
                if orig_chargemol_cmd is None:
                    os.environ.pop("CHARGEMOL_COMMAND", None)
                else:
                    os.environ["CHARGEMOL_COMMAND"] = orig_chargemol_cmd

    except Exception as e:
        logger.warning(f"DDEC6 analysis failed for {material_id}: {e}")
        return None
