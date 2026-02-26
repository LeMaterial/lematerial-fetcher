# Copyright 2025 Entalpic
"""Direct S3-to-Parquet pipeline for LeMatRho charge density data.

Bypasses PostgreSQL entirely: downloads from S3, compresses charge densities,
runs Bader/DDEC6 analysis, and writes Parquet files directly.
"""

import concurrent.futures
import gc
import json
import os
import shutil
import subprocess
import tempfile
from datetime import datetime
from glob import glob
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from pymatgen.core import Structure

from lematerial_fetcher.fetcher.lematrho.transform import (
    get_cross_compatibility,
    parse_acf_dat,
    parse_ddec6_charges,
    read_potcar_zval,
)
from lematerial_fetcher.fetcher.lematrho.utils import (
    BADER_TIMEOUT,
    CHARGEMOL_TIMEOUT,
    CHGSUM_TIMEOUT,
    GRID_KEY_MAP,
    RELAX_CALC_TYPE,
    STATIC_CALC_TYPE,
    STATIC_FILES,
    VALID_PREFIXES,
    compress_chgcar,
    download_gz_file_from_s3,
    parse_vasprun_structure,
    write_potcar,
)
from lematerial_fetcher.models.optimade import Functional, OptimadeStructure
from lematerial_fetcher.utils.aws import get_authenticated_aws_client
from lematerial_fetcher.utils.config import DirectPipelineConfig
from lematerial_fetcher.utils.logging import logger
from lematerial_fetcher.utils.structure import get_optimade_from_pymatgen

# Columns in the output Parquet files (matches HuggingFace Features schema)
PARQUET_COLUMNS = [
    "elements",
    "nsites",
    "chemical_formula_anonymous",
    "chemical_formula_reduced",
    "chemical_formula_descriptive",
    "nelements",
    "dimension_types",
    "nperiodic_dimensions",
    "lattice_vectors",
    "immutable_id",
    "cartesian_site_positions",
    "species",
    "species_at_sites",
    "last_modified",
    "elements_ratios",
    "stress_tensor",
    "energy",
    "energy_corrected",
    "magnetic_moments",
    "forces",
    "total_magnetization",
    "charges",
    "dos_ef",
    "functional",
    "cross_compatibility",
    "bawl_fingerprint",
    "space_group_it_number",
    "compressed_charge_density",
    "compressed_aeccar0",
    "compressed_aeccar1",
    "compressed_aeccar2",
    "charge_density_grid_shape",
    "bader_charges",
    "bader_atomic_volume",
    "ddec6_charges",
]

# PyArrow schema matching the HuggingFace Features
PARQUET_SCHEMA = pa.schema(
    [
        ("elements", pa.list_(pa.string())),
        ("nsites", pa.int32()),
        ("chemical_formula_anonymous", pa.string()),
        ("chemical_formula_reduced", pa.string()),
        ("chemical_formula_descriptive", pa.string()),
        ("nelements", pa.int8()),
        ("dimension_types", pa.list_(pa.int8())),
        ("nperiodic_dimensions", pa.int8()),
        ("lattice_vectors", pa.list_(pa.list_(pa.float64()))),
        ("immutable_id", pa.string()),
        ("cartesian_site_positions", pa.list_(pa.list_(pa.float64()))),
        ("species", pa.string()),  # JSON-serialized
        ("species_at_sites", pa.list_(pa.string())),
        ("last_modified", pa.string()),
        ("elements_ratios", pa.list_(pa.float64())),
        ("stress_tensor", pa.list_(pa.list_(pa.float64()))),
        ("energy", pa.float64()),
        ("energy_corrected", pa.float64()),
        ("magnetic_moments", pa.list_(pa.float64())),
        ("forces", pa.list_(pa.list_(pa.float64()))),
        ("total_magnetization", pa.float64()),
        ("charges", pa.list_(pa.float64())),
        ("dos_ef", pa.float64()),
        ("functional", pa.string()),
        ("cross_compatibility", pa.bool_()),
        ("bawl_fingerprint", pa.string()),
        ("space_group_it_number", pa.int32()),
        ("compressed_charge_density", pa.string()),  # JSON-serialized
        ("compressed_aeccar0", pa.string()),  # JSON-serialized
        ("compressed_aeccar1", pa.string()),  # JSON-serialized
        ("compressed_aeccar2", pa.string()),  # JSON-serialized
        ("charge_density_grid_shape", pa.list_(pa.int32())),
        ("bader_charges", pa.list_(pa.float64())),
        ("bader_atomic_volume", pa.list_(pa.float64())),
        ("ddec6_charges", pa.list_(pa.float64())),
    ]
)

# Files needed for Bader analysis (must keep raw bytes)
_BADER_FILES = {"CHGCAR.gz", "AECCAR0.gz", "AECCAR2.gz"}
# Files needed for DDEC6 analysis
_DDEC6_FILES = {"CHGCAR.gz"}



def _run_bader_from_bytes(
    structure: Structure,
    raw_files: dict[str, bytes],
    tool_paths: dict,
    material_id: str,
) -> tuple[Optional[list[float]], Optional[list[float]]]:
    """Run Bader charge analysis from raw decompressed file bytes.

    Writes raw VASP files to a temp directory, generates POTCAR, runs
    ``chgsum.pl`` and ``bader``, and parses the resulting ACF.dat.

    Args:
        structure: Pymatgen Structure for POTCAR generation.
        raw_files: Mapping of VASP filenames to their raw bytes,
            e.g. ``{"CHGCAR": b"...", "AECCAR0": b"...", "AECCAR2": b"..."}``.
        tool_paths: Tool path configuration dict from ``_validate_tools()``.
        material_id: Material identifier, used for logging.

    Returns:
        Tuple of ``(net_charges, atomic_volumes)`` or ``(None, None)`` on failure.
    """
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write raw decompressed files (bader expects plain-text VASP format)
            for name in ["CHGCAR", "AECCAR0", "AECCAR2"]:
                with open(os.path.join(tmpdir, name), "wb") as f:
                    f.write(raw_files[name])

            # Generate POTCAR
            write_potcar(structure, tmpdir)

            # Sum AECCAR0 + AECCAR2 -> CHGCAR_sum
            subprocess.run(
                [
                    tool_paths["perl_path"],
                    tool_paths["chgsum_script_path"],
                    "AECCAR0",
                    "AECCAR2",
                ],
                cwd=tmpdir,
                timeout=CHGSUM_TIMEOUT,
                check=True,
                capture_output=True,
            )

            # Run Bader
            subprocess.run(
                [tool_paths["bader_path"], "CHGCAR", "-ref", "CHGCAR_sum"],
                cwd=tmpdir,
                timeout=BADER_TIMEOUT,
                check=True,
                capture_output=True,
            )

            # Parse results
            electron_counts, atomic_volumes = parse_acf_dat(
                os.path.join(tmpdir, "ACF.dat")
            )
            zval = read_potcar_zval(os.path.join(tmpdir, "POTCAR"))

            net_charges = []
            for site, ec in zip(structure.sites, electron_counts):
                element = str(site.specie)
                valence = zval.get(element, 0)
                net_charges.append(valence - ec)

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


def _run_ddec6_from_bytes(
    structure: Structure,
    raw_files: dict[str, bytes],
    tool_paths: dict,
    material_id: str,
) -> Optional[list[float]]:
    """Run DDEC6 charge analysis from raw decompressed file bytes.

    Writes CHGCAR and POTCAR to a temp directory, runs chargemol, and
    parses the DDEC6 net atomic charges.

    Args:
        structure: Pymatgen Structure for POTCAR generation.
        raw_files: Mapping with at least ``{"CHGCAR": b"..."}``.
        tool_paths: Tool path configuration dict from ``_validate_tools()``.
        material_id: Material identifier, used for logging.

    Returns:
        DDEC6 net charges per site, or ``None`` on failure.
    """
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "CHGCAR"), "wb") as f:
                f.write(raw_files["CHGCAR"])

            write_potcar(structure, tmpdir)

            # Write chargemol job control file
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
                f"{tool_paths['atomic_densities_path']}\n"
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

            # Run chargemol
            env = os.environ.copy()
            env["DDEC6_ATOMIC_DENSITIES_DIR"] = tool_paths["atomic_densities_path"]
            subprocess.run(
                [tool_paths["chargemol_path"]],
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


def _structure_to_row(
    optimade_structure: OptimadeStructure,
) -> dict:
    """Convert an OptimadeStructure to a flat dict matching the Parquet schema.

    JSON-serializes species and compressed charge density fields.
    Converts ``Functional`` enums to their string value and ``datetime``
    to ISO format.

    Args:
        optimade_structure: Validated ``OptimadeStructure`` instance.

    Returns:
        Flat dict with one key per ``PARQUET_COLUMNS`` entry, ready for
        ``pyarrow.Table.from_pydict()``.
    """
    row = {}
    for col in PARQUET_COLUMNS:
        row[col] = getattr(optimade_structure, col, None)

    # Convert datetime to ISO string
    if row["last_modified"] is not None:
        row["last_modified"] = row["last_modified"].isoformat()

    # Convert Functional enum to string
    if row["functional"] is not None:
        row["functional"] = row["functional"].value

    # JSON-serialize complex fields
    if row["species"] is not None:
        row["species"] = json.dumps(row["species"])

    for col in [
        "compressed_charge_density",
        "compressed_aeccar0",
        "compressed_aeccar1",
        "compressed_aeccar2",
    ]:
        if row[col] is not None:
            row[col] = json.dumps(row[col])

    return row


class LeMatRhoDirectPipeline:
    """Direct S3-to-Parquet pipeline for LeMatRho charge density data.

    Downloads from S3, compresses charge densities via pyrho, optionally runs
    Bader and DDEC6 charge analysis, and writes Parquet files directly.
    No PostgreSQL required.

    Args:
        config: Pipeline configuration.
        debug: If ``True``, process sequentially in the main process.
    """

    def __init__(self, config: DirectPipelineConfig, debug: bool = False):
        self.config = config
        self.debug = debug
        self._checkpoint_path = os.path.join(config.output_dir, ".checkpoint.txt")
        self._processed_ids: set[str] = set()

        # Validate external tools
        self._tool_paths = self._validate_tools()

        # Create output directory
        os.makedirs(config.output_dir, exist_ok=True)

    def _validate_tools(self) -> dict:
        """Check availability of external tools for Bader/DDEC6 analysis.

        Returns:
            Dict with keys ``bader_path``, ``chargemol_path``,
            ``chgsum_script_path``, ``perl_path``, ``atomic_densities_path``,
            ``can_generate_potcar``, ``can_run_bader``, ``can_run_ddec6``.
        """
        tools = {}

        tools["bader_path"] = self.config.bader_path or shutil.which("bader")
        if not tools["bader_path"]:
            logger.warning(
                "bader executable not found. Bader charges will not be computed."
            )

        tools["chargemol_path"] = (
            self.config.chargemol_path
            or shutil.which("Chargemol_09_26_2017_linux_serial")
            or shutil.which("chargemol")
        )
        if not tools["chargemol_path"]:
            logger.warning(
                "chargemol executable not found. DDEC6 charges will not be computed."
            )

        tools["chgsum_script_path"] = self.config.chgsum_script_path
        if tools["chgsum_script_path"] and not os.path.isfile(
            tools["chgsum_script_path"]
        ):
            logger.warning(
                f"chgsum.pl not found at {tools['chgsum_script_path']}. "
                "Bader analysis requires this script."
            )
            tools["chgsum_script_path"] = None

        tools["perl_path"] = shutil.which("perl")
        if not tools["perl_path"]:
            logger.warning(
                "perl not found. Bader analysis requires perl for chgsum.pl."
            )

        tools["atomic_densities_path"] = self.config.atomic_densities_path
        if tools["atomic_densities_path"] and not os.path.isdir(
            tools["atomic_densities_path"]
        ):
            logger.warning(
                f"Atomic densities directory not found: {tools['atomic_densities_path']}. "
                "DDEC6 analysis requires this directory."
            )
            tools["atomic_densities_path"] = None

        tools["can_generate_potcar"] = bool(os.environ.get("PMG_VASP_PSP_DIR"))
        if not tools["can_generate_potcar"]:
            logger.warning(
                "PMG_VASP_PSP_DIR not set. POTCAR generation will fail. "
                "Bader and DDEC6 analysis require POTCAR."
            )

        tools["can_run_bader"] = bool(
            tools["bader_path"]
            and tools["chgsum_script_path"]
            and tools["perl_path"]
            and tools["can_generate_potcar"]
        )
        tools["can_run_ddec6"] = bool(
            tools["chargemol_path"]
            and tools["atomic_densities_path"]
            and tools["can_generate_potcar"]
        )

        return tools

    def run(self) -> None:
        """Run the full pipeline: list materials, process, write Parquet, optionally push.

        In debug mode, materials are processed sequentially in the main process.
        Otherwise, uses a ``ProcessPoolExecutor`` with a work-stealing pattern.
        Writes Parquet chunks of ``config.parquet_chunk_size`` rows using atomic
        rename. Appends each processed ID to a checkpoint file for crash recovery.
        """
        # 1. List material folders from S3
        logger.info("Listing material folders from S3...")
        material_ids = self._list_materials()
        logger.info(f"Found {len(material_ids)} materials in S3")

        # 2. Load checkpoint, filter already-processed
        self._processed_ids = self._load_checkpoint()
        remaining = [m for m in material_ids if m not in self._processed_ids]

        # Apply limit if set
        if self.config.limit is not None and len(remaining) > self.config.limit:
            remaining = remaining[: self.config.limit]

        logger.info(
            f"Already processed: {len(self._processed_ids)}, "
            f"remaining: {len(remaining)}"
        )

        if not remaining:
            logger.info("All materials already processed.")
            if self.config.hf_repo_id:
                self._push_to_huggingface()
            return

        # 3. Process materials
        buffer = []
        chunk_index = self._get_next_chunk_index()
        processed_count = 0
        failed_count = 0

        if self.debug:
            for material_id in remaining:
                result = self._process_material(
                    material_id, self.config, self._tool_paths
                )
                if result is not None:
                    buffer.append(result)
                    self._append_checkpoint(material_id)
                    processed_count += 1
                else:
                    failed_count += 1

                if len(buffer) >= self.config.parquet_chunk_size:
                    self._write_parquet_chunk(buffer, chunk_index)
                    buffer.clear()
                    chunk_index += 1

                total = processed_count + failed_count
                if total % self.config.log_every == 0 and total > 0:
                    logger.info(
                        f"Progress: {processed_count} processed, {failed_count} failed"
                    )
        else:
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=self.config.num_workers
            ) as executor:
                remaining_iter = iter(remaining)
                futures = {}

                # Submit initial batch (2x workers for pipeline saturation)
                initial_count = min(self.config.num_workers * 2, len(remaining))
                for _ in range(initial_count):
                    mid = next(remaining_iter)
                    future = executor.submit(
                        self._process_material, mid, self.config, self._tool_paths
                    )
                    futures[future] = mid

                while futures:
                    done, _ = concurrent.futures.wait(
                        futures,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    for future in done:
                        material_id = futures.pop(future)

                        try:
                            result = future.result()
                            if result is not None:
                                buffer.append(result)
                                self._append_checkpoint(material_id)
                                processed_count += 1
                            else:
                                failed_count += 1
                        except Exception as e:
                            logger.warning(
                                f"Worker exception for {material_id}: {e}"
                            )
                            failed_count += 1

                        # Write chunk if buffer is full
                        if len(buffer) >= self.config.parquet_chunk_size:
                            self._write_parquet_chunk(buffer, chunk_index)
                            buffer.clear()
                            chunk_index += 1

                        # Submit replacement (work-stealing)
                        try:
                            next_id = next(remaining_iter)
                            f = executor.submit(
                                self._process_material,
                                next_id,
                                self.config,
                                self._tool_paths,
                            )
                            futures[f] = next_id
                        except StopIteration:
                            pass

                    total = processed_count + failed_count
                    if total % self.config.log_every == 0 and total > 0:
                        logger.info(
                            f"Progress: {processed_count} processed, "
                            f"{failed_count} failed"
                        )

        # Write remaining buffer
        if buffer:
            self._write_parquet_chunk(buffer, chunk_index)

        logger.info(
            f"Done. {processed_count} processed, {failed_count} failed."
        )

        # 4. Push if configured
        if self.config.hf_repo_id:
            self._push_to_huggingface()

    def _list_materials(self) -> list[str]:
        """List material folder prefixes from S3, filtered by ``VALID_PREFIXES``.

        Returns:
            Sorted list of material IDs (e.g. ``["agm000001", "mp-123", ...]``).
        """
        client = get_authenticated_aws_client()
        bucket = self.config.lematrho_bucket_name
        paginator = client.get_paginator("list_objects_v2")

        material_folders = []
        for page in paginator.paginate(Bucket=bucket, Delimiter="/"):
            for prefix_info in page.get("CommonPrefixes", []):
                folder_name = prefix_info["Prefix"].rstrip("/")
                if folder_name.startswith(VALID_PREFIXES):
                    material_folders.append(folder_name)

        return material_folders

    @staticmethod
    def _process_material(
        material_id: str,
        config: DirectPipelineConfig,
        tool_paths: dict,
    ) -> Optional[dict]:
        """Process a single material: download, compress, analyze, return row dict.

        Designed to run in a worker process. Creates a fresh AWS client per
        invocation (boto3 clients are not multiprocess-safe). Calls
        ``gc.collect()`` after each material to free memory from large CHGCAR
        arrays.

        Args:
            material_id: Material folder name, e.g. ``"agm000001"``.
            config: Pipeline configuration.
            tool_paths: Tool path dict from ``_validate_tools()``.

        Returns:
            Flat dict matching ``PARQUET_COLUMNS``, or ``None`` on failure.
        """
        bucket = config.lematrho_bucket_name
        grid_shape = config.lematrho_grid_shape

        try:
            # Fresh client per worker (boto3 clients are NOT multiprocess-safe)
            aws_client = get_authenticated_aws_client()

            # Step 1: Download and parse vasprun.xml.gz for structure
            vasprun_key = f"{material_id}/{RELAX_CALC_TYPE}/vasprun.xml.gz"
            try:
                vasprun_bytes = download_gz_file_from_s3(
                    aws_client, bucket, vasprun_key
                )
                structure = parse_vasprun_structure(vasprun_bytes)
                del vasprun_bytes
            except Exception as e:
                logger.warning(
                    f"Failed to parse vasprun.xml.gz for {material_id}: {e}"
                )
                return None

            # Step 2: Download and compress charge density files
            compressed_grids = {}
            raw_files = {}  # Keep raw bytes for Bader/DDEC6

            # Determine which raw files to keep
            need_raw = set()
            if tool_paths["can_run_bader"]:
                need_raw |= _BADER_FILES
            if tool_paths["can_run_ddec6"]:
                need_raw |= _DDEC6_FILES

            for filename in STATIC_FILES:
                s3_key = f"{material_id}/{STATIC_CALC_TYPE}/{filename}"
                grid_name = GRID_KEY_MAP[filename]
                try:
                    raw_bytes = download_gz_file_from_s3(aws_client, bucket, s3_key)

                    # Compress via pyrho
                    compressed = compress_chgcar(raw_bytes, grid_shape)
                    compressed_grids[grid_name] = compressed
                    del compressed

                    # Keep raw bytes if needed for analysis
                    if filename in need_raw:
                        vasp_name = filename.replace(".gz", "")
                        raw_files[vasp_name] = raw_bytes
                    del raw_bytes
                except Exception as e:
                    logger.warning(
                        f"Failed to process {filename} for {material_id}: {e}"
                    )

            # Step 3: Bader analysis (if tools available and files downloaded)
            bader_charges = None
            bader_atomic_volume = None
            if tool_paths["can_run_bader"] and all(
                k in raw_files for k in ["CHGCAR", "AECCAR0", "AECCAR2"]
            ):
                bader_charges, bader_atomic_volume = _run_bader_from_bytes(
                    structure, raw_files, tool_paths, material_id
                )

            # Step 4: DDEC6 analysis (if tools available and CHGCAR downloaded)
            ddec6_charges = None
            if tool_paths["can_run_ddec6"] and "CHGCAR" in raw_files:
                ddec6_charges = _run_ddec6_from_bytes(
                    structure, raw_files, tool_paths, material_id
                )

            # Free raw file bytes
            del raw_files

            # Step 5: Build OptimadeStructure (Pydantic validation)
            optimade_dict = get_optimade_from_pymatgen(structure)
            cross_compatibility = get_cross_compatibility(optimade_dict["elements"])

            optimade_structure = OptimadeStructure(
                id=material_id,
                source="lematrho",
                immutable_id=material_id,
                last_modified=datetime.now(),
                **optimade_dict,
                functional=Functional.PBE,
                cross_compatibility=cross_compatibility,
                compressed_charge_density=compressed_grids.get("charge_density"),
                compressed_aeccar0=compressed_grids.get("aeccar0"),
                compressed_aeccar1=compressed_grids.get("aeccar1"),
                compressed_aeccar2=compressed_grids.get("aeccar2"),
                charge_density_grid_shape=list(grid_shape),
                bader_charges=bader_charges,
                bader_atomic_volume=bader_atomic_volume,
                ddec6_charges=ddec6_charges,
                compute_space_group=True,
                compute_bawl_hash=True,
            )

            # Step 6: Convert to flat dict for Parquet
            row = _structure_to_row(optimade_structure)
            del optimade_structure, compressed_grids

            # Step 7: Force garbage collection in worker
            gc.collect()

            return row

        except Exception as e:
            logger.error(f"Failed to process material {material_id}: {e}")
            gc.collect()
            return None

    def _load_checkpoint(self) -> set[str]:
        """Load processed material IDs from checkpoint file.

        Returns:
            Set of already-processed material IDs. Empty set if no checkpoint exists.
        """
        if not os.path.exists(self._checkpoint_path):
            return set()
        with open(self._checkpoint_path, "r") as f:
            return {line.strip() for line in f if line.strip()}

    def _append_checkpoint(self, material_id: str) -> None:
        """Append a material ID to the checkpoint file and flush to disk.

        Args:
            material_id: ID to record as processed.
        """
        with open(self._checkpoint_path, "a") as f:
            f.write(material_id + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _get_next_chunk_index(self) -> int:
        """Determine next chunk index from existing ``chunk_*.parquet`` files.

        Ignores ``.tmp`` files left by interrupted writes.

        Returns:
            Next available chunk index (0 if no existing chunks).
        """
        existing = glob(os.path.join(self.config.output_dir, "chunk_*.parquet"))
        if not existing:
            return 0
        indices = []
        for path in existing:
            basename = os.path.basename(path)
            try:
                idx = int(basename.replace("chunk_", "").replace(".parquet", ""))
                indices.append(idx)
            except ValueError:
                pass
        return max(indices) + 1 if indices else 0

    def _write_parquet_chunk(self, rows: list[dict], chunk_index: int) -> None:
        """Write rows to a Parquet file atomically (write ``.tmp``, then rename).

        Args:
            rows: List of flat dicts matching ``PARQUET_COLUMNS``.
            chunk_index: Chunk sequence number (zero-padded in filename).
        """
        final_path = os.path.join(
            self.config.output_dir, f"chunk_{chunk_index:06d}.parquet"
        )
        tmp_path = final_path + ".tmp"

        # Build column-oriented data from row-oriented dicts
        columns = {col: [row.get(col) for row in rows] for col in PARQUET_COLUMNS}
        table = pa.table(columns, schema=PARQUET_SCHEMA)
        pq.write_table(table, tmp_path)

        # Atomic rename
        os.rename(tmp_path, final_path)
        logger.info(
            f"Wrote chunk {chunk_index} ({len(rows)} rows) to {final_path}"
        )

    def _push_to_huggingface(self) -> None:
        """Load all Parquet files and push to HuggingFace as a private dataset."""
        from datasets import load_dataset

        parquet_files = os.path.join(self.config.output_dir, "chunk_*.parquet")
        logger.info(f"Loading Parquet files from {parquet_files}")

        dataset = load_dataset("parquet", data_files=parquet_files)

        logger.info(f"Pushing to HuggingFace repo: {self.config.hf_repo_id}")
        dataset["train"].push_to_hub(
            self.config.hf_repo_id,
            token=self.config.hf_token,
            private=True,
        )
        logger.info("Push complete.")
