# oc20_dataset_utils.py
import os
import tarfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Optional
from pymatgen.core import Structure
import numpy as np
import pandas as pd
import pickle

import lzma
import multiprocessing as mp
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from torch_geometric.data import Data

PathType = str | Path


from lematerial_fetcher.utils.logging import logger
from lematerial_fetcher.utils.structure import get_optimade_from_pymatgen

OC20_BASE_URL = "https://dl.fbaipublicfiles.com/opencatalystproject/data/is2res_train_val_test_lmdbs.tar.gz"


def download_and_extract(
    url: str = OC20_BASE_URL,
    target_dir: Optional[str] = None,
    extract: bool = True,
    unlink: bool = False,
) -> str:
    """
    Download a file from a URL and optionally extract it.

    Parameters
    ----------
    url : str
        URL to download the file from
    target_dir : Optional[str]
        Directory to save the downloaded file. If None, uses the cache directory.
    extract : bool
        Whether to extract the file if it's a compressed archive (tar, tar.gz, zip)
    unlink : bool
        Whether to remove the downloaded file after extraction

    Returns
    -------
    str
        Path to the downloaded file or extracted directory
    """

    os.makedirs(target_dir, exist_ok=True)

    # Determine the file extension
    if url.endswith(".tar.gz"):
        file_ext = ".tar.gz"
    else:
        file_ext = os.path.splitext(url)[1]
        if file_ext == "":
            # If no extension in URL, try to determine from the last part
            file_ext = os.path.splitext(os.path.basename(url))[1]

    # If still no extension, assume it's a tar.gz
    if file_ext == "":
        file_ext = ".tar.gz"

    target_file = os.path.join(target_dir, os.path.basename(url))

    if not os.path.exists(target_file):
        print(f"Downloading {url} to {target_file}...")

        def report_progress(block_num, block_size, total_size):
            if total_size > 0:
                pbar.update(block_size)
                if block_num == 0:
                    pbar.total = total_size

        with tqdm(
            unit="B",
            unit_scale=True,
            miniters=1,
            desc=f"Downloading {os.path.basename(url)}",
        ) as pbar:
            urllib.request.urlretrieve(
                url, filename=target_file, reporthook=report_progress
            )

    if extract:
        if file_ext in [".tar.gz", ".tgz"]:
            print(f"Extracting {target_file}...")
            with tarfile.open(target_file, "r:gz") as tar:
                tar.extractall(path=target_dir)
            # Return the directory containing the extracted files
            return_dir = target_dir
        elif file_ext == ".tar":
            print(f"Extracting {target_file}...")
            with tarfile.open(target_file, "r:") as tar:
                tar.extractall(path=target_dir)
            # Return the directory containing the extracted files
            return_dir = target_dir
        elif file_ext == ".zip":
            print(f"Extracting {target_file}...")
            with zipfile.ZipFile(target_file, "r") as zip_ref:
                zip_ref.extractall(path=target_dir)
            # Return the directory containing the extracted files
            return_dir = target_dir
        else:
            logger.warning(f"Cannot extract unknown file extension: {file_ext}")
            return_dir = target_file
    else:
        return_dir = target_file

    if unlink:
        os.unlink(target_file)

    return return_dir


def remove_all_extensions(path: str | Path) -> str:
    """Remove all extensions from a filename.

    For example:
        'file.tar.gz' -> 'file'
        'file.db' -> 'file'
    """
    path = Path(path)
    return path.stem.split(".")[0]


def uncompress_xz(file_path: PathType) -> PathType:
    """Uncompress a single .xz file.

    Parameters
    ----------
    file_path : str
        Path to the .xz file to uncompress

    Returns
    -------
    str
        Path to the uncompressed file, or original path if file was not compressed
        or decompression failed

    Notes
    -----
    The original .xz file is deleted after successful decompression.
    """
    if not file_path.endswith(".xz"):
        logger.warning(f"File {file_path} is not a .xz file, will not be uncompressed")
        return file_path
    try:
        with lzma.open(file_path, "rb") as f_in:
            with open(file_path.replace(".xz", ""), "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(file_path)
        return file_path.replace(".xz", "")
    except Exception as e:
        logger.error(f"Error uncompressing {file_path}: {str(e)}")
        return file_path


def uncompress_dir(
    path: PathType, recursive: bool = False, num_workers: Optional[int] = None
) -> PathType:
    """Uncompress all .xz files in a directory using multiple processes.

    Parameters
    ----------
    path : str | Path
        Directory path containing .xz files
    recursive : bool, optional
        Whether to recursively process subdirectories, by default False
    num_workers : int, optional
        Number of worker processes to use. If None, uses CPU count - 1, by default None

    Returns
    -------
    str
        The input path after processing all files

    Notes
    -----
    Uses ProcessPoolExecutor for parallel processing. Each worker process handles
    decompression of individual files independently.

    Examples
    --------
    >>> uncompress_dir("/path/to/data")  # Uses all available CPUs - 1
    >>> uncompress_dir("/path/to/data", recursive=True, num_workers=4)  # Use 4 workers
    """
    if num_workers is None:
        num_workers = mp.cpu_count() - 1

    # Collect all .xz files
    xz_files = []
    for root, dirs, files in os.walk(path):
        xz_files.extend([os.path.join(root, f) for f in files if f.endswith(".xz")])
        if not recursive:
            break

    if not xz_files:
        return path

    logger.info(
        f"Found {len(xz_files)} .xz files to uncompress using {num_workers} workers"
    )

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        future_to_file = {executor.submit(uncompress_xz, f): f for f in xz_files}

        # Process completed tasks with progress tracking
        with tqdm(total=len(xz_files), desc="Uncompressing files") as pbar:
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    _ = future.result()
                    logger.debug(f"Successfully uncompressed {file_path}")
                except Exception as e:
                    logger.error(f"Failed to uncompress {file_path}: {str(e)}")
                pbar.update(1)

    return path


def convert_pyg_data(data: Data):
    """
    Recreates the PyTorch Geometric Data object, useful to convert old
    Data objects to the new format.

    Parameters
    ----------
    data : Data
        The data to convert

    Returns
    -------
    Data
        The converted data
    """

    return Data(**{k: v for k, v in data.__dict__.items() if v is not None})


def oc20_to_structures(data_row):
    """Extract slab and adsorbate as pymatgen.Structure objects from OC20 row."""
    atomic_numbers = data_row["atomic_numbers"]
    positions = data_row["pos"]
    tags = data_row["tags"]
    lattice = data_row["cell"]

    # Boolean masks
    slab_mask = np.isin(tags, [0, 1])
    molecule_mask = tags == 2

    # Extract structures
    slab = Structure(
        lattice=lattice,
        species=atomic_numbers[slab_mask],
        coords=positions[slab_mask],
        coords_are_cartesian=True,
    )
    molecule = Structure(
        lattice=lattice,
        species=atomic_numbers[molecule_mask],
        coords=positions[molecule_mask],
        coords_are_cartesian=True,
    )
    adslab = Structure(
        lattice=lattice,
        species=atomic_numbers,
        coords=positions,
        coords_are_cartesian=True,
    )
    return slab, molecule, adslab


def oc20_to_optimade_dicts(data_row):
    slab, molecule, adslab = oc20_to_structures(data_row)
    molecule_energy = 0.0
    slab_energy = data_row["y_init"] - molecule_energy
    adslab_energy = data_row["y_relaxed"]

    eq_left = molecule.composition.formula + " + " + slab.composition.formula
    eq_right = adslab.composition.formula
    equation = f"{eq_left} -> {eq_right}"

    reaction_energy = adslab_energy - slab_energy - molecule_energy

    row = {
        "publication": "oc20",
        "equation": equation,
        "reaction_energy": reaction_energy,
        "activation_energy": None,
        "dftCode": "VASP",
        "dftFunctional": "PBE",
        "miller_index": (None, None, None),
        "sites": None,
        "other_structure": [],
        "other_structure_energy": [],
        "bulk_structure": [],
        "bulk_structure_energy": [],
        "neb_structure": [],
        "neb_structure_energy": [],
    }

    for role in ["slab", "molecule", "adslab", "other"]:
        row[f"reactant_{role}"] = []
        row[f"reactant_{role}_energy"] = []
        row[f"product_{role}"] = []
        row[f"product_{role}_energy"] = []

    row["reactant_slab"].append(get_optimade_from_pymatgen(slab))
    row["reactant_slab_energy"].append(slab_energy)

    row["reactant_molecule"].append(get_optimade_from_pymatgen(molecule))
    row["reactant_molecule_energy"].append(molecule_energy)

    row["product_adslab"].append(get_optimade_from_pymatgen(adslab))
    row["product_adslab_energy"].append(adslab_energy)

    row["sid"] = data_row["sid"]

    return row


def oc20_dataset_to_df(dataset, mapping_pickle_path, save_csv_path):
    # Load mapping
    mapping = pickle.load(open(mapping_pickle_path, "rb"))
    mapping_df = pd.DataFrame(mapping)
    mapping_df["join_key"] = "random" + mapping_df["sid"].astype(str)

    rows = []
    for data in dataset:  # dataset is a list of OC20 Data objects
        row_dict = oc20_to_optimade_dicts(data)
        join_key = "random" + str(row_dict["sid"])
        row_dict["join_key"] = join_key
        rows.append(row_dict)
        

    oc20_df = pd.DataFrame(rows)

    # Merge with mapping
    merged = oc20_df.merge(mapping_df, on="join_key", how="left")
    merged.to_csv(save_csv_path, index=False)
    return merged


def load_metadata(downloaded_pkl_path):
    metadata = pickle.load(
        open(
            Path(downloaded_pkl_path),
            "rb",
        )
    )
    # Handle dict of dicts
    if isinstance(metadata, dict):
        df = pd.DataFrame.from_dict(metadata, orient="index").reset_index()
        df.rename(columns={"index": "sid"}, inplace=True)
    else:
        # Assume it's already a list of dicts
        df = pd.DataFrame(metadata)
    return df
