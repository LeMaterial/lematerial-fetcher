# oc20_dataset_utils.py
import os
import tarfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Optional
from pymatgen.core import Structure
import numpy as np
from datasets import Dataset
import pandas as pd
import pickle
from ase import Atoms
from pymatgen.core import Composition

import lzma
import multiprocessing as mp
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from torch_geometric.data import Data

PathType = str | Path


from lematerial_fetcher.utils.logging import logger
from lematerial_fetcher.utils.structure import (
    get_optimade_from_pymatgen_oc20,
    get_optimade_from_atoms,
)

OC20_BASE_URL = "https://dl.fbaipublicfiles.com/opencatalystproject/data/is2res_train_val_test_lmdbs.tar.gz"
OC20_METADATA_URL = (
    "https://dl.fbaipublicfiles.com/opencatalystproject/data/oc20_data_mapping.pkl"
)
OC20_ADSLAB_SLAB_MAPPING_URL = (
    "https://dl.fbaipublicfiles.com/opencatalystproject/data/mapping_adslab_slab.pkl"
)
OC20_SLAB_URL = (
    "https://dl.fbaipublicfiles.com/opencatalystproject/data/slab_trajectories.tar"
)
OC20_ADSLAB_H_URL = (
    "https://dl.fbaipublicfiles.com/opencatalystproject/data/per_adsorbate_is2res/1.tar"
)


REF_ENERGIES = {
    "H": -3.477,
    "O": -7.204,
    "C": -7.282,
    "N": -8.083,
}


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
            import ssl

            ssl._create_default_https_context = ssl._create_unverified_context

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


def download_and_extract_tar(
    url: str,
    target_dir: Optional[str] = None,
    unlink: bool = False,
) -> str:
    """
    Download a .tar/.tar.gz/.tar.xz file and extract it.

    Parameters
    ----------
    url : str
        URL to download the tar archive from.
    target_dir : Optional[str]
        Directory to extract into. If None, uses current working directory.
    unlink : bool
        Whether to remove the downloaded archive after extraction.

    Returns
    -------
    str
        Path to the directory containing the extracted files.
    """
    if target_dir is None:
        target_dir = os.getcwd()
    os.makedirs(target_dir, exist_ok=True)

    # File to save archive
    archive_path = os.path.join(target_dir, os.path.basename(url))

    # Download with progress
    if not os.path.exists(archive_path):
        print(f"Downloading {url} to {archive_path}...")
        with tqdm(
            unit="B", unit_scale=True, desc=f"Downloading {os.path.basename(url)}"
        ) as pbar:

            def report_progress(block_num, block_size, total_size):
                if total_size > 0:
                    if block_num == 0:
                        pbar.total = total_size
                    pbar.update(block_size)

            urllib.request.urlretrieve(
                url, filename=archive_path, reporthook=report_progress
            )

    # Extract tar (auto-detect compression)
    print(f"Extracting {archive_path}...")
    with tarfile.open(archive_path, "r:*") as tar:
        tar.extractall(path=target_dir)

    # Optional cleanup
    if unlink:
        os.remove(archive_path)

    return target_dir


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


def ref_molecule_energy(
    chemical_formula: str, ref_energies: dict[str, float] = REF_ENERGIES
) -> float:
    """
    Compute the reference energy of a molecule from its descriptive formula,
    using a linear combination of atomic reference energies.

    Parameters
    ----------
    chemical_formula : str
        A chemical formula string (e.g. "H2O", "CH4", "NH3", etc.)
    ref_energies : dict[str, float]
        Reference atomic energies (in eV/atom)

    Returns
    -------
    float
        Total reference energy of the molecule (eV)
    """
    comp = Composition(chemical_formula)
    counts = {el: int(round(amt)) for el, amt in comp.get_el_amt_dict().items()}

    total_energy = 0.0
    for el, n in counts.items():
        if el not in ref_energies:
            raise ValueError(f"No reference energy defined for element {el}")
        total_energy += n * ref_energies[el]

    return total_energy


def get_structures(data_row):
    """Extract slab and adsorbate as pymatgen.Structure objects from OC20 row."""
    atomic_numbers = data_row["atomic_numbers"]
    initial_positions = data_row["pos"]
    final_positions = data_row["pos_relaxed"]
    tags = data_row["tags"]
    lattice = data_row["cell"]

    # Boolean masks
    # slab_mask = np.isin(tags, [0, 1])
    molecule_mask = tags == 2

    # Extract structures
    # slab = Structure(
    #     lattice=lattice,
    #     species=atomic_numbers[slab_mask],
    #     coords=final_positions[slab_mask],
    #     coords_are_cartesian=True,
    # )
    molecule = Structure(
        lattice=lattice,
        species=atomic_numbers[molecule_mask],
        coords=initial_positions[molecule_mask],
        coords_are_cartesian=True,
    )
    adslab = Structure(
        lattice=lattice,
        species=atomic_numbers,
        coords=final_positions,
        coords_are_cartesian=True,
    )
    return molecule, adslab


def data_to_row(data_row):
    molecule, adslab = get_structures(data_row)
    # molecule_energy = None
    # slab_energy = None
    # adslab_energy = None
    reaction_energy = data_row.get("y_relaxed", None)

    row = {
        "publication": "oc20",
        "reaction_energy": reaction_energy,
        "other_structure": [],
        "other_structure_energy": [],
    }

    for role in ["slab", "molecule", "adslab", "other"]:
        row[f"reactant_{role}"] = []
        row[f"reactant_{role}_energy"] = []
        row[f"product_{role}"] = []
        row[f"product_{role}_energy"] = []

    row["join_key"] = "random" + str(data_row["sid"])
    immutable_id = "oc20-" + str(data_row["sid"])

    # row["reactant_slab"].append(
    #     get_optimade_from_pymatgen_oc20(slab, role="slab", immutable_id=immutable_id)
    # )
    # row["reactant_slab_energy"].append(slab_energy)

    row["reactant_molecule"].append(
        get_optimade_from_pymatgen_oc20(
            molecule,
            role="molecule",
            immutable_id=immutable_id,
        )
    )

    molecule_energy = ref_molecule_energy(
        row["reactant_molecule"][0]["chemical_formula_descriptive"]
    )

    row["reactant_molecule_energy"].append(molecule_energy)

    row["product_adslab"].append(
        get_optimade_from_pymatgen_oc20(
            adslab,
            role="adslab",
            immutable_id=immutable_id,
        )
    )
    # row["product_adslab_energy"].append(adslab_energy)

    row["product_adslab"][0]["system_name"] = (
        row["reactant_molecule"][0]["chemical_formula_reduced"] + "star"
    )

    eq_left = row["reactant_molecule"][0]["system_name"] + " + " + "star"
    eq_right = row["product_adslab"][0]["system_name"]
    row["equation"] = f"{eq_left} -> {eq_right}"

    return row


def get_left_out_fields(oc_20_df_metadata, oc20_slab):

    oc_20_df_metadata["reactant_molecule"] = oc_20_df_metadata.apply(
        update_immutable_id_ads, axis=1
    )

    oc_20_df_metadata["miller_index"] = oc_20_df_metadata["miller_index"].apply(
        lambda x: (
            [int(i) for i in x] if isinstance(x, (tuple, list)) else [None, None, None]
        )
    )

    oc_20_df_metadata["sites"] = oc_20_df_metadata.apply(
        lambda row: {
            "shift": row["shift"],
            "top": row["top"],
            "sites_coords": row["adsorption_site"],
        },
        axis=1,
    )

    cols_to_drop = [
        "bulk_id",
        "ads_id",
        "bulk_mpid",
        "bulk_symbols",
        "ads_symbols",
        "class",
        "anomaly",
        "split",
        "shift",
        "top",
        "adsorption_site",
        "join_key",
    ]

    oc_20_df_metadata.drop(columns=cols_to_drop, inplace=True)

    oc_20_df_metadata["reactant_slab"] = oc_20_df_metadata.apply(
        lambda row: update_slab_immutable_id(row, oc20_slab), axis=1
    )

    oc_20_df_metadata["reactant_slab_energy"] = oc_20_df_metadata.apply(
        lambda row: get_slab_energy_from_mapping(row, oc20_slab), axis=1
    )

    # logger.info(f'reactant slab energy: {oc_20_df_metadata["reactant_slab_energy"]}')
    return oc_20_df_metadata


def update_immutable_id_ads(row):
    ads_id = row.get("ads_id")
    molecules = row.get("reactant_molecule", [])
    for struct in molecules:
        struct["immutable_id"] = (
            "oc20-" + str(ads_id) if ads_id is not None else struct.get("immutable_id")
        )
    return molecules


def update_slab_immutable_id(row, mapping_df):
    slab = row.get("reactant_slab", [])
    adslab = row.get("product_adslab", [])

    adslab_immutable_id = adslab[0]["immutable_id"]
    adslab_rid = int(adslab_immutable_id.replace("oc20-", ""))

    match = mapping_df[mapping_df["adslab_rid"] == adslab_rid]

    if not match.empty:
        atoms = match.iloc[0]["last_atoms"]
        if not isinstance(atoms, Atoms):
            # Skip this row if it's not a valid structure (nan)
            return slab

        slab_rid = "oc20-" + str(match.iloc[0]["slab_rid"])
        slab_structure = get_optimade_from_atoms(
            atoms=atoms, role="slab", name="star", immutable_id=slab_rid
        )
        slab.append(slab_structure)

    return slab


def get_slab_energy_from_mapping(row, mapping_df):
    reactant_slab_energy = row.get("reactant_slab_energy", [])
    adslab = row.get("product_adslab", [])

    adslab_immutable_id = adslab[0]["immutable_id"]
    adslab_rid = int(adslab_immutable_id.replace("oc20-", ""))

    match = mapping_df[mapping_df["adslab_rid"] == adslab_rid]

    if not match.empty:
        slab_energy = match.iloc[0]["slab_energy"]
        reactant_slab_energy.append(slab_energy)

    return reactant_slab_energy


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


def get_concatenated_df(output_dir):
    all_dfs = []

    for fname in os.listdir(output_dir):
        if fname.endswith(".pkl") and "concatenated" not in fname:
            full_path = os.path.join(output_dir, fname)
            try:
                df = pd.read_pickle(full_path)
                all_dfs.append(df)
            except Exception as e:
                print(f"Failed to read {fname}: {e}")

    if not all_dfs:
        logger.info("No valid .pkl files found.")
        return pd.DataFrame()

    combined_df = pd.concat(all_dfs, ignore_index=True)

    return combined_df


def upload_pkl_to_huggingface_dataset(pkl_path: str, dataset_name: str):
    """
    Convert a pickle file containing structures to OPTIMADE format and upload it to the Hugging Face Hub.

    Parameters
    ----------
    pkl_path : str
        Path to the pickle file containing the adsorption reaction dataset.
    dataset_name : str
        Name of the dataset on the Hugging Face Hub (e.g. "username/dataset_name").

    Returns
    -------
    None
    """
    df = pd.read_pickle(pkl_path)

    hf_dataset = Dataset.from_pandas(df)
    hf_dataset.push_to_hub(dataset_name)
