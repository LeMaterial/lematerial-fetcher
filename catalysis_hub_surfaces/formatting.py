import os
import pandas as pd
import numpy as np
from pymatgen.analysis.structure_matcher import StructureMatcher
from pymatgen.core import Structure
from tqdm import tqdm
from datasets import Dataset
from lematerial_fetcher.utils.structure import get_optimade_from_pymatgen
from matid import Classifier, SymmetryAnalyzer
from pymatgen.io.ase import AseAtomsAdaptor
from material_hasher.hasher.bawl import BAWLHasher


def get_concatenated_df(directory: str, save_path: str = None) -> pd.DataFrame:
    """
    Reads and concatenates all .pkl DataFrames from a directory.

    Parameters
    ----------
    directory : str
        Path to the directory containing .pkl files.
    save_path : str, optional
        If provided, saves the combined DataFrame to this path.

    Returns
    -------
    pd.DataFrame
        The concatenated DataFrame.
    """
    all_dfs = []

    print(f"Scanning folder: {directory}")
    for fname in os.listdir(directory):
        if fname.endswith(".pkl"):
            full_path = os.path.join(directory, fname)
            print(f"Reading {full_path}")
            try:
                df = pd.read_pickle(full_path)
                all_dfs.append(df)
            except Exception as e:
                print(f"Failed to read {fname}: {e}")

    if not all_dfs:
        print("No valid .pkl files found.")
        return pd.DataFrame()

    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Total adsorption reactions: {len(combined_df)}")

    if save_path:
        combined_df.to_pickle(save_path)
        print(f"Saved combined DataFrame to {save_path}")

    return combined_df


def get_unique_slabs(
    df: pd.DataFrame, structure_column: str = "slab_structure"
) -> pd.DataFrame:
    matcher = StructureMatcher()
    unique_structures = []
    unique_rows = []

    for i, row in tqdm(df.iterrows(), total=len(df), desc="Checking unique slabs"):
        slab: Structure = row[structure_column]
        try:
            if not any(matcher.fit(slab, existing) for existing in unique_structures):
                unique_structures.append(slab)
                unique_rows.append(row)
        except ValueError as e:
            print(f"Skipping index {i} due to error: {e}")
            continue

    return pd.DataFrame(unique_rows)


def get_column_value_distribution(pickle_file_path: str, column_name: str):
    """
    Display the value distribution of a specified column from a pickled DataFrame.

    Parameters
    ----------
    pickle_file_path : str
        Path to the pickle file containing the DataFrame.
    column_name : str
        Name of the column for which to display the value distribution.

    Returns
    -------
    None
    """
    try:
        df = pd.read_pickle(pickle_file_path)
        print("File loaded successfully.")

        if column_name in df.columns:
            print(f"\nValue distribution for column '{column_name}':\n")
            print(df[column_name].value_counts())
        else:
            print(f"Column '{column_name}' does not exist in the DataFrame.")

    except Exception as e:
        print(f"Error loading file or accessing column: {e}")


def convert_structures_to_optimade(
    df: pd.DataFrame, structure_cols=None
) -> pd.DataFrame:
    """
    Convert specified structure columns in the DataFrame to OPTIMADE-compatible format.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the structure data.
    structure_cols : list of str, optional
        List of column names to convert. Defaults to ["slab_structure", "adsorbate_structure", "adslab_structure"].

    Returns
    -------
    pd.DataFrame
        DataFrame with structure columns converted to OPTIMADE format.
    """
    if structure_cols is None:
        structure_cols = ["slab_structure", "adsorbate_structure", "adslab_structure"]

    for col in structure_cols:
        if col in df.columns:
            print(f"Converting column '{col}' to OPTIMADE format...")
            df[col] = df[col].apply(get_optimade_from_pymatgen)
        else:
            print(f"Column '{col}' not found in DataFrame. Skipping...")

    return df


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
    print("Pickle file read to DataFrame. Number of rows:", len(df))

    hf_dataset = Dataset.from_pandas(df)
    print(f"Uploading dataset to Hugging Face Hub as '{dataset_name}'...")
    hf_dataset.push_to_hub(dataset_name)

    print("Dataset uploaded successfully.")


def primitive_cell(atoms):
    classifier = Classifier()
    classification = classifier.classify(atoms)
    # Visualize the cell that was found by matid
    prototype_cell = classification.prototype_cell

    # Visualize the corresponding conventional cell
    analyzer = SymmetryAnalyzer(prototype_cell, symmetry_tol=0.5)

    # Visualize the corresponding primitive cell
    prim_sys = analyzer.get_primitive_system()

    return AseAtomsAdaptor.get_structure(prim_sys)


def add_bawl_hash_column(
    pkl_path: str, structure_col: str = "slab_structure", hash_col: str = "BAWL_hash"
) -> pd.DataFrame:
    """
    Load a pickle file containing structures into a DataFrame and add a new column containing
    BAWL hashes of the primitive cells derived from the specified structure column.

    Parameters
    ----------
    pkl_path : str
        Path to the .pkl file containing a DataFrame with pymatgen Structure objects.
    structure_col : str, optional
        The name of the column containing the structure data. Defaults to "slab_structure".
    hash_col : str, optional
        The name of the new column to store BAWL hashes. Defaults to "BAWL_hash".

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the original data and a new column with BAWL hashes.
    """
    # Load the DataFrame
    df = pd.read_pickle(pkl_path)
    print("Pickle file read to DataFrame. Number of rows:", len(df))

    # Initialize the hasher
    hasher = BAWLHasher()

    def compute_hash(structure):
        try:
            structure = AseAtomsAdaptor.get_atoms(structure)

            prim = primitive_cell(structure)
            return hasher.get_material_hash(prim)
        except Exception as e:
            print(f"Failed to compute hash: {e}")
            return None

    # Compute hashes
    print(f"Computing BAWL hashes from column '{structure_col}'...")
    df[hash_col] = df[structure_col].apply(compute_hash)
    print(f"Added column '{hash_col}' with primitive cell hashes.")

    return df


def is_empty_or_na(x):
    if x is None:
        return True
    if isinstance(x, float) and pd.isna(x):
        return True
    if isinstance(x, (list, tuple)) and len(x) == 0:
        return True
    if isinstance(x, np.ndarray) and x.size == 0:
        return True
    return False


def adsorption_reactions_dataset(df_path: str, store_path: str):
    """
    Filters the dataset to retain only valid adsorption reactions of the form:
    slab + molecule → adslab, with all other roles empty.

    Parameters
    ----------
    df_path : str
        Path to the input pickle file containing all reactions.
    store_path : str
        Path where the filtered DataFrame will be stored.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame with only valid adsorption reactions.
    """
    df = pd.read_pickle(df_path)
    print("Pickle file read to DataFrame. Number of rows:", len(df))
    df_ads = df[
        df["publication"].notna()
        & df["equation"].notna()
        & df["reaction_energy"].notna()
        & df["reactant_slab"].apply(lambda x: isinstance(x, list) and len(x) == 1)
        & df["reactant_molecule"].apply(lambda x: isinstance(x, list) and len(x) == 1)
        & df["product_adslab"].apply(lambda x: isinstance(x, list) and len(x) == 1)
        & df["product_slab"].apply(is_empty_or_na)
        & df["product_molecule"].apply(is_empty_or_na)
        & df["reactant_adslab"].apply(is_empty_or_na)
        & df["reactant_other"].apply(is_empty_or_na)
        & df["product_other"].apply(is_empty_or_na)
    ]

    df_ads.to_pickle(store_path)
    print("Filtered DataFrame saved to:", store_path)

    return df_ads


if __name__ == "__main__":
    get_column_value_distribution(
        pickle_file_path="/lustre/catalysis-hub-surfaces/reaction_dataset/filtered_adsorption_reactions.pkl",
        column_name="publication",
    )
    # adsorption_reactions_dataset(
    #     df_path="/lustre/catalysis-hub-surfaces/reaction_dataset/concatenated_reactions_dataset.pkl",
    #     store_path="/lustre/catalysis-hub-surfaces/reaction_dataset/filtered_adsorption_reactions.pkl",
    # )

    # upload_pkl_to_huggingface_dataset(
    #     pkl_path="/lustre/catalysis-hub-surfaces/reaction_dataset/filtered_adsorption_reactions.pkl",
    #     dataset_name="Entalpic/Catalysis_Hub_adsorption_reactions_dataset",
    # )
