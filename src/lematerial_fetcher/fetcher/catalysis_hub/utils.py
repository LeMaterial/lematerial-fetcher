# Copyright 2025 Entalpic
import os
import io
import re
import tempfile
from datetime import datetime
from typing import Optional
import json
import pandas as pd
import numpy as np
from datasets import Dataset
from pathlib import Path

from lematerial_fetcher.database.mysql import MySQLDatabase, execute_sql_file
from lematerial_fetcher.utils.io import (
    download_file,
    get_page_content,
    list_download_links_from_page,
)
from lematerial_fetcher.utils.logging import logger

import copy
import ase.io
import ase.calculators.singlepoint
import requests
from ase.io import write
from material_hasher.hasher.bawl import BAWLHasher
from pymatgen.io.ase import AseAtomsAdaptor
from matid import Classifier, SymmetryAnalyzer
from lematerial_fetcher.utils.structure import (
    get_optimade_from_pymatgen,
    get_optimade_from_atoms,
)


GRAPHQL = "http://api.catalysis-hub.org/graphql"


def fetch(query):
    return requests.get(GRAPHQL, {"query": query}).json()["data"]


def fetch_all_pub_ids():
    pub_ids = []
    has_next_page = True
    start_cursor = ""
    page_size = 100

    while has_next_page:
        query = """{{
          publications(first: {page_size}, after: "{start_cursor}") {{
            pageInfo {{
              hasNextPage
              endCursor
            }}
            edges {{
              node {{
                pubId
              }}
            }}
          }}
        }}""".format(
            page_size=page_size, start_cursor=start_cursor
        )

        data = fetch(query)
        publications = data["publications"]
        for edge in publications["edges"]:
            pub_ids.append(edge["node"]["pubId"])
        has_next_page = publications["pageInfo"]["hasNextPage"]
        start_cursor = publications["pageInfo"]["endCursor"]

    return pub_ids


def reactions_from_dataset(pub_id, page_size=10):
    reactions = []
    seen_cursors = set()
    has_next_page = True
    start_cursor = ""
    page = 0

    while has_next_page:
        query = f"""{{ 
          reactions(pubId: "{pub_id}", first: {page_size}, after: "{start_cursor}") {{
            totalCount
            pageInfo {{
              hasNextPage
              endCursor
            }}
            edges {{
              node {{
                Equation
                surfaceComposition
                chemicalComposition
                facet
                reactionEnergy
                reactants
                products
                reactionSystems {{
                  name
                  systems {{
                    energy
                    InputFile(format: "json")
                  }}
                }}
              }}
            }}
          }}
        }}"""

        data = fetch(query)
        page_info = data["reactions"]["pageInfo"]
        edges = data["reactions"]["edges"]

        print(f"[Page {page}] cursor: {start_cursor}, edges: {len(edges)}")
        if not edges or start_cursor in seen_cursors:
            print("Stuck or empty page — stopping pagination.")
            break

        seen_cursors.add(start_cursor)
        start_cursor = page_info["endCursor"]
        has_next_page = page_info["hasNextPage"]
        page += 1

        reactions.extend(edge["node"] for edge in edges)

    return reactions


def aseify_reactions(reactions):
    for i, reaction in enumerate(reactions):
        for j, _ in enumerate(reaction["reactionSystems"]):
            with io.StringIO() as tmp_file:
                system = reaction["reactionSystems"][j].pop("systems")
                energy = system.pop("energy")
                tmp_file.write(system.pop("InputFile"))
                tmp_file.seek(0)
                atoms = ase.io.read(tmp_file, format="json")
            calculator = ase.calculators.singlepoint.SinglePointCalculator(
                atoms, energy=energy
            )
            atoms.calc = calculator
            reaction["reactionSystems"][j]["atoms"] = atoms
            reaction["reactionSystems"][j]["energy"] = energy
        reaction["reactionSystems"] = {
            x["name"]: {"atoms": x["atoms"], "energy": x["energy"]}
            for x in reaction["reactionSystems"]
        }


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


def get_system_role(name: str) -> str:
    """
    Returns the role of the system based on its name.
    'star'       -> slab
    contains 'star' and != 'star' -> adslab
    contains 'gas'                -> molecule
    else                          -> other
    """
    name_lower = name.lower()
    if name_lower == "star":
        return "slab"
    elif "gas" in name_lower:
        return "molecule"
    elif "star" in name_lower and name_lower != "star":
        return "adslab"
    else:
        return "other"


def parse_reactions_with_roles(pub_ids):
    """
    Parses reactions from the given publication IDs and assigns system roles
    ('slab', 'molecule', 'adslab', 'other') to reactants and products.
    Supports multiple systems per role and ensures all columns exist for each row.

    Returns
    -------
    list of dict
        Each dictionary contains structured data about a reaction, including the roles
        of reactant and product systems and their corresponding structures and energies.
    """
    data = []

    for pub_id in pub_ids:
        print(f"Treating publication: {pub_id}")
        try:
            raw_reactions = reactions_from_dataset(pub_id)
            reactions = copy.deepcopy(raw_reactions)
            print("Number of reactions in publication:", len(reactions))
            aseify_reactions(reactions)

            for r in reactions:
                # print("equation:", r["Equation"])
                reactants_dict = json.loads(r["reactants"])
                products_dict = json.loads(r["products"])
                systems = r["reactionSystems"]

                reactants_dict = {
                    name: coeff
                    for name, coeff in reactants_dict.items()
                    if name in systems
                }
                products_dict = {
                    name: coeff
                    for name, coeff in products_dict.items()
                    if name in systems
                }
                other_dict = {
                    name: systems[name]
                    for name in systems
                    if name not in reactants_dict and name not in products_dict
                }

                facet_raw = r.get("facet", "")
                miller_index = [None, None, None]
                if isinstance(facet_raw, str) and facet_raw.isdigit():
                    for i, char in enumerate(facet_raw[:3]):
                        miller_index[i] = int(char)

                # Initialize row with lists and all expected keys
                row = {
                    "publication": pub_id,
                    "equation": r["Equation"],
                    "reaction_energy": r.get("reactionEnergy", ""),
                    "activation_energy": r.get("activationEnergy", ""),
                    "miller_index": miller_index,
                    "sites": r.get("sites", ""),
                    "other_structure": [],
                    "other_structure_energy": [],
                }

                for role in ["slab", "molecule", "adslab", "other"]:
                    row[f"reactant_{role}"] = []
                    row[f"reactant_{role}_energy"] = []
                    row[f"product_{role}"] = []
                    row[f"product_{role}_energy"] = []

                # Process reactants
                for name in reactants_dict:
                    system = r["reactionSystems"][name]
                    atoms = system.get("atoms")
                    energy = system.get("energy")
                    try:
                        optimade_structure = get_optimade_from_atoms(atoms, role=role)

                    except Exception as e:
                        print(
                            f"Failed to convert atoms to optimade for system '{name}': {e}"
                        )
                        optimade_structure = None
                    role = get_system_role(name)
                    row[f"reactant_{role}"].append(optimade_structure)
                    row[f"reactant_{role}_energy"].append(energy)

                # Process products
                for name in products_dict:
                    system = r["reactionSystems"][name]
                    atoms = system.get("atoms")
                    energy = system.get("energy")
                    try:
                        optimade_structure = get_optimade_from_atoms(atoms, role=role)

                    except Exception as e:
                        print(
                            f"Failed to convert atoms to optimade for system '{name}': {e}"
                        )
                        optimade_structure = None

                    role = get_system_role(name)
                    row[f"product_{role}"].append(optimade_structure)
                    row[f"product_{role}_energy"].append(energy)

                # Process other
                for name in other_dict:
                    system = r["reactionSystems"][name]
                    atoms = system.get("atoms")
                    energy = system.get("energy")
                    try:
                        optimade_structure = get_optimade_from_atoms(
                            atoms, role="other"
                        )

                    except Exception as e:
                        print(
                            f"Failed to convert atoms to optimade for system '{name}': {e}"
                        )
                        optimade_structure = None

                    row["other_structure"].append(optimade_structure)
                    row["other_structure_energy"].append(energy)

                data.append(row)

        except Exception as e:
            print(f"Error with {pub_id}: {e}")

    return data


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
    return df_ads


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


if __name__ == "__main__":
    pub_ids = ["JiangModelling2021"]

    name = "Hstar"
    print(get_system_role(name))
