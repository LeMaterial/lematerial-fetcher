# Copyright 2025 Entalpic
import os
import io
import re
import tempfile
from datetime import datetime
from typing import Optional
import json

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


def parse_reactions(pub_ids):
    data = []

    for pub_id in pub_ids:
        print(f"Treating publication : {pub_id}")
        try:
            raw_reactions = reactions_from_dataset(pub_id)
            reactions = copy.deepcopy(raw_reactions)
            print("Number of reactions in publication :", len(reactions))
            aseify_reactions(reactions)
            # breakpoint()
            for r in reactions:
                reactants = list(json.loads(r["reactants"]).keys())
                products = list(json.loads(r["products"]).keys())
                system_names = list(r["reactionSystems"].keys())

                star_in_reactants = "star" in reactants

                adslab_key = next(
                    (name for name in products if "star" in name and name != "star"),
                    None,
                )

                gas_in_reactants = [name for name in reactants if "gas" in name.lower()]
                gas_in_products = [name for name in products if "gas" in name.lower()]
                has_one_gas_only_in_reactants = (
                    len(gas_in_reactants) == 1 and not gas_in_products
                )

                bulk_key = next(
                    (name for name in system_names if "bulk" in name.lower()), None
                )

                if star_in_reactants and adslab_key and has_one_gas_only_in_reactants:
                    slab_structure = AseAtomsAdaptor.get_structure(
                        r["reactionSystems"]["star"]["atoms"]
                    )
                    adslab_structure = AseAtomsAdaptor.get_structure(
                        r["reactionSystems"][adslab_key]["atoms"]
                    )
                    adsorbate_structure = AseAtomsAdaptor.get_structure(
                        r["reactionSystems"][gas_in_reactants[0]]["atoms"]
                    )

                    # if bulk_key is not None:
                    #     bulk_structure = AseAtomsAdaptor.get_structure(
                    #         r["reactionSystems"][bulk_key]["atoms"]
                    #     )

                    # else:
                    #     bulk_structure = primitive_cell(
                    #         r["reactionSystems"]["star"]["atoms"]
                    #     )

                    # # initialize the hasher
                    # emh = BAWLHasher()
                    # # get the hash
                    # bulk_hash = emh.get_material_hash(bulk_structure)
                    slab_energy = r["reactionSystems"]["star"]["energy"]
                    adslab_energy = r["reactionSystems"][adslab_key]["energy"]
                    adsorbate_energy = r["reactionSystems"][gas_in_reactants[0]][
                        "energy"
                    ]

                    facet = r.get("facet", "")
                    sites = r.get("sites", "")
                    reaction_energy = r.get("reactionEnergy", "")
                    activation_energy = r.get("activationEnergy", "")
                    dftCode = r.get("dftCode", "")
                    dftFunctional = r.get("dftFunctional", "")
                    data.append(
                        {
                            "publication": pub_id,
                            # "bulk_hash": bulk_hash,
                            "slab_structure": slab_structure,
                            "slab_energy": slab_energy,
                            "adsorbate_structure": adsorbate_structure,
                            "adsorbate_energy": adsorbate_energy,
                            "adslab_structure": adslab_structure,
                            "adslab_energy": adslab_energy,
                            "reaction_energy": reaction_energy,
                            "activation_energy": activation_energy,
                            "dftCode": dftCode,
                            "dftFunctional": dftFunctional,
                            "facet": facet,
                            "sites": sites,
                        }
                    )

        except Exception as e:
            print(f"Erreur avec {pub_id}: {e}")

    return data


def parse_reactions_surface(pub_ids):
    data = []

    for pub_id in pub_ids:
        print(f"Treating publication : {pub_id}", flush=True)
        try:
            raw_reactions = reactions_from_dataset(pub_id)
            reactions = copy.deepcopy(raw_reactions)
            print("Number of reactions in publication :", len(reactions), flush=True)
            aseify_reactions(reactions)

            for r in reactions:
                bulk_key = next(
                    (name for name in r["reactionSystems"] if "bulk" in name.lower()),
                    None,
                )
                if bulk_key is not None:
                    bulk_structure = AseAtomsAdaptor.get_structure(
                        r["reactionSystems"][bulk_key]["atoms"]
                    )
                    bulk_energy = r["reactionSystems"][bulk_key]["energy"]

                    # initialize the hasher
                    emh = BAWLHasher()
                    # get the hash
                    bulk_hash = emh.get_material_hash(bulk_structure)

                    if "star" in r["reactionSystems"]:
                        slab_structure = AseAtomsAdaptor.get_structure(
                            r["reactionSystems"]["star"]["atoms"]
                        )
                        slab_energy = r["reactionSystems"]["star"]["energy"]

                        facet = r.get("facet", "")
                        sites = r.get("sites", "")
                        dftCode = r.get("dftCode", "")
                        dftFunctional = r.get("dftFunctional", "")

                        data.append(
                            {
                                "publication": pub_id,
                                "bulk_structure": bulk_structure,
                                "bulk_energy": bulk_energy,
                                "bulk_hash": bulk_hash,
                                "slab_structure": slab_structure,
                                "slab_energy": slab_energy,
                                "dftCode": dftCode,
                                "dftFunctional": dftFunctional,
                                "facet": facet,
                                "sites": sites,
                            }
                        )

        except Exception as e:
            print(f"Erreur avec {pub_id}: {e}", flush=True)
    return data


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

                # print("reactant", reactants_dict)
                # print("products", products_dict)
                # print("other", other_dict)

                # Initialize row with lists and all expected keys
                row = {
                    "publication": pub_id,
                    "equation": r["Equation"],
                    "reaction_energy": r.get("reactionEnergy", ""),
                    "activation_energy": r.get("activationEnergy", ""),
                    "dftCode": r.get("dftCode", ""),
                    "dftFunctional": r.get("dftFunctional", ""),
                    "facet": r.get("facet", ""),
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
                        optimade_structure = get_optimade_from_atoms(atoms)
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
                        optimade_structure = get_optimade_from_atoms(atoms)
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
                        optimade_structure = get_optimade_from_atoms(atoms)
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


if __name__ == "__main__":
    pub_ids = ["JiangModelling2021"]

    name = "Hstar"
    print(get_system_role(name))
