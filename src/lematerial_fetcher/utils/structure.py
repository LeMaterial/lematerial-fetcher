import numpy as np
from pymatgen.core import Composition, Structure


def get_element_ratios_from_composition_reduced(
    composition_reduced: dict[str, float],
) -> list[float]:
    elements = list(composition_reduced.keys())
    ratios = list(composition_reduced.values())
    ratios = [int(ratio) for ratio in ratios]

    element_ratios = [ratios[i] / sum(ratios) for i in np.argsort(elements)]
    return element_ratios


def get_composition_reduced_from_reduced_dict(reduced_dict: dict[str, float]) -> dict:
    """
    Extracts the composition from a reduced dictionary.
    """
    items_reduced = [
        f"{element}{int(reduced_dict[element])}"
        if int(reduced_dict[element]) > 1
        else element
        for element in sorted(list(reduced_dict.keys()))  # alphabetical order
    ]
    chemical_formula_reduced = "".join(items_reduced)
    return chemical_formula_reduced


def get_composition_reduced_from_descriptive_formula(batch):
    for i in range(len(batch["chemical_formula_descriptive"])):
        composition = Composition(batch["chemical_formula_descriptive"][i])
        batch["chemical_formula_reduced"][i] = (
            get_composition_reduced_from_reduced_dict(composition.to_reduced_dict)
        )
    return batch


def get_optimade_from_pymatgen(structure: Structure) -> dict:
    """
    Extracts the possible fields from a pymatgen Structure object
    that are compatible with the OPTIMADE schema.

    Parameters
    ----------
    structure : Structure
        A pymatgen Structure object

    Returns
    -------
    dict
        An OPTIMADE-compliant dictionary containing partial structure data
        necessary to create an OptimadeStructure object.
    """
    # Basic chemistry fields
    elements = sorted(list(set(str(site.specie) for site in structure.sites)))
    # Note that this function returns a different result than the composition.to_reduced_dict method of pymatgen
    reduced_dict = structure.composition.to_reduced_dict
    elements_ratios = get_element_ratios_from_composition_reduced(reduced_dict)

    # Formula fields
    chemical_formula_reduced = get_composition_reduced_from_reduced_dict(reduced_dict)
    chemical_formula_anonymous = structure.composition.anonymized_formula
    # TODO(Ramlaoui): Maybe we should use the factor here?
    chemical_formula_descriptive = structure.composition.formula

    # Site and position data
    cartesian_site_positions = structure.cart_coords.tolist()
    species_at_sites = [str(site.specie) for site in structure.sites]
    species = [
        {
            "mass": None,
            "name": element,
            "attached": None,
            "nattached": None,
            "concentration": [1],
            "original_name": None,
            "chemical_symbols": [element],
        }
        for element in elements
    ]

    # Structure metadata
    nsites = len(structure.sites)
    nelements = len(elements)

    # Lattice and dimensionality
    lattice_vectors = structure.lattice.matrix.tolist()
    nperiodic_dimensions = 3  # Assuming 3D structure, adjust if needed
    dimension_types = [1, 1, 1]  # Assuming 3D periodic, adjust if needed

    return {
        # Required fields
        "elements": elements,
        "nelements": nelements,
        "elements_ratios": elements_ratios,
        "nsites": nsites,
        "cartesian_site_positions": cartesian_site_positions,
        "species_at_sites": species_at_sites,
        "species": species,
        "chemical_formula_anonymous": chemical_formula_anonymous,
        "chemical_formula_descriptive": chemical_formula_descriptive,
        "chemical_formula_reduced": chemical_formula_reduced,
        "dimension_types": dimension_types,
        "nperiodic_dimensions": nperiodic_dimensions,
        "lattice_vectors": lattice_vectors,
    }


def stress_matrix_from_voigt_6_stress(voigt_6_stress: list[float]) -> list[float]:
    """
    Convert a 6-element voigt notation stress tensor to a full 3x3 stress matrix.

    The voigt notation stress tensor is defined as:
    [sigma_xx, sigma_yy, sigma_zz, sigma_yz, sigma_xz, sigma_xy]

    The full 3x3 stress matrix is defined as:
    [
        [sigma_xx, sigma_xy, sigma_xz],
        [sigma_xy, sigma_yy, sigma_yz],
        [sigma_xz, sigma_yz, sigma_zz],
    ]

    Parameters
    ----------
    voigt_6_stress : list[float]
        The 6-element voigt notation stress tensor

    Returns
    -------
    list[float]
        The full 3x3 stress matrix
    """
    return [
        [voigt_6_stress[0], voigt_6_stress[5], voigt_6_stress[4]],
        [voigt_6_stress[5], voigt_6_stress[1], voigt_6_stress[3]],
        [voigt_6_stress[4], voigt_6_stress[3], voigt_6_stress[2]],
    ]
