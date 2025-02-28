import numpy as np
from pymatgen.core import Structure

from material_fetcher.model.models import RawStructure
from material_fetcher.model.optimade import OptimadeStructure


def filter_mp_structure(raw_structure: RawStructure) -> bool:
    """
    Filter a raw Materials Project structure based on whether they have to be included in the
    database.

    This function defines the criteria for including a structure in the database.

    Parameters
    ----------
    raw_structure : RawStructure
        The raw Materials Project structure to filter.

    Returns
    -------
    bool
        True if the structure should be included in the database, False otherwise.
    """
    # TODO(ramlaoui): Implement the filter logic
    breakpoint()
    return True


def transform_mp_structure(raw_structure: RawStructure) -> OptimadeStructure:
    """
    Transform a raw Materials Project structure into an OptimadeStructure.

    This function takes a RawStructure object and performs the necessary transformations
    to convert it into an OptimadeStructure object.

    Parameters
    ----------
    raw_structure : RawStructure
        The raw Materials Project structure to transform.

    Returns
    -------
    OptimadeStructure
        The transformed OptimadeStructure object.
    """
    pmg_structure = Structure.from_dict(raw_structure.attributes["structure"])

    # TODO(ramlaoui): This does not handle with disordered structures

    species_at_sites = [str(site.specie) for site in pmg_structure.sites]
    cartesian_site_positions = pmg_structure.cart_coords.tolist()
    lattice_vectors = pmg_structure.lattice.matrix.tolist()

    chemical_formula_reduced_dict = raw_structure.attributes["composition_reduced"]
    chemical_formula_reduced_elements = list(chemical_formula_reduced_dict.keys())
    chemical_formula_reduced_ratios = list(chemical_formula_reduced_dict.values())
    chemical_formula_reduced_ratios = [
        str(ratio) if ratio != 1 else "" for ratio in chemical_formula_reduced_ratios
    ]
    chemical_formula_reduced_elements_alphabet_sorted = np.argsort(
        np.array(chemical_formula_reduced_elements)
    )
    chemical_formula_reduced = "".join(
        [
            chemical_formula_reduced_elements[i]
            + str(chemical_formula_reduced_ratios[i])
            for i in chemical_formula_reduced_elements_alphabet_sorted
        ]
    )

    chemical_formula_reduced_ratios = list(chemical_formula_reduced_dict.values())
    element_ratios = [
        chemical_formula_reduced_ratios[i] / sum(chemical_formula_reduced_ratios)
        for i in chemical_formula_reduced_elements_alphabet_sorted
    ]

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
        for element in raw_structure.attributes["elements"]
    ]

    optimade_structure = OptimadeStructure(
        # Basic fields
        id=raw_structure.attributes["material_id"],
        source="mp",
        immutable_id=raw_structure.attributes["material_id"],
        # Structural fields
        elements=raw_structure.attributes["elements"],
        nelements=raw_structure.attributes["nelements"],
        elements_ratios=element_ratios,
        # sites
        nsites=raw_structure.attributes["nsites"],
        cartesian_site_positions=cartesian_site_positions,
        species_at_sites=species_at_sites,
        species=species,
        # chemistry
        chemical_formula_anonymous=raw_structure.attributes["formula_anonymous"],
        chemical_formula_descriptive=raw_structure.attributes["formula_pretty"],
        chemical_formula_reduced=chemical_formula_reduced,
        # dimensionality
        dimension_types=[1, 1, 1],
        nperiodic_dimensions=3,
        lattice_vectors=lattice_vectors,
        # misc
        last_modified=raw_structure.attributes["builder_meta"]["build_date"]["$date"],
    )

    return optimade_structure
