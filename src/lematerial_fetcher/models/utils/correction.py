# Copyright 2025 Entalpic
import json
import os

from pymatgen.core import Structure
from pymatgen.entries.compatibility import (
    ComputedStructureEntry,
    MaterialsProject2020Compatibility,
)

from lematerial_fetcher.models.utils.enums import Functional, Source
from lematerial_fetcher.utils.logging import logger

MPC = MaterialsProject2020Compatibility()


POTCAR_INFO = json.load(open(os.path.join(os.path.dirname(__file__), "potcar.json")))
U_VALUES = {
    "Co": 3.32,
    "Cr": 3.7,
    "Fe": 5.3,
    "Mn": 3.9,
    "Mo": 4.38,
    "Ni": 6.2,
    "V": 3.25,
    "W": 6.2,
}


def apply_mp_2020_energy_correction(
    structure: Structure,
    energy: float | None,
    functional: Functional,
    source: Source,
) -> float | None:
    """
    Apply the MP 2020 energy correction to the energy.

    Parameters
    ----------
    structure : Structure
        The structure to apply the correction to.
    energy : float | None
        The energy to apply the correction to.
    functional : Functional
        The functional to use for the correction.
    source : Source
        The source of the structure.

    Returns
    -------
    float | None
        The corrected energy.
    """

    if energy is None or functional != Functional.PBE:
        return energy

    elements = [e.name for e in structure.composition.elements]

    if any(element in ["Po", "At"] for element in elements):
        return None

    if source in [Source.MP, Source.OQMD] and "V" in elements:
        return None

    # Check if the structure contains O or F to use the correct U value
    hubbards = None
    if any(element in ["O", "F"] for element in elements):
        hubbards = {k: v for k, v in U_VALUES.items() if k in elements}

    potcar_sym = [
        POTCAR_INFO[element]
        for element in (set(elements) - set("V"))
        if element in POTCAR_INFO
    ]

    if source == Source.ALEXANDRIA and "V" in elements:
        potcar_sym.append("PAW_PBE V_sv 07Sep2000")

    try:
        cse = ComputedStructureEntry(
            structure,
            energy,
            parameters={
                "run_type": "GGA",
                "hubbards": hubbards,
                "potcar_symbols": potcar_sym,
            },
        )
        processed_cse = MPC.process_entry(cse)
        return processed_cse.energy if processed_cse else None
    except Exception as e:
        logger.warning(f"Failed to apply MP 2020 energy correction: {e}")
        return None
