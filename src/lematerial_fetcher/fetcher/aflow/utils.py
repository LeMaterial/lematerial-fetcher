# Copyright 2025 Entalpic
from datetime import datetime, timezone
from typing import Any, Optional

from pymatgen.core import Lattice, Structure

from lematerial_fetcher.models.optimade import Functional, OptimadeStructure
from lematerial_fetcher.utils.structure import get_optimade_from_pymatgen

# Properties requested from the AFLUX API for every entry.
# `geometry`, `species`, `composition` and `positions_fractional` are enough to
# rebuild the unit cell; the rest is provenance and target data.
# Properties marked required use the AFLUX existence filter `property(*)` so that
# entries missing them (e.g. some LIB3 entries) are excluded server-side instead of
# wasting pages that the transformer would have to skip.
AFLUX_REQUIRED_PROPERTIES = [
    "geometry",
    "species",
    "composition",
    "positions_fractional",
]
AFLUX_OPTIONAL_PROPERTIES = [
    "compound",
    "spacegroup_relax",
    "auid",
    "aurl",
    "catalog",
    "aflowlib_date",
    "energy_cell",
    # DFT provenance, kept in the raw table for a future energy-correction /
    # cross-compatibility audit (AFLOW runs GGA+U with its own U values, which
    # differ from the MP-2020 assumptions; see PR discussion).
    "dft_type",
    "ldau_type",
    "ldau_TLUJ",
    "species_pp_version",
    "spin_cell",
]


def build_aflux_query(base_url: str, page: int, per_page: int) -> str:
    """Build an AFLUX request URL for one page of entries (1-based pages).

    Parameters
    ----------
    base_url : str
        The AFLUX API base URL, e.g. ``http://aflow.org/API/aflux/``.
    page : int
        1-based page number.
    per_page : int
        Number of entries per page.

    Returns
    -------
    str
        The full request URL.
    """
    # $paging(0) is a magic value in AFLUX that returns the ENTIRE result set
    # in one response, so an off-by-one here would download the whole database.
    if page < 1:
        raise ValueError(
            f"AFLUX pages are 1-based (got {page}); "
            f"$paging(0) would return the entire result set"
        )
    properties = ",".join(
        [f"{p}(*)" for p in AFLUX_REQUIRED_PROPERTIES] + AFLUX_OPTIONAL_PROPERTIES
    )
    return f"{base_url}?{properties},$paging({page},{per_page})"


def parse_aflowlib_date(raw: Any) -> Optional[datetime]:
    """Parse an AFLOW ``aflowlib_date`` value (e.g. ``20200426_10:04:20_GMT-5``).

    The timezone suffix is ignored; day-level precision is enough for versioning.
    """
    if not raw:
        return None
    if isinstance(raw, list):
        raw = raw[-1] if raw else None
        if not raw:
            return None
    try:
        return datetime.strptime(str(raw)[:17], "%Y%m%d_%H:%M:%S")
    except ValueError:
        return None


def build_structure_from_aflux(attributes: dict[str, Any]) -> Structure:
    """Rebuild a pymatgen Structure from AFLUX geometry properties.

    - ``geometry``             : [a, b, c, alpha, beta, gamma]
    - ``species``              : unique elements, in POSCAR order
    - ``composition``          : per-species atom counts, aligned with ``species``
    - ``positions_fractional`` : fractional coordinates, in the same species order
    """
    a, b, c, alpha, beta, gamma = [float(x) for x in attributes["geometry"]]
    lattice = Lattice.from_parameters(a, b, c, alpha, beta, gamma)

    species = attributes["species"]
    if isinstance(species, str):
        species = [s for s in species.split(",") if s]

    composition = attributes["composition"]
    if isinstance(composition, str):
        composition = [int(float(x)) for x in composition.split(",") if x]
    else:
        composition = [int(float(x)) for x in composition]

    species_at_sites: list[str] = []
    for element, count in zip(species, composition):
        species_at_sites.extend([element] * count)

    positions = attributes["positions_fractional"]
    if len(species_at_sites) != len(positions):
        raise ValueError(
            f"Site count mismatch: sum(composition)={len(species_at_sites)} "
            f"!= len(positions_fractional)={len(positions)}"
        )

    return Structure(lattice, species_at_sites, positions, coords_are_cartesian=False)


def transform_aflux_entry(
    attributes: dict[str, Any], entry_id: str
) -> OptimadeStructure:
    """Transform one raw AFLUX entry into an OptimadeStructure.

    Parameters
    ----------
    attributes : dict
        The raw AFLUX entry (as stored in ``RawStructure.attributes``).
    entry_id : str
        The AFLOW ``auid`` (with or without the ``aflow:`` prefix).

    Returns
    -------
    OptimadeStructure
        The validated structure, with space group and BAWL fingerprint computed.
    """
    # AFLOW is not uniformly PBE: some entries (e.g. in the LIB catalogs) were
    # computed with LDA pseudopotentials. The Functional enum cannot represent
    # those, so refuse to label them PBE; the transformer will skip the row.
    dft_type = attributes.get("dft_type")
    if dft_type is not None:
        dft_types = dft_type if isinstance(dft_type, list) else [dft_type]
        if any(t != "PAW_PBE" for t in dft_types):
            raise ValueError(
                f"Unsupported dft_type {dft_types}: only PAW_PBE entries can be "
                f"mapped to functional=pbe"
            )

    structure = build_structure_from_aflux(attributes)
    optimade_fields = get_optimade_from_pymatgen(structure)

    auid = str(entry_id).replace("aflow:", "").strip()
    immutable_id = f"aflow-{auid}"

    energy = attributes.get("energy_cell")
    last_modified = parse_aflowlib_date(attributes.get("aflowlib_date"))

    return OptimadeStructure(
        **optimade_fields,
        id=f"{immutable_id}-{Functional.PBE.value}",
        immutable_id=immutable_id,
        source="aflow",
        functional=Functional.PBE,
        last_modified=last_modified or datetime.now(timezone.utc),
        # Conservative until AFLOW DFT settings (spin polarization, pseudopotentials)
        # are audited against the compatibility criteria used for MP/Alexandria/OQMD.
        cross_compatibility=False,
        energy=float(energy) if energy is not None else None,
        compute_space_group=True,
        compute_bawl_hash=True,
    )
