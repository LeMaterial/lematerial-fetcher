# Copyright 2025 Entalpic
import datetime
import math
import re
import warnings
from typing import Optional

import moyopy
import numpy as np
from material_hasher.hasher.entalpic import EntalpicMaterialsHasher
from moyopy.interface import MoyoAdapter
from pydantic import BaseModel, Field, field_validator, model_validator
from pymatgen.core import Element, Structure

from lematerial_fetcher.models.utils.correction import apply_mp_2020_energy_correction
from lematerial_fetcher.models.utils.enums import Functional, Source
from lematerial_fetcher.utils.logging import logger

# TODO(Ramlaoui, msiron): Take care of warnings in the hasher
warnings.filterwarnings("ignore")

SG_MOYOPY_SYMPREC = 1e-4

MAX_FORCE_EV_A = 0.1  # eV/Å


class OptimadeStructure(BaseModel):
    """
    An extended Pydantic model for an OPTIMADE-like structure object with
    improved validation checks.
    """

    # Basic fields
    id: str = Field(
        ...,
        min_length=1,
        description="Unique identifier for the structure",
    )
    source: Source = Field(
        ...,
        min_length=1,
        description="Source database of the structure",
    )

    # Primary structural fields
    elements: list[str] = Field(
        ...,
        min_length=1,
        description="List of chemical elements in alphabetical order",
    )
    nelements: int = Field(
        ...,
        gt=0,
        description="Number of different elements",
    )
    elements_ratios: list[float] = Field(
        ...,
        min_length=1,
        description="Ratios of elements (must sum to 1.0)",
    )

    # Sites
    nsites: int = Field(
        ...,
        gt=0,
        description="Number of sites",
    )
    cartesian_site_positions: list[list[float]] = Field(
        ...,
        min_length=1,
        description="List of 3D cartesian coordinates for each site",
    )
    species_at_sites: list[str] = Field(
        ...,
        min_length=1,
        description="Chemical species at each site",
    )
    species: list[dict] = Field(
        ...,
        min_length=1,
        description="Detailed information about chemical species",
    )

    # Chemistry
    chemical_formula_anonymous: str = Field(
        ...,
        min_length=1,
        description="Anonymous formula (e.g., A2B)",
    )
    chemical_formula_descriptive: str = Field(
        ...,
        min_length=1,
        description="Descriptive formula (e.g., H2O)",
    )
    chemical_formula_reduced: str = Field(
        ...,
        min_length=1,
        description="Reduced formula",
    )

    # Dimensionality
    dimension_types: list[int] = Field(
        ...,
        min_length=1,
        max_length=3,
        description="Dimensionality type for each direction",
    )
    nperiodic_dimensions: int = Field(
        ...,
        ge=0,
        le=3,
        description="Number of periodic dimensions",
    )
    lattice_vectors: list[list[float]] = Field(
        ...,
        min_length=3,
        max_length=3,
        description="3x3 matrix of lattice vectors",
    )

    # Misc
    immutable_id: str = Field(
        ...,
        min_length=1,
        description="Immutable identifier",
    )
    last_modified: datetime.datetime = Field(
        ..., description="Last modification timestamp"
    )

    # Physical properties — these can be optional
    stress_tensor: Optional[list[list[float]]] = Field(
        None,
        min_length=3,
        max_length=3,
        description="3x3 stress tensor matrix",
    )
    energy: Optional[float] = Field(
        None,
        description="Total energy in eV",
    )
    energy_corrected: Optional[float] = Field(
        None,
        description="Corrected energy in eV",
    )
    magnetic_moments: Optional[list[float]] = Field(
        None,
        min_length=1,
        description="Magnetic moments per site (must match number of sites when provided)",
    )
    forces: Optional[list[list[float]]] = Field(
        None,
        min_length=1,
        description="Forces on each site (must match number of sites when provided)",
    )
    total_magnetization: Optional[float] = Field(
        None,
        description="Total magnetization in μB",
    )
    dos_ef: Optional[float] = Field(
        None,
        description="Density of states at Fermi level",
    )
    charges: Optional[list[float]] = Field(
        None,
        min_length=1,
        description="Charges on each site",
    )
    band_gap_indirect: Optional[float] = Field(
        None,
        description="Indirect band gap in eV",
    )
    functional: Optional[Functional] = Field(
        None, description="Exchange-correlation functional"
    )
    cross_compatibility: bool = Field(description="Cross-compatibility flag")
    space_group_it_number: Optional[int] = Field(
        None,
        description="Space group international number",
    )
    bawl_fingerprint: Optional[str] = Field(
        None,
        min_length=1,
        description="BAWL fingerprint hash",
    )

    def __init__(
        self,
        compute_space_group: bool = True,
        compute_bawl_hash: bool = False,
        **kwargs,
    ):
        try:
            structure = Structure(
                species=kwargs["species_at_sites"],
                coords=kwargs["cartesian_site_positions"],
                lattice=kwargs["lattice_vectors"],
                coords_are_cartesian=True,
            )

            # Compute space group with moyopy
            if compute_space_group:
                cell = MoyoAdapter.from_structure(structure)
                dataset = moyopy.MoyoDataset(
                    cell=cell,
                    symprec=SG_MOYOPY_SYMPREC,
                    angle_tolerance=None,
                    setting=None,
                )
                space_group = dataset.number
                kwargs["space_group_it_number"] = space_group

            if compute_bawl_hash:
                kwargs["bawl_fingerprint"] = (
                    EntalpicMaterialsHasher().get_material_hash(structure)
                )

        except Exception as e:
            logger.warning(
                f"Failed to create pymatgen structure from {kwargs['immutable_id']}. Error: {e}"
            )

        super().__init__(**kwargs)

    #
    # Field-level validators
    #

    def _validate_with_number_of_sites(self, v, nsites, field_name=""):
        if v is None:
            return v
        if len(v) != nsites:
            raise ValueError(
                f"List {field_name} must have exactly {nsites} items. "
                f"Got {len(v)} items. Input value: {v}"
            )
        return v

    @field_validator("cartesian_site_positions", "forces", mode="before")
    @classmethod
    def validate_3d_vector(cls, v):
        try:
            if v is None:
                return v
            if any(len(row) != 3 for row in v):
                invalid_rows = [i for i, row in enumerate(v) if len(row) != 3]
                raise ValueError(
                    f"Each vector must have exactly 3 components. Found vectors with wrong dimensions at indices: {invalid_rows}. "
                    f"Expected format: [[x, y, z], ...], got: {v}"
                )
            return v
        except Exception as e:
            raise ValueError(
                f"Invalid vector format: {str(e)}. Input value: {v}"
            ) from e

    @field_validator("stress_tensor", "lattice_vectors", mode="before")
    @classmethod
    def validate_3x3_matrix(cls, v):
        if v is None:
            return v
        if len(v) != 3 or any(len(row) != 3 for row in v):
            raise ValueError(
                f"Matrix must be a 3x3 matrix. Got shape {len(v)}x{len(v[0]) if v else 0}. "
                f"Input value: {v}"
            )
        return v

    @field_validator("species_at_sites")
    @classmethod
    def validate_species_at_sites(cls, v):
        """
        Ensure that the species contain only valid elements.
        """
        if any(not Element.is_valid_symbol(element) for element in v):
            raise ValueError(
                f"Field species_at_sites must contain only valid elements. Got: {v}"
            )
        return v

    @field_validator("elements_ratios")
    @classmethod
    def validate_sum_of_elements_ratios(cls, v):
        """
        For many OPTIMADE use cases, the sum of elements_ratios should be ~1.0.
        """
        ratio_sum = sum(v)
        if not math.isclose(ratio_sum, 1.0, rel_tol=1e-5, abs_tol=1e-8):
            raise ValueError(
                f"Sum of elements_ratios must be 1.0 (got {ratio_sum:.6f}). "
                f"Current ratios: {v}. Each ratio represents the fraction of each element in the structure."
            )
        return v

    @field_validator("elements")
    @classmethod
    def validate_elements_order(cls, v):
        """
        Ensure elements are in alphabetical order.
        """
        if v != sorted(v):
            raise ValueError(
                f"Elements must be in alphabetical order. "
                f"Current order: {', '.join(v)}, "
                f"Expected order: {', '.join(sorted(v))}. "
                f"Please reorder the elements list."
            )
        return v

    @field_validator("chemical_formula_anonymous")
    @classmethod
    def validate_and_reorder_anonymous_formula(cls, v: str) -> str:
        """
        Reorder anonymous formula by descending numbers.
        Example: A2B2C5D12 → A12B5C2D2
        """
        # validate format (single uppercase letter followed by optional number)
        pattern = r"^[A-Z](?:\d+)?(?:[A-Z](?:\d+)?)*$"
        if not re.match(pattern, v):
            raise ValueError(
                "Invalid anonymous formula format. "
                "Formula must consist of capital letters with optional numbers (e.g., A2B3C). "
                f"Got: '{v}'. Please check for invalid characters or format."
            )

        # extract letter-number pairs
        pairs = [
            (m.group(1), int(m.group(2) if m.group(2) else 1))
            for m in re.finditer(r"([A-Z])(\d+)?", v)
        ]

        numbers = [x[1] for x in pairs]
        numbers.sort(reverse=True)
        pairs = [
            (chr(64 + i + 1), number) for i, number in enumerate(numbers)
        ]  # letters in alphabetical order

        return "".join(
            letter + (str(number) if number > 1 else "") for letter, number in pairs
        )

    @field_validator("chemical_formula_descriptive")
    @classmethod
    def validate_chemical_formula_descriptive(cls, v: str) -> str:
        """
        Ensure the chemical formula descriptive is properly formatted.
        Example: H2 O1 -> H2 O or Ce1 O1 -> Ce O
        """
        # Remove trailing numbers
        v = re.sub(r"([A-Z][a-z]?)1\b", r"\1", v)

        pattern = re.compile(
            r"^(?:[A-Z][a-z]?(?:[2-9]\d*|1\d+)?)(?:\s+[A-Z][a-z]?(?:[2-9]\d*|1\d+)?)*$"
        )
        if not pattern.match(v):
            raise ValueError(
                "Invalid descriptive formula format. "
                "Formula must consist of element symbols (capital letter + optional lowercase) "
                "with optional numbers, separated by spaces. "
                f"Got: '{v}'. Example of valid format: 'H2 O' or 'Fe2 O3'"
            )
        return v

    @field_validator("chemical_formula_reduced")
    @classmethod
    def validate_chemical_formula_reduced(cls, v: str) -> str:
        """
        Ensure the chemical formula reduced is properly formatted.
        Example: CsO9H7 is valid, Cs1O9 is not valid (no trailing ones)
        No parentheses are allowed in the formula.
        """
        # Check for parentheses
        if "(" in v or ")" in v:
            raise ValueError(
                f"Chemical formula reduced must not contain parentheses. Got: '{v}'. "
                "Please remove all parentheses from the formula."
            )

        # Check for any "1" in the formula (not just trailing ones)
        if re.search(r"([A-Z][a-z]?)1(?!\d)", v):
            matches = re.finditer(r"([A-Z][a-z]?)1(?!\d)", v)
            problematic_elements = [m.group(1) for m in matches]
            raise ValueError(
                f"Chemical formula reduced must not have ones (e.g., {', '.join(problematic_elements)}1). "
                f"Got: '{v}'. Remove the '1' subscripts or use proper stoichiometric numbers."
            )

        # Validate format (element symbols followed by optional numbers)
        pattern = re.compile(r"^(?:[A-Z][a-z]?(?:\d+)?)+$")
        if not pattern.match(v):
            raise ValueError(
                "Invalid reduced formula format. "
                "Formula must consist of element symbols followed by optional numbers. "
                f"Got: '{v}'. Example of valid format: 'Fe2O3' or 'NaCl'"
            )
        return v

    @field_validator("last_modified")
    @classmethod
    def validate_date_format(cls, v: datetime.datetime) -> datetime.datetime:
        """
        Ensure the datetime is properly formatted.
        Example: 2023-11-16 06:57:59
        """
        try:
            # Convert to string in desired format to verify it matches
            formatted = v.strftime("%Y-%m-%d")
            # Parse back to datetime to ensure it's valid
            return datetime.datetime.strptime(formatted, "%Y-%m-%d")
        except (ValueError, AttributeError) as e:
            raise ValueError(
                "Invalid date format for last_modified. "
                f"Got: {v}. Expected format: 'YYYY-MM-DD'. "
                f"Error details: {str(e)}"
            ) from e

    @field_validator("space_group_it_number")
    @classmethod
    def validate_space_group_it_number(cls, v: int) -> int:
        """
        Ensure the space group IT number is properly formatted.
        """
        if v is None:
            return v
        if v < 1 or v > 230:
            raise ValueError(
                f"Space group IT number must be between 1 and 230. Got: {v}"
            )
        return v

    @field_validator("dimension_types")
    @classmethod
    def validate_dimension_types(cls, v: list[int]) -> list[int]:
        """
        Ensure the dimension types are properly formatted.

        We should expect it to be [1, 1, 1] for any structure.
        """
        if v != [1, 1, 1]:
            raise ValueError(f"Field dimension_types must be [1, 1, 1]. Got: {v}")
        return v

    @field_validator("nperiodic_dimensions")
    @classmethod
    def validate_nperiodic_dimensions(cls, v: int) -> int:
        """
        Ensure the number of periodic dimensions is 3.
        """
        if v != 3:
            raise ValueError(f"Field nperiodic_dimensions must be 3. Got: {v}")
        return v

    @field_validator("forces")
    @classmethod
    def validate_forces_too_high(
        cls, v: list[list[float]] | None
    ) -> list[list[float]] | None:
        """
        Ensure the forces are not too high.
        """
        if v is None:
            return v
        max_force = max(np.linalg.norm(force) for force in v)
        if max_force > MAX_FORCE_EV_A:
            raise ValueError(
                f"Forces are too high. Maximum allowed force is {MAX_FORCE_EV_A} eV/Å. Got: {max_force}"
            )
        return v

    #
    # Cross-field validators
    #

    @model_validator(mode="after")
    def check_consistency(self):
        """
        A root validator that checks consistency among multiple fields.
        """
        elements = self.elements
        elements_ratios = self.elements_ratios
        nelements = self.nelements
        nsites = self.nsites

        # Check elements and ratios consistency
        if not (
            len(elements)
            == len(elements_ratios)
            == nelements
            == len(self.species)
            == len(self.chemical_formula_descriptive.split())
        ):
            raise ValueError(
                f"Number of elements ({len(elements)}) must match number of element ratios ({len(elements_ratios)}), "
                f"nelements ({nelements}), species ({len(self.species)}) and chemical formula descriptive "
                f"({len(self.chemical_formula_descriptive.split())})"
            )

        # Realign elements and ratios (maintaining alphabetical order)
        sorted_pairs = sorted(zip(elements, elements_ratios), key=lambda x: x[0])
        sorted_elements, sorted_ratios = zip(*sorted_pairs)
        self.elements = list(sorted_elements)
        self.elements_ratios = list(sorted_ratios)

        # Check nsites consistency
        self.cartesian_site_positions = self._validate_with_number_of_sites(
            self.cartesian_site_positions, nsites, "cartesian_site_positions"
        )
        self.species_at_sites = self._validate_with_number_of_sites(
            self.species_at_sites, nsites, "species_at_sites"
        )
        self.forces = self._validate_with_number_of_sites(self.forces, nsites, "forces")
        self.magnetic_moments = self._validate_with_number_of_sites(
            self.magnetic_moments, nsites, "magnetic_moments"
        )
        self.charges = self._validate_with_number_of_sites(
            self.charges, nsites, "charges"
        )

        #  Validation using the Pymatgen structure
        structure = Structure(
            self.lattice_vectors,
            self.species_at_sites,
            self.cartesian_site_positions,
            coords_are_cartesian=True,
        )

        # Apply the energy correction
        if self.energy_corrected is None:
            self.energy_corrected = apply_mp_2020_energy_correction(
                structure, self.energy, self.functional, self.source
            )

        return self


# TODO(Ramlaoui): Check that rows with MP match the API
