# Copyright 2025 Entalpic
import datetime
import math
import re
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class Functional(str, Enum):
    PBE = "pbe"
    PBESOL = "pbesol"
    SCAN = "scan"


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
    source: str = Field(
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
    functional: Optional[Functional] = Field(
        None, description="Exchange-correlation functional"
    )
    cross_compatibility: Optional[bool] = Field(
        None, description="Cross-compatibility flag"
    )
    entalpic_fingerprint: Optional[list[float]] = Field(
        None,
        min_length=1,
        description="Entalpic fingerprint hash",
    )

    #
    # Field-level validators
    #

    def _validate_with_number_of_sites(self, v, nsites):
        if v is None:
            return v
        if len(v) != nsites:
            raise ValueError(f"List must have exactly {nsites} items")
        return v

    @field_validator("cartesian_site_positions", "forces", mode="before")
    @classmethod
    def validate_3d_vector(cls, v):
        try:
            if v is None:
                return v
            if any(len(row) != 3 for row in v):
                raise ValueError("Vector must have exactly 3 components")
            return v
        except Exception as e:
            raise ValueError(f"Invalid vector format: {e}") from e

    @field_validator("stress_tensor", "lattice_vectors", mode="before")
    @classmethod
    def validate_3x3_matrix(cls, v):
        if v is None:
            return v
        if len(v) != 3 or any(len(row) != 3 for row in v):
            raise ValueError("Matrix must be a 3x3 matrix")
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
                f"Sum of elements_ratios must be 1.0 (got {ratio_sum:.6f}). Each ratio represents the fraction of each element in the structure."
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
                f"Elements must be in alphabetical order. Current order: {', '.join(v)}, Expected order: {', '.join(sorted(v))}"
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
                "Anonymous formula must consist of capital letters with optional numbers. "
                f"Got: {v}"
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
                "last_modified must be in format 'YYYY-MM-DD'. "
                f"Got: {v}. Error: {str(e)}"
            ) from e

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
        if len(elements) != len(elements_ratios):
            raise ValueError(
                f"Number of elements ({len(elements)}) must match number of element ratios ({len(elements_ratios)})"
            )
        if nelements != len(elements):
            raise ValueError(
                f"nelements ({nelements}) must match the number of unique elements ({len(elements)})"
            )

        # Realign elements and ratios (maintaining alphabetical order)
        sorted_pairs = sorted(zip(elements, elements_ratios), key=lambda x: x[0])
        sorted_elements, sorted_ratios = zip(*sorted_pairs)
        self.elements = list(sorted_elements)
        self.elements_ratios = list(sorted_ratios)

        # Check nsites consistency
        self.cartesian_site_positions = self._validate_with_number_of_sites(
            self.cartesian_site_positions, nsites
        )
        self.species_at_sites = self._validate_with_number_of_sites(
            self.species_at_sites, nsites
        )
        self.forces = self._validate_with_number_of_sites(self.forces, nsites)
        self.magnetic_moments = self._validate_with_number_of_sites(
            self.magnetic_moments, nsites
        )

        return self
