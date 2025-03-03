import datetime
import math
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class Functional(str, Enum):
    PBE = "PBE"
    PBESOL = "PBESOL"
    SCAN = "SCAN"


class OptimadeStructure(BaseModel):
    """
    An extended Pydantic model for an OPTIMADE-like structure object with
    improved validation checks.
    """

    # Basic fields
    id: str = Field(..., min_length=1)
    source: str = Field(..., min_length=1)

    # Primary structural fields
    elements: list[str] = Field(..., min_items=1)
    nelements: int = Field(..., gt=0)
    elements_ratios: list[float] = Field(..., min_items=1)

    # Sites
    nsites: int = Field(..., gt=0)
    cartesian_site_positions: list[list[float]] = Field(..., min_items=1)
    species_at_sites: list[str] = Field(..., min_items=1)
    species: list[dict] = Field(..., min_items=1)

    # Chemistry
    chemical_formula_anonymous: str = Field(..., min_length=1)
    chemical_formula_descriptive: str = Field(..., min_length=1)
    chemical_formula_reduced: str = Field(..., min_length=1)

    # Dimensionality
    dimension_types: list[int] = Field(..., min_items=1, max_items=3)
    nperiodic_dimensions: int = Field(..., ge=0, le=3)
    lattice_vectors: list[list[float]] = Field(..., min_items=3, max_items=3)

    # Misc
    immutable_id: str = Field(..., min_length=1)
    last_modified: datetime.datetime = Field(...)

    # Physical properties — these can be optional
    stress_tensor: Optional[list[list[float]]] = Field(None, min_items=3, max_items=3)
    energy: Optional[float] = Field(None)
    magnetic_moments: Optional[list[float]] = Field(None)
    forces: Optional[list[list[float]]] = Field(None)
    total_magnetization: Optional[float] = Field(None)
    dos_ef: Optional[float] = Field(None)
    functional: Optional[Functional] = Field(None)
    cross_compatibility: Optional[bool] = Field(None)
    entalpic_fingerprint: Optional[list[float]] = Field(None)

    #
    # Field-level validators
    #

    @field_validator("stress_tensor")
    @classmethod
    def validate_stress_tensor(cls, v):
        """
        Stress tensor must be a 3x3 matrix.
        """
        if v is None:  # Handle optional field
            return v
        if len(v) != 3 or any(len(row) != 3 for row in v):  # Check both dimensions
            raise ValueError("stress_tensor must be a 3x3 matrix.")
        return v

    @field_validator("cartesian_site_positions")
    @classmethod
    def validate_positions(cls, v):
        """
        Each position must be a 3D vector.
        """
        if len(v) < 1:  # Match min_items=1
            raise ValueError("cartesian_site_positions must not be empty.")
        if any(len(pos) != 3 for pos in v):
            raise ValueError("All cartesian_site_positions must be 3D vectors.")
        return v

    @field_validator("forces")
    @classmethod
    def validate_forces(cls, v):
        """
        Each force must be a 3D vector.
        """
        if v is None:  # Handle optional field
            return v
        if any(len(force) != 3 for force in v):
            raise ValueError("All forces must be 3D vectors.")
        return v

    @field_validator("elements_ratios")
    @classmethod
    def validate_sum_of_elements_ratios(cls, v):
        """
        For many OPTIMADE use cases, the sum of elements_ratios should be ~1.0.
        If that's not a requirement in your specific use case, you can remove
        or relax this check.
        """
        ratio_sum = sum(v)
        if not math.isclose(ratio_sum, 1.0, rel_tol=1e-5, abs_tol=1e-8):
            raise ValueError(
                f"Sum of elements_ratios must be close to 1.0 (got {ratio_sum})."
            )
        return v

    @field_validator("elements")
    @classmethod
    def validate_elements_order(cls, v):
        """
        Ensure elements are in alphabetical order.
        """
        if v != sorted(v):
            raise ValueError("Elements must be in alphabetical order.")
        return v

    @field_validator("chemical_formula_anonymous")
    @classmethod
    def validate_and_reorder_anonymous_formula(cls, v: str) -> str:
        """
        Reorder anonymous formula by descending numbers.
        Example: A2B2C5D12 → A12B5C2D2
        """
        # Split the formula into element-number pairs
        pairs = []
        current = ""
        for char in v:
            if char.isdigit():
                current += char
            else:
                if current:
                    pairs.append((pairs[-1][0], int(current)))
                    current = ""
                pairs.append((char, None))
        if current:
            pairs.append((pairs[-1][0], int(current)))
            pairs.pop(-2)

        # Convert None to 1 and sort by number (descending)
        pairs = [(elem, num if num is not None else 1) for elem, num in pairs]
        pairs.sort(key=lambda x: (-x[1], x[0]))

        # Reconstruct the formula
        result = ""
        for elem, num in pairs:
            result += elem
            if num > 1:
                result += str(num)

        return result

    @field_validator("last_modified")
    @classmethod
    def validate_date_format(cls, v: datetime.datetime) -> datetime.datetime:
        """
        Ensure the datetime is properly formatted.
        Example: 2023-11-16 06:57:59
        """
        try:
            # Convert to string in desired format to verify it matches
            formatted = v.strftime("%Y-%m-%d %H:%M:%S")
            # Parse back to datetime to ensure it's valid
            return datetime.datetime.strptime(formatted, "%Y-%m-%d %H:%M:%S")
        except (ValueError, AttributeError) as e:
            raise ValueError(
                "last_modified must be in format 'YYYY-MM-DD HH:MM:SS'"
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
        cartesian_site_positions = self.cartesian_site_positions
        species_at_sites = self.species_at_sites

        # Check elements and ratios consistency
        if len(elements) != len(elements_ratios):
            raise ValueError("elements and elements_ratios must have the same length.")
        if nelements != len(elements):
            raise ValueError("nelements must match the length of elements.")

        # Realign elements and ratios (maintaining alphabetical order)
        sorted_pairs = sorted(zip(elements, elements_ratios), key=lambda x: x[0])
        sorted_elements, sorted_ratios = zip(*sorted_pairs)
        self.elements = list(sorted_elements)
        self.elements_ratios = list(sorted_ratios)

        # Check nsites consistency
        if len(cartesian_site_positions) != nsites:
            raise ValueError("Length of cartesian_site_positions must match nsites.")
        if len(species_at_sites) != nsites:
            raise ValueError("Length of species_at_sites must match nsites.")

        return self
