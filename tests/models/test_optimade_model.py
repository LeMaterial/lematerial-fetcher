import datetime

import pytest

from lematerial_fetcher.models.optimade import Functional, OptimadeStructure

# Test data for a valid structure
VALID_STRUCTURE_DATA = {
    "id": "test_id",
    "source": "test_source",
    "elements": ["Al", "O"],  # Alphabetically ordered
    "nelements": 2,
    "elements_ratios": [0.4, 0.6],  # Sum to 1.0
    "nsites": 2,
    "cartesian_site_positions": [[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]],
    "species_at_sites": ["Al", "O"],
    "species": [{"name": "Al"}, {"name": "O"}],
    "chemical_formula_anonymous": "A2B3",
    "chemical_formula_descriptive": "Al2O3",
    "chemical_formula_reduced": "Al2O3",
    "dimension_types": [1, 1, 1],
    "nperiodic_dimensions": 3,
    "lattice_vectors": [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
    "immutable_id": "test_immutable_id",
    "last_modified": datetime.datetime(2024, 1, 1, 12, 0, 0),
}


def test_valid_structure():
    """Test creation of a valid structure."""
    structure = OptimadeStructure(**VALID_STRUCTURE_DATA)
    assert structure.id == "test_id"
    assert structure.nelements == 2
    assert len(structure.cartesian_site_positions) == 2


def test_optional_fields():
    """Test structure creation with optional fields."""
    data = VALID_STRUCTURE_DATA.copy()
    data.update(
        {
            "stress_tensor": [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
            "energy": -100.0,
            "magnetic_moments": [0.0, 1.0],
            "forces": [[0.0, 0.0, 0.0], [0.1, 0.1, 0.1]],
            "total_magnetization": 1.0,
            "dos_ef": 0.5,
            "functional": Functional.PBE,
            "cross_compatibility": True,
            "entalpic_fingerprint": [1.0, 2.0, 3.0],
        }
    )
    structure = OptimadeStructure(**data)
    assert structure.energy == -100.0
    assert structure.functional == Functional.PBE


def test_invalid_elements_order():
    """Test validation of elements ordering."""
    data = VALID_STRUCTURE_DATA.copy()
    data["elements"] = ["O", "Al"]  # Not in alphabetical order
    with pytest.raises(
        ValueError,
        match=r"Elements must be in alphabetical order\. Current order: O, Al, Expected order: Al, O",
    ):
        OptimadeStructure(**data)


def test_invalid_elements_ratios_sum():
    """Test validation of elements_ratios sum."""
    data = VALID_STRUCTURE_DATA.copy()
    data["elements_ratios"] = [0.3, 0.3]  # Sum != 1.0
    with pytest.raises(
        ValueError, match=r"Sum of elements_ratios must be 1\.0 \(got 0\.600000\)"
    ):
        OptimadeStructure(**data)


def test_invalid_stress_tensor():
    """Test validation of stress tensor dimensions."""
    data = VALID_STRUCTURE_DATA.copy()
    data["stress_tensor"] = [[1.0, 0.0], [0.0, 1.0]]  # Not 3x3
    with pytest.raises(ValueError, match="Matrix must be a 3x3 matrix"):
        OptimadeStructure(**data)


def test_invalid_forces():
    """Test validation of forces dimensions."""
    data = VALID_STRUCTURE_DATA.copy()
    data["forces"] = [[1.0, 0.0], [0.0, 1.0]]  # Not 3D vectors
    with pytest.raises(ValueError, match="Vector must have exactly 3 components"):
        OptimadeStructure(**data)


def test_invalid_positions():
    """Test validation of cartesian positions dimensions."""
    data = VALID_STRUCTURE_DATA.copy()
    data["cartesian_site_positions"] = [[1.0, 0.0], [0.0, 1.0]]  # Not 3D vectors
    with pytest.raises(ValueError, match="Vector must have exactly 3 components"):
        OptimadeStructure(**data)


def test_inconsistent_site_counts():
    """Test validation of site count consistency."""
    data = VALID_STRUCTURE_DATA.copy()
    data["nsites"] = 3  # Doesn't match length of positions
    with pytest.raises(ValueError, match="List must have exactly 3 items"):
        OptimadeStructure(**data)


def test_invalid_date_format():
    """Test validation of last_modified date format."""
    data = VALID_STRUCTURE_DATA.copy()
    data["last_modified"] = "2024-13-13"  # Invalid format
    with pytest.raises(ValueError, match="Input should be a valid datetime"):
        OptimadeStructure(**data)


def test_empty_required_fields():
    """Test validation of empty required fields."""
    required_fields = [
        ("elements", [], "List should have at least 1 item after validation"),
        ("source", "", "String should have at least 1 character"),
        ("id", "", "String should have at least 1 character"),
        ("chemical_formula_anonymous", "", "String should have at least 1 character"),
        ("chemical_formula_descriptive", "", "String should have at least 1 character"),
        ("chemical_formula_reduced", "", "String should have at least 1 character"),
        ("immutable_id", "", "String should have at least 1 character"),
    ]

    for field, empty_value, error_msg in required_fields:
        data = VALID_STRUCTURE_DATA.copy()
        data[field] = empty_value
        with pytest.raises(ValueError, match=error_msg):
            OptimadeStructure(**data)


def test_invalid_dimension_types():
    """Test validation of dimension_types constraints."""
    data = VALID_STRUCTURE_DATA.copy()

    # Test too many dimensions
    data["dimension_types"] = [1, 1, 1, 1]
    with pytest.raises(
        ValueError, match="List should have at most 3 items after validation"
    ):
        OptimadeStructure(**data)

    # Test empty dimensions
    data["dimension_types"] = []
    with pytest.raises(
        ValueError, match="List should have at least 1 item after validation"
    ):
        OptimadeStructure(**data)


def test_invalid_nperiodic_dimensions():
    """Test validation of nperiodic_dimensions constraints."""
    data = VALID_STRUCTURE_DATA.copy()

    # Test negative value
    data["nperiodic_dimensions"] = -1
    with pytest.raises(ValueError, match="Input should be greater than or equal to 0"):
        OptimadeStructure(**data)

    # Test too many dimensions
    data["nperiodic_dimensions"] = 4
    with pytest.raises(ValueError, match="Input should be less than or equal to 3"):
        OptimadeStructure(**data)


def test_invalid_lattice_vectors():
    """Test validation of lattice_vectors constraints."""
    data = VALID_STRUCTURE_DATA.copy()

    # Test wrong number of vectors
    data["lattice_vectors"] = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]
    with pytest.raises(ValueError, match="Matrix must be a 3x3 matrix"):
        OptimadeStructure(**data)

    # Test wrong vector dimensions
    data["lattice_vectors"] = [[1.0, 0.0], [0.0, 1.0], [0.0, 0.0]]
    with pytest.raises(ValueError, match="Matrix must be a 3x3 matrix"):
        OptimadeStructure(**data)


def test_chemical_formula_reordering():
    """Test chemical formula reordering."""
    data = VALID_STRUCTURE_DATA.copy()

    # Test reordering by number (descending)
    data["chemical_formula_anonymous"] = "A2B5C3"
    structure = OptimadeStructure(**data)
    assert structure.chemical_formula_anonymous == "A5B3C2"


def test_cross_field_validation():
    """Test cross-field validation rules."""
    data = VALID_STRUCTURE_DATA.copy()

    # Test elements/ratios length mismatch
    data["elements_ratios"] = [0.4, 0.3, 0.3]
    with pytest.raises(
        ValueError,
        match=r"Number of elements \(\d+\) must match number of element ratios \(\d+\)",
    ):
        OptimadeStructure(**data)

    # Test species_at_sites length mismatch
    data = VALID_STRUCTURE_DATA.copy()
    data["species_at_sites"] = ["Al"]
    with pytest.raises(ValueError, match="List must have exactly 2 items"):
        OptimadeStructure(**data)

    # Test magnetic_moments length mismatch
    data = VALID_STRUCTURE_DATA.copy()
    data["magnetic_moments"] = [1.0]  # Should be length 2 to match nsites
    with pytest.raises(ValueError, match="List must have exactly 2 items"):
        OptimadeStructure(**data)


def test_optional_field_validation():
    """Test validation of optional field formats."""
    data = VALID_STRUCTURE_DATA.copy()

    # Test invalid stress tensor format
    data["stress_tensor"] = [[1.0, 0.0], [0.0, 1.0], [0.0, 0.0, 1.0]]
    with pytest.raises(ValueError, match="Matrix must be a 3x3 matrix"):
        OptimadeStructure(**data)

    # Test invalid forces format
    data = VALID_STRUCTURE_DATA.copy()
    data["forces"] = [[1.0, 0.0], [0.0, 1.0, 0.0]]  # Inconsistent dimensions
    with pytest.raises(
        ValueError, match="Invalid vector format: Vector must have exactly 3 components"
    ):
        OptimadeStructure(**data)

    # Test invalid magnetic moments (not matching nsites)
    data = VALID_STRUCTURE_DATA.copy()
    data["magnetic_moments"] = [1.0, 2.0, 3.0]  # Too many values for nsites=2
    with pytest.raises(ValueError, match="List must have exactly 2 items"):
        OptimadeStructure(**data)


def test_functional_enum():
    """Test validation of functional enum values."""
    data = VALID_STRUCTURE_DATA.copy()
    data["functional"] = "INVALID"  # Invalid functional
    with pytest.raises(ValueError, match=r"Input should be 'PBE', 'PBESOL' or 'SCAN'"):
        OptimadeStructure(**data)

    # Test valid functionals
    for func in [Functional.PBE, Functional.PBESOL, Functional.SCAN]:
        data["functional"] = func
        structure = OptimadeStructure(**data)
        assert structure.functional == func
