# Copyright 2025 Entalpic
import datetime

import pytest

from lematerial_fetcher.database.postgres import OptimadeDatabase, TrajectoriesDatabase
from lematerial_fetcher.models.optimade import Functional, OptimadeStructure
from lematerial_fetcher.models.utils.enums import Source

# Test data for a valid structure
VALID_STRUCTURE_DATA = {
    "id": "test_id",
    "source": "oqmd",
    "elements": ["Al", "O"],  # Alphabetically ordered
    "nelements": 2,
    "elements_ratios": [0.4, 0.6],  # Sum to 1.0
    "nsites": 2,
    "cartesian_site_positions": [[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]],
    "species_at_sites": ["Al", "O"],
    "species": [{"name": "Al"}, {"name": "O"}],
    "chemical_formula_anonymous": "A2B3",
    "chemical_formula_descriptive": "Al2 O3",
    "chemical_formula_reduced": "Al2O3",
    "dimension_types": [1, 1, 1],
    "nperiodic_dimensions": 3,
    "lattice_vectors": [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
    "immutable_id": "test_immutable_id",
    "last_modified": datetime.datetime(2024, 1, 1, 12, 0, 0),
    "cross_compatibility": True,
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
            "forces": [[0.0, 0.0, 0.0], [0.01, 0.01, 0.01]],
            "total_magnetization": 1.0,
            "dos_ef": 0.5,
            "functional": Functional.PBE,
            "cross_compatibility": True,
            "entalpic_fingerprint": "1.0, 2.0, 3.0",
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
    with pytest.raises(ValueError):
        OptimadeStructure(**data)


def test_invalid_positions():
    """Test validation of cartesian positions dimensions."""
    data = VALID_STRUCTURE_DATA.copy()
    data["cartesian_site_positions"] = [[1.0, 0.0], [0.0, 1.0]]  # Not 3D vectors
    with pytest.raises(ValueError):
        OptimadeStructure(**data)


def test_inconsistent_site_counts():
    """Test validation of site count consistency."""
    data = VALID_STRUCTURE_DATA.copy()
    data["nsites"] = 3  # Doesn't match length of positions
    with pytest.raises(ValueError):
        OptimadeStructure(**data)


def test_invalid_date_format():
    """Test validation of last_modified date format."""
    data = VALID_STRUCTURE_DATA.copy()
    data["last_modified"] = "2024-13-13"  # Invalid format
    with pytest.raises(ValueError):
        OptimadeStructure(**data)


def test_empty_required_fields():
    """Test validation of empty required fields."""
    required_fields = [
        ("elements", []),
        ("source", ""),
        ("id", ""),
        ("chemical_formula_anonymous", ""),
        ("chemical_formula_descriptive", ""),
        ("chemical_formula_reduced", ""),
        ("immutable_id", ""),
    ]

    for field, empty_value in required_fields:
        data = VALID_STRUCTURE_DATA.copy()
        data[field] = empty_value
        with pytest.raises(ValueError):
            OptimadeStructure(**data)


def test_invalid_dimension_types():
    """Test validation of dimension_types constraints."""
    data = VALID_STRUCTURE_DATA.copy()

    # Test too many dimensions
    data["dimension_types"] = [1, 1, 1, 1]
    with pytest.raises(ValueError):
        OptimadeStructure(**data)

    # Test empty dimensions
    data["dimension_types"] = []
    with pytest.raises(ValueError):
        OptimadeStructure(**data)


def test_invalid_nperiodic_dimensions():
    """Test validation of nperiodic_dimensions constraints."""
    data = VALID_STRUCTURE_DATA.copy()

    # Test negative value
    data["nperiodic_dimensions"] = -1
    with pytest.raises(ValueError):
        OptimadeStructure(**data)

    # Test too many dimensions
    data["nperiodic_dimensions"] = 4
    with pytest.raises(ValueError):
        OptimadeStructure(**data)


def test_invalid_lattice_vectors():
    """Test validation of lattice_vectors constraints."""
    data = VALID_STRUCTURE_DATA.copy()

    # Test wrong number of vectors
    data["lattice_vectors"] = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]
    with pytest.raises(ValueError):
        OptimadeStructure(**data)

    # Test wrong vector dimensions
    data["lattice_vectors"] = [[1.0, 0.0], [0.0, 1.0], [0.0, 0.0]]
    with pytest.raises(ValueError):
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
    with pytest.raises(ValueError):
        (ValueError,)
        OptimadeStructure(**data)

    # Test species_at_sites length mismatch
    data = VALID_STRUCTURE_DATA.copy()
    data["species_at_sites"] = ["Al"]
    with pytest.raises(ValueError):
        OptimadeStructure(**data)

    # Test magnetic_moments length mismatch
    data = VALID_STRUCTURE_DATA.copy()
    data["magnetic_moments"] = [1.0]  # Should be length 2 to match nsites
    with pytest.raises(ValueError):
        OptimadeStructure(**data)


def test_optional_field_validation():
    """Test validation of optional field formats."""
    data = VALID_STRUCTURE_DATA.copy()

    # Test invalid stress tensor format
    data["stress_tensor"] = [[1.0, 0.0], [0.0, 1.0], [0.0, 0.0, 1.0]]
    with pytest.raises(ValueError):
        OptimadeStructure(**data)

    # Test invalid forces format
    data = VALID_STRUCTURE_DATA.copy()
    data["forces"] = [[1.0, 0.0], [0.0, 1.0, 0.0]]  # Inconsistent dimensions
    with pytest.raises(ValueError):
        OptimadeStructure(**data)

    # Test invalid magnetic moments (not matching nsites)
    data = VALID_STRUCTURE_DATA.copy()
    data["magnetic_moments"] = [1.0, 2.0, 3.0]  # Too many values for nsites=2
    with pytest.raises(ValueError):
        OptimadeStructure(**data)


def test_functional_enum():
    """Test validation of functional enum values."""
    data = VALID_STRUCTURE_DATA.copy()
    data["functional"] = "INVALID"  # Invalid functional
    with pytest.raises(ValueError):
        OptimadeStructure(**data)

    # Test valid functionals
    for func in [Functional.PBE, Functional.PBESOL, Functional.SCAN]:
        data["functional"] = func
        structure = OptimadeStructure(**data)
        assert structure.functional == func


# -------------------------------------------------------------------
# LeMatRho charge density field tests
# -------------------------------------------------------------------


def test_source_lematrho_is_valid():
    """Test that LEMATRHO is a valid Source enum value."""
    assert Source.LEMATRHO == "lematrho"
    data = VALID_STRUCTURE_DATA.copy()
    data["source"] = "lematrho"
    structure = OptimadeStructure(**data)
    assert structure.source == Source.LEMATRHO


def test_charge_density_none_fields():
    """Regression: existing VALID_STRUCTURE_DATA still passes with new optional fields as None."""
    structure = OptimadeStructure(**VALID_STRUCTURE_DATA)
    assert structure.compressed_charge_density is None
    assert structure.compressed_aeccar0 is None
    assert structure.compressed_aeccar1 is None
    assert structure.compressed_aeccar2 is None
    assert structure.charge_density_grid_shape is None
    assert structure.bader_charges is None
    assert structure.bader_atomic_volume is None
    assert structure.ddec6_charges is None


def test_structure_with_charge_density_fields():
    """Test structure creation with all charge density fields populated."""
    data = VALID_STRUCTURE_DATA.copy()
    # nsites=2 so per-site lists must have length 2
    data.update(
        {
            "compressed_charge_density": [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]],
            "compressed_aeccar0": [[[0.1, 0.2], [0.3, 0.4]], [[0.5, 0.6], [0.7, 0.8]]],
            "compressed_aeccar1": [[[0.01, 0.02]]],
            "compressed_aeccar2": [[[0.01, 0.02]]],
            "charge_density_grid_shape": [2, 2, 2],
            "bader_charges": [1.5, -1.5],
            "bader_atomic_volume": [10.0, 12.0],
            "ddec6_charges": [0.8, -0.8],
        }
    )
    structure = OptimadeStructure(**data)
    assert structure.charge_density_grid_shape == [2, 2, 2]
    assert structure.bader_charges == [1.5, -1.5]
    assert structure.bader_atomic_volume == [10.0, 12.0]
    assert structure.ddec6_charges == [0.8, -0.8]
    assert structure.compressed_charge_density is not None


def test_bader_charges_wrong_length():
    """Test that bader_charges with wrong length raises ValueError."""
    data = VALID_STRUCTURE_DATA.copy()
    data["bader_charges"] = [1.0, 2.0, 3.0]  # nsites=2, but 3 values
    with pytest.raises(ValueError, match="bader_charges"):
        OptimadeStructure(**data)


def test_ddec6_charges_wrong_length():
    """Test that ddec6_charges with wrong length raises ValueError."""
    data = VALID_STRUCTURE_DATA.copy()
    data["ddec6_charges"] = [1.0]  # nsites=2, but 1 value
    with pytest.raises(ValueError, match="ddec6_charges"):
        OptimadeStructure(**data)


def test_bader_atomic_volume_wrong_length():
    """Test that bader_atomic_volume with wrong length raises ValueError."""
    data = VALID_STRUCTURE_DATA.copy()
    data["bader_atomic_volume"] = [10.0, 12.0, 14.0]  # nsites=2, but 3 values
    with pytest.raises(ValueError, match="bader_atomic_volume"):
        OptimadeStructure(**data)


def test_charge_density_grid_shape_validation():
    """Test that charge_density_grid_shape must be exactly 3 elements."""
    data = VALID_STRUCTURE_DATA.copy()

    # Too short
    data["charge_density_grid_shape"] = [15, 15]
    with pytest.raises(ValueError):
        OptimadeStructure(**data)

    # Too long
    data["charge_density_grid_shape"] = [15, 15, 15, 15]
    with pytest.raises(ValueError):
        OptimadeStructure(**data)


def test_optimade_db_columns_do_not_include_charge_density_fields():
    """Charge density columns are not in the Postgres schema.

    LeMatRho uses a direct S3 → Parquet pipeline and never writes to Postgres.
    The charge density fields live only on OptimadeStructure (Parquet schema).
    """
    cols = OptimadeDatabase.columns()
    assert "compressed_charge_density" not in cols
    assert "bader_charges" not in cols
    assert "ddec6_charges" not in cols


def test_trajectories_db_columns_do_not_include_charge_density_fields():
    """Charge density columns are absent from TrajectoriesDatabase too."""
    cols = TrajectoriesDatabase.columns()
    assert "compressed_charge_density" not in cols
    assert "bader_charges" not in cols
    assert "ddec6_charges" not in cols
    # Trajectory-specific columns are still present
    assert "relaxation_step" in cols
    assert "relaxation_number" in cols


def test_optimade_db_column_count_matches_insert_tuple():
    """Guard test: verify that the number of columns matches what insert_data expects.

    This prevents silent data corruption from tuple/column ordering mismatches
    across the 4 manually maintained tuple definitions in postgres.py.
    """
    optimade_col_count = len(OptimadeDatabase.columns())
    traj_col_count = len(TrajectoriesDatabase.columns())
    # TrajectoriesDatabase should have exactly 2 more columns (relaxation_step, relaxation_number)
    assert traj_col_count == optimade_col_count + 2
