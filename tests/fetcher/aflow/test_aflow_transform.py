import pytest
from datetime import datetime
from lematerial_fetcher.fetcher.aflow.transform import AflowTransformer
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import Functional, OptimadeStructure

# Mock configuration (if needed by BaseTransformer)
class MockConfig:
    dest_table_name = "test_table"
    # Add other config fields if BaseTransformer expects them

@pytest.fixture
def transformer():
    # You might need to mock the config depending on BaseTransformer's __init__
    # Assuming standard init allows empty/default config
    return AflowTransformer(config=MockConfig()) 

# Mock configuration
class MockConfig:
    dest_table_name = "test_table"

@pytest.fixture
def transformer():
    return AflowTransformer(config=MockConfig()) 

# The list of inputs and expected outcomes
@pytest.mark.parametrize("dft_type_input, expected_functional", [
    # --- ALLOWED TYPES ---
    (["PAW_PBE"], Functional.PBE),             # Standard PBE -> Pass
    (["PAW_PBE_KIN:SCAN"], Functional.SCAN),   # Specific SCAN -> Pass
    # --- REJECTED TYPES (Return empty list) ---
    (["PAW_GGA"], None),
    (["PAW_PBE_KIN"], None),       # Rejected based on your request
    (["PAW_LDA"], None),
    (["PAW_LDA_KIN"], None),
    (["LDA"], None),
    (["GGA"], None),
    (["PAW_RPBE"], None),
    (["PAW_GGA_KIN"], None),
    (["PAW_PBE:SCAN"], None),      # Rejected (only KIN:SCAN allowed)
    (["PAW_GGA_KIN:SCAN"], None),
    (["Unknown_Trash"], None),
])
def test_aflow_dft_type_filtering(transformer, dft_type_input, expected_functional):
    """
    Parametrized test to verify that only specific DFT types are accepted.
    """
    # 1. Setup Mock Data
    raw_data = {
        "data": {
            "auid": "aflow:test_entry",
            "dft_type": dft_type_input,  # Injected from parameter
            "enthalpy_cell": -10.0,
            "geometry": [3.0, 3.0, 3.0, 90, 90, 90],
            "positions_fractional": [[0,0,0]],
            "species": ["Fe"],
            "composition": [1]
        }
    }
    
    raw_row = RawStructure(
        id="row_test", 
        type="structure", 
        attributes=raw_data, 
        last_modified=datetime.now().isoformat()
    )

    # 2. Run Transformation
    result = transformer.transform_row(raw_row)

    # 3. Validation logic
    if expected_functional is None:
        # We expect the row to be dropped (filtered out)
        assert result == [], f"DFT Type '{dft_type_input}' should have been dropped but returned {result}"
    else:
        # We expect success
        assert len(result) == 1, f"DFT Type '{dft_type_input}' should have returned 1 row"
        assert result[0].functional == expected_functional


def test_aflow_transform_success(transformer):
    """
    Test Case 1: The Happy Path.
    Verifies that a valid AFLOW JSON entry is correctly converted into an OptimadeStructure.
    """
    # 1. Mock the Input Data (RawStructure from Postgres)
    # This mimics exactly what your Fetcher saves: attributes={"data": {...}}
    raw_data = {
        "data": {
            "auid": "aflow:test_entry_001",
            "dft_type": ["PAW_PBE"],
            "enthalpy_cell": -12.5,  # Total energy
            "Egap": 1.2,
            
            # Structure Data (Ag2O1 example)
            "geometry": [3.5, 3.5, 3.5, 90, 90, 90],  # Cubic
            "positions_fractional": [
                [0.0, 0.0, 0.0],      # Atom 1
                [0.5, 0.5, 0.5],      # Atom 2
                [0.25, 0.25, 0.25]    # Atom 3
            ],
            "species": ["Ag", "O"],
            "composition": [2, 1]     # 2 Silver, 1 Oxygen
        }
    }

    raw_row = RawStructure(
        id="test_row_1",
        type="structure",
        attributes=raw_data,
        last_modified=datetime.now().isoformat()
    )

    # 2. Run the Transformation
    print('Try to transform the raw row. Still in the test.')
    result = transformer.transform_row(raw_row)

    # 3. Assertions
    assert len(result) == 1
    optimade_obj = result[0]

    # Check Identity
    assert isinstance(optimade_obj, OptimadeStructure)
    assert optimade_obj.id == "aflow-aflow:test_entry_001"
    
    # Check Structure construction
    assert optimade_obj.nsites == 3
    assert len(optimade_obj.cartesian_site_positions) == 3
    assert optimade_obj.lattice_vectors[0][0] == 3.5

    # Check Properties
    assert optimade_obj.functional == Functional.PBE
    assert optimade_obj.energy == -12.5
    assert optimade_obj.band_gap_indirect == 1.2


def test_aflow_transform_invalid_structure(transformer):
    """
    Test Case 2: The Sad Path.
    Verifies that the transformer gracefully handles corrupted data 
    (mismatch between atoms and coordinates) by returning an empty list.
    """
    # 1. Mock Corrupted Data
    bad_data = {
        "auid": "aflow:broken_entry",
        "geometry": [5.0, 5.0, 5.0, 90, 90, 90],
        
        # DEFINING 2 POSITIONS...
        "positions_fractional": [
            [0.0, 0.0, 0.0],
            [0.5, 0.5, 0.5]
        ],
        "species": ["Fe"],
        # ... BUT SAYING THERE ARE 100 ATOMS
        "composition": [100] 
    }

    raw_row = RawStructure(
        id="test_row_bad",
        type="structure",
        attributes=bad_data,
        last_modified=datetime.now().isoformat()
    )

    # 2. Run Transformation
    result = transformer.transform_row(raw_row)

    # 3. Assertions
    # It should return an empty list, NOT crash with an IndexError/ValueError
    assert result == []
