"""Test that the fetch.py and transform.py scripts play nicely together."""
import pytest
from unittest.mock import MagicMock, patch
from lematerial_fetcher.fetcher.aflow.fetch import AflowFetcher
from lematerial_fetcher.fetcher.aflow.transform import AflowTransformer
from lematerial_fetcher.fetch import BatchInfo
from lematerial_fetcher.models.optimade import OptimadeStructure, Functional

@pytest.mark.slow
def test_aflow_fetch_and_transform_integration():
    """
    End-to-End Integration Test:
    1. Fetches 1 real page from AFLOW API (Network Call).
    2. Captures the data (intercepts the DB save).
    3. Passes that data immediately to AflowTransformer.
    4. Verifies valid OptimadeStructure objects are produced.
    """
    print("\n--- STARTING INTEGRATION TEST ---")

    # --- STEP 1: FETCH (with intercepted DB) ---
    class MockConfig: 
        db_conn_str = "postgresql://user:pass@localhost/test_db"
        table_name = "aflow_source"
        dest_table_name = "optimade_structures"
        
        # Add these fields which Fetcher logic accesses:
        base_url = "https://aflow.org/API/aflux/?"
        page_limit = 10
        page_offset = 0
        max_retries = 3
        retry_delay = 1
        log_every = 100
        
    mock_config = MockConfig()
    
    # We ask for a very small page (5 items) to keep it fast
    batch = BatchInfo(offset=0, limit=5)
    
    captured_raw_data = []

    # Patch the database so we don't need Postgres running
    with patch("lematerial_fetcher.fetcher.aflow.fetch.StructuresDatabase") as MockDB:
        mock_db_instance = MockDB.return_value
        
        # Define a side_effect to capture what would have been written to DB
        def capture_insert(data_list):
            captured_raw_data.extend(data_list)

        # IMPORTANT: Patch batch_insert_data (not insert_data)
        mock_db_instance.batch_insert_data.side_effect = capture_insert

        # Run the fetcher
        print("1. Fetching data from AFLOW API...")
        success = AflowFetcher._process_batch(
            batch=batch, 
            config=mock_config, 
            manager_dict={},
            worker_id=1
        )
        
        assert success is True, "Fetcher failed to retrieve data from API"
        assert len(captured_raw_data) > 0, "Fetcher retrieved 0 entries"
        print(f"   Success! Captured {len(captured_raw_data)} raw entries.")

    # --- STEP 2: TRANSFORM ---
    print("2. Initializing Transformer...")
    # We patch the target DB connection since transformer init usually connects to checking versions
    with patch("lematerial_fetcher.transform.OptimadeDatabase"):
        transformer = AflowTransformer(config=mock_config)
        
        print(f"3. Transforming {len(captured_raw_data)} rows...")
        
        successful_transforms = 0
        for i, raw_entry in enumerate(captured_raw_data):
            # raw_entry is now a RawStructure object!
            
            # Run the transformation logic
            results = transformer.transform_row(raw_entry)
            
            if not results:
                # FIX: Access attributes['data'] instead of raw_entry['data']
                auid = raw_entry.attributes['data'].get('auid', 'Unknown')
                print(f"   Row {i}: Skipped/Failed (AUID: {auid})")
                continue
                
            successful_transforms += 1
            optimade_obj = results[0]
            
            # --- STEP 3: VERIFICATION ---
            # Verify one entry in detail (the first one we successfully transform)
            if successful_transforms == 1:
                print("\n   --- INSPECTING FIRST RESULT ---")
                print(f"   ID: {optimade_obj.id}")
                print(f"   Formula: {optimade_obj.chemical_formula_descriptive}")
                print(f"   Elements: {optimade_obj.elements}")
                print(f"   Sites: {optimade_obj.nsites}")
                print(f"   Lattice: {optimade_obj.lattice_vectors}")
                print(f"   Energy: {optimade_obj.energy} eV")
                print(f"   Functional: {optimade_obj.functional}")
                
                # Critical Assertions
                assert isinstance(optimade_obj, OptimadeStructure)
                assert optimade_obj.nsites == len(optimade_obj.cartesian_site_positions)
                assert optimade_obj.nsites == len(optimade_obj.species_at_sites)
                assert optimade_obj.nelements == len(optimade_obj.elements)
                
                # Check that logic for PBE/LDA parsing worked
                assert isinstance(optimade_obj.functional, Functional) or optimade_obj.functional is None

        print(f"\n4. Summary: Successfully transformed {successful_transforms}/{len(captured_raw_data)} entries.")
        assert successful_transforms > 0, "Transformer failed to convert any fetched rows!"
