import pytest
from unittest.mock import MagicMock, patch
from lematerial_fetcher.fetcher.aflow.fetch import AflowFetcher
from lematerial_fetcher.fetch import BatchInfo, FetcherConfig

@pytest.mark.slow
def test_aflow_unlimited_mode_config():
    """
    Verifies that get_items_to_process returns None for total_count,
    triggering the BaseFetcher's unlimited pagination loop.
    """
    mock_config = MagicMock(spec=FetcherConfig)
    mock_config.db_conn_str = "postgresql://dummy"
    mock_config.table_name = "test_aflow"

    with patch("lematerial_fetcher.fetch.DatasetVersions"): 
        fetcher = AflowFetcher(config=mock_config)
        items_info = fetcher.get_items_to_process()

    print(f"\nFetched Total Count: {items_info.total_count}")
    
    assert items_info.total_count is None
    assert items_info.start_offset == 0


@pytest.mark.slow
def test_aflow_process_batch_live():
    """
    Integration test that hits the actual AFLOW API for one page
    but Mocks the database insertion to avoid needing a real DB.
    """
    
    # 1. Create a Mock Config
    mock_config = MagicMock(spec=FetcherConfig)
    mock_config.db_conn_str = "postgresql://user:pass@localhost/db"
    mock_config.table_name = "test_aflow"
    
    # 2. Create a BatchInfo for Page 1
    # Offset 0, Limit 10 -> Should result in paging(1, 10)
    batch = BatchInfo(offset=0, limit=10)
    
    # 3. Patch the StructuresDatabase so we don't try to connect to Postgres
    # We patch inside the module where it is imported in fetch.py
    with patch("lematerial_fetcher.fetcher.aflow.fetch.StructuresDatabase") as MockDB:
        
        # Setup the mock DB instance
        mock_db_instance = MockDB.return_value
        
        # 4. Run the static method
        # manager_dict is for IPC, we can pass a dummy dict
        success = AflowFetcher._process_batch(
            batch=batch, 
            config=mock_config, 
            manager_dict={},
            worker_id=99
        )

        # 5. Assertions
        assert success is True, "Process batch should return True if data was found"

        assert mock_db_instance.batch_insert_data.called, "Should call batch_insert_data"
        
        # --- FIX 2: Get args from the correct method ---
        call_args = mock_db_instance.batch_insert_data.call_args
        # First arg of the first call is the list of structures
        inserted_list = call_args[0][0] 
        
        assert len(inserted_list) > 0
        
        # --- FIX 3: Inspect RawStructure objects ---
        # The list now contains RawStructure objects, NOT dicts.
        first_structure = inserted_list[0]
        
        # Verify it's a RawStructure
        assert hasattr(first_structure, "attributes"), "Should be a RawStructure object"
        
        # Unwrap the data (attributes["data"])
        first_entry = first_structure.attributes["data"]
        
        print(f"\nFetched Entry: {first_entry.get('auid', 'Unknown')}")
        assert "auid" in first_entry
        assert "species" in first_entry

        # --- DATA TYPE INSPECTION ---
        print("\n--- DATA TYPE INSPECTION ---")
        geo = first_entry.get("geometry")
        print(f"Geometry Type: {type(geo)}")
        
        pos = first_entry.get("positions_fractional")
        print(f"Positions Type: {type(pos)}")

        comp = first_entry.get("composition")
        print(f"Composition Type: {type(comp)}")
