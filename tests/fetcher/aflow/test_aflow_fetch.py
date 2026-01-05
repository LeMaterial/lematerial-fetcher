import pytest
from unittest.mock import MagicMock, patch
from lematerial_fetcher.fetcher.aflow.fetch import AflowFetcher
from lematerial_fetcher.fetch import BatchInfo, FetcherConfig

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
        
        # Verify DB insertion was called
        assert mock_db_instance.insert_data.called
        
        # Check what was inserted
        call_args = mock_db_instance.insert_data.call_args
        inserted_data = call_args[0][0] # First arg of the first call
        
        assert len(inserted_data) > 0
        assert "data" in inserted_data[0]
        
        # Check an actual AFLOW field inside the JSON blob
        first_entry = inserted_data[0]["data"]
        print(f"\nFetched Entry: {first_entry.get('auid', 'Unknown')}")
        assert "auid" in first_entry
        assert "species" in first_entry
