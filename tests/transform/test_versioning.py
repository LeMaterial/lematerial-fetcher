from datetime import datetime, timezone
from threading import Thread
from unittest.mock import MagicMock, patch

import pytest

from lematerial_fetcher.database.postgres import (
    DatasetVersions,
    OptimadeDatabase,
    StructuresDatabase,
)
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.transform import BaseTransformer
from lematerial_fetcher.utils.config import TransformerConfig


class TestTransformer(BaseTransformer):
    """Test implementation of BaseTransformer for testing versioning."""

    def transform_row(self, raw_structure: RawStructure, task_table_name=None):
        return []


@pytest.fixture
def mock_version_db():
    """Create a mock version database."""
    mock_db = MagicMock(spec=DatasetVersions)
    mock_db.get_last_synced_version.return_value = None
    return mock_db


@pytest.fixture
def mock_source_db():
    """Create a mock source database."""
    mock_db = MagicMock(spec=StructuresDatabase)
    mock_db.fetch_items.return_value = []  # no items to process by default
    return mock_db


@pytest.fixture
def mock_target_db():
    """Create a mock target database."""
    mock_db = MagicMock(spec=OptimadeDatabase)
    return mock_db


@pytest.fixture
def transformer():
    """Create a test transformer instance."""
    config = TransformerConfig(
        source_db_conn_str="mock://source",
        dest_db_conn_str="mock://dest",
        source_table_name="test_source",
        dest_table_name="test_dest",
        batch_size=100,
        log_every=100,
        log_dir="./logs",
        max_retries=3,
        num_workers=2,
        retry_delay=2,
    )
    return TestTransformer(config)


@pytest.fixture
def patched_transformer(transformer, mock_version_db, mock_source_db, mock_target_db):
    """Create a transformer with patched database classes."""
    with (
        patch(
            "lematerial_fetcher.transform.DatasetVersions", return_value=mock_version_db
        ),
        patch(
            "lematerial_fetcher.transform.StructuresDatabase",
            return_value=mock_source_db,
        ),
        patch(
            "lematerial_fetcher.transform.OptimadeDatabase", return_value=mock_target_db
        ),
    ):
        yield transformer


def test_get_transform_version_initial(patched_transformer, mock_version_db):
    """Test getting transform version when none exists."""
    mock_version_db.get_last_synced_version.return_value = None
    version = patched_transformer.get_transform_version()
    assert version is None
    mock_version_db.get_last_synced_version.assert_called_once_with(
        "test_dest_transform"
    )


def test_update_transform_version(patched_transformer, mock_version_db):
    """Test updating transform version."""
    test_version = "2025-01-01"
    patched_transformer.update_transform_version(test_version)

    mock_version_db.update_version.assert_called_once_with(
        "test_dest_transform", test_version
    )

    mock_version_db.get_last_synced_version.return_value = test_version
    version = patched_transformer.get_transform_version()
    assert version == test_version


def test_get_new_transform_version(patched_transformer):
    """Test getting new transform version."""
    version = patched_transformer.get_new_transform_version()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert version == today


def test_transform_updates_version(patched_transformer, mock_version_db):
    """Test that transform process updates version."""
    initial_version = "2024-12-31"
    mock_version_db.get_last_synced_version.return_value = initial_version

    # mock the get_new_transform_version to return a known value
    new_version = "2025-01-01"
    patched_transformer.get_new_transform_version = lambda: new_version

    patched_transformer.transform()

    # check version was updated
    mock_version_db.update_version.assert_called_with(
        "test_dest_transform", new_version
    )


def test_transform_skips_version_update_if_same(patched_transformer, mock_version_db):
    """Test that transform doesn't update version if it hasn't changed."""
    initial_version = "2025-01-01"
    mock_version_db.get_last_synced_version.return_value = initial_version

    patched_transformer.get_new_transform_version = lambda: initial_version

    patched_transformer.transform()

    mock_version_db.update_version.assert_not_called()


def test_transform_handles_version_error(patched_transformer, mock_version_db):
    """Test transform handles errors in version operations gracefully."""

    def raise_error(*args, **kwargs):
        raise Exception("Version error")

    # mock version operations to raise error
    mock_version_db.get_last_synced_version.side_effect = raise_error

    # transform should still complete without version operations
    patched_transformer.transform()


def test_cleanup_resources(patched_transformer):
    """Test that cleanup properly removes thread-local resources."""
    # access all database properties to ensure they're created
    _ = patched_transformer.source_db
    _ = patched_transformer.target_db
    _ = patched_transformer.get_transform_version()  # creates version_db

    patched_transformer.cleanup_resources()

    assert not hasattr(patched_transformer._thread_local, "source_db")
    assert not hasattr(patched_transformer._thread_local, "target_db")
    assert not hasattr(patched_transformer._thread_local, "version_db")


@patch("lematerial_fetcher.transform.DatasetVersions")
@patch("lematerial_fetcher.transform.StructuresDatabase")
@patch("lematerial_fetcher.transform.OptimadeDatabase")
def test_thread_local_databases(
    mock_optimade_class, mock_structures_class, mock_dataset_versions_class, transformer
):
    """Test that each thread gets its own database connections."""
    from threading import Lock

    results = {}
    results_lock = Lock()

    thread_mocks = {
        "thread1": (
            MagicMock(spec=DatasetVersions),
            MagicMock(spec=StructuresDatabase),
            MagicMock(spec=OptimadeDatabase),
        ),
        "thread2": (
            MagicMock(spec=DatasetVersions),
            MagicMock(spec=StructuresDatabase),
            MagicMock(spec=OptimadeDatabase),
        ),
    }

    def thread_func(thread_name):
        mock_version_db, mock_source_db, mock_target_db = thread_mocks[thread_name]

        mock_dataset_versions_class.return_value = mock_version_db
        mock_structures_class.return_value = mock_source_db
        mock_optimade_class.return_value = mock_target_db

        version_db = transformer.get_transform_version()
        source_db = transformer.source_db
        target_db = transformer.target_db

        # store results thread-safely
        with results_lock:
            results[thread_name] = (version_db, source_db, target_db)
            print(f"Thread {thread_name} stored results: {results[thread_name]}")

    threads = [
        Thread(target=thread_func, args=("thread1",)),
        Thread(target=thread_func, args=("thread2",)),
    ]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == 2, f"Expected 2 thread results, got {len(results)}"
    assert results["thread1"] != results["thread2"], (
        "Thread database instances should be different"
    )


@pytest.fixture
def setup_source_data(mock_source_db):
    """Set up test data in source database."""
    test_data = RawStructure(
        id="mp-123",
        type="structure",
        attributes={
            "material_id": "mp-123",
            "elements": ["Fe", "O"],
            "nelements": 2,
            "nsites": 3,
            "structure": {
                "lattice": {"a": 1.0, "b": 1.0, "c": 1.0},
                "sites": [
                    {"species": [{"element": "Fe"}], "xyz": [0, 0, 0]},
                    {"species": [{"element": "O"}], "xyz": [0.5, 0.5, 0]},
                    {"species": [{"element": "O"}], "xyz": [0, 0.5, 0.5]},
                ],
            },
            "composition_reduced": {"Fe": 1, "O": 2},
            "formula_anonymous": "AB2",
            "formula_pretty": "FeO2",
            "builder_meta": {"build_date": {"$date": "2025-01-01T00:00:00Z"}},
        },
        last_modified="2025-01-01",
    )

    mock_source_db.fetch_items.return_value = [test_data]
    return test_data


def test_get_new_transform_version_fallback(patched_transformer, mock_target_db):
    """Test getting new transform version fallback."""
    version = patched_transformer.get_new_transform_version()

    # should return today's date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert version == today
