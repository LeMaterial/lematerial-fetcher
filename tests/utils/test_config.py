# Copyright 2025 Entalpic
import os

import dotenv
import pytest
from dotenv import load_dotenv

from lematerial_fetcher.utils.config import load_fetcher_config


@pytest.fixture(autouse=True, scope="function")
def clean_env():
    """Clean environment variables before each test"""
    # Store original env vars
    original_env = dict(os.environ)

    # Remove all LEMATERIALFETCHER_ env vars
    for key in list(os.environ.keys()):
        if key.startswith("LEMATERIALFETCHER_"):
            del os.environ[key]

    yield

    # Restore original env vars
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture(autouse=True, scope="session")
def mock_load_dotenv():
    """Prevent load_dotenv from loading any .env files during tests"""
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(dotenv, "load_dotenv", lambda *args, **kwargs: None)
        yield


def test_load_env_file(tmp_path):
    env_path = tmp_path / ".env"
    env_content = """
    TEST_API_KEY=mock_key_456
    TEST_DEBUG=false
    TEST_PORT=9090
    """
    env_path.write_text(env_content)

    load_dotenv(env_path)

    assert os.getenv("TEST_API_KEY") == "mock_key_456"
    assert os.getenv("TEST_DEBUG") == "false"
    assert os.getenv("TEST_PORT") == "9090"


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Fixture to set up test environment variables"""
    test_env_vars = {
        "TEST_API_KEY": "mock_key_456",
        "TEST_DEBUG": "false",
        "TEST_PORT": "9090",
    }
    for key, value in test_env_vars.items():
        monkeypatch.setenv(key, value)
    return test_env_vars


def test_env_variables(mock_env_vars):
    assert os.getenv("TEST_API_KEY") == mock_env_vars["TEST_API_KEY"]
    assert os.getenv("TEST_DEBUG") == mock_env_vars["TEST_DEBUG"]
    assert os.getenv("TEST_PORT") == mock_env_vars["TEST_PORT"]


def test_env_variable_not_set():
    # Test behavior when trying to access non-existent env variable
    assert os.getenv("NONEXISTENT_VAR") is None
    assert os.getenv("NONEXISTENT_VAR", "default") == "default"


@pytest.fixture
def mock_config_env_vars(monkeypatch):
    """Fixture to set up test environment variables"""
    test_env_vars = {
        # Base config vars
        "LEMATERIALFETCHER_LOG_DIR": "./logs",
        "LEMATERIALFETCHER_MAX_RETRIES": "3",
        "LEMATERIALFETCHER_NUM_WORKERS": "2",
        "LEMATERIALFETCHER_RETRY_DELAY": "2",
        "LEMATERIALFETCHER_LOG_EVERY": "1000",
        # Fetcher specific vars
        "LEMATERIALFETCHER_API_BASE_URL": "https://api.test.com",
        "LEMATERIALFETCHER_DB_USER": "testuser",
        "LEMATERIALFETCHER_DB_PASSWORD": "testpass",
        "LEMATERIALFETCHER_DB_NAME": "testdb",
        "LEMATERIALFETCHER_TABLE_NAME": "test_table",
        "LEMATERIALFETCHER_PAGE_LIMIT": "10",
        "LEMATERIALFETCHER_PAGE_OFFSET": "0",
        "LEMATERIALFETCHER_MP_BUCKET_NAME": "test-bucket",
        "LEMATERIALFETCHER_MP_BUCKET_PREFIX": "test/prefix",
    }
    for key, value in test_env_vars.items():
        monkeypatch.setenv(key, value)
    return test_env_vars


def test_load_fetcher_config(mock_config_env_vars):
    """Test loading fetcher configuration with all required variables"""
    config = load_fetcher_config()

    # Test base config values
    assert config.log_dir == mock_config_env_vars["LEMATERIALFETCHER_LOG_DIR"]
    assert config.max_retries == int(
        mock_config_env_vars["LEMATERIALFETCHER_MAX_RETRIES"]
    )
    assert config.num_workers == int(
        mock_config_env_vars["LEMATERIALFETCHER_NUM_WORKERS"]
    )
    assert config.retry_delay == int(
        mock_config_env_vars["LEMATERIALFETCHER_RETRY_DELAY"]
    )
    assert config.log_every == int(mock_config_env_vars["LEMATERIALFETCHER_LOG_EVERY"])

    # Test fetcher specific values
    assert config.base_url == mock_config_env_vars["LEMATERIALFETCHER_API_BASE_URL"]
    assert config.table_name == mock_config_env_vars["LEMATERIALFETCHER_TABLE_NAME"]
    assert (
        config.mp_bucket_name
        == mock_config_env_vars["LEMATERIALFETCHER_MP_BUCKET_NAME"]
    )
    assert (
        config.mp_bucket_prefix
        == mock_config_env_vars["LEMATERIALFETCHER_MP_BUCKET_PREFIX"]
    )
    assert config.page_limit == int(
        mock_config_env_vars["LEMATERIALFETCHER_PAGE_LIMIT"]
    )
    assert config.page_offset == int(
        mock_config_env_vars["LEMATERIALFETCHER_PAGE_OFFSET"]
    )

    # Test database connection string
    expected_db_conn = (
        f"user={mock_config_env_vars['LEMATERIALFETCHER_DB_USER']} "
        f"password={mock_config_env_vars['LEMATERIALFETCHER_DB_PASSWORD']} "
        f"dbname={mock_config_env_vars['LEMATERIALFETCHER_DB_NAME']} "
        "sslmode=disable"
    )
    assert config.db_conn_str == expected_db_conn
