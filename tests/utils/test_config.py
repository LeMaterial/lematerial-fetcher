# Copyright 2025 Entalpic
import os

import dotenv
import pytest
from dotenv import load_dotenv


@pytest.fixture(autouse=True, scope="module")
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
        "MATERIALFETCHER_API_BASE_URL": "https://api.material-fetcher.com",
        "MATERIALFETCHER_DB_USER": "testuser",
        "MATERIALFETCHER_DB_PASSWORD": "testpass",
        "MATERIALFETCHER_DB_NAME": "testdb",
        "MATERIALFETCHER_TABLE_NAME": "test_table",
        "MATERIALFETCHER_MP_BUCKET_NAME": "test_bucket",
        "MATERIALFETCHER_MP_BUCKET_PREFIX": "test_prefix",
        "MATERIALFETCHER_MP_COLLECTIONS_PREFIX": "test_collections",
    }
    for key, value in test_env_vars.items():
        monkeypatch.setenv(key, value)
    return test_env_vars


def test_load_config(mock_config_env_vars):
    """Test loading configuration from env file with all required variables"""
    from material_fetcher.utils.config import load_config

    config = load_config()

    assert config.base_url == mock_config_env_vars["MATERIALFETCHER_API_BASE_URL"]
    assert config.table_name == mock_config_env_vars["MATERIALFETCHER_TABLE_NAME"]
    assert (
        config.mp_bucket_name == mock_config_env_vars["MATERIALFETCHER_MP_BUCKET_NAME"]
    )
    assert (
        config.mp_bucket_prefix
        == mock_config_env_vars["MATERIALFETCHER_MP_BUCKET_PREFIX"]
    )
    assert (
        config.mp_collections_prefix
        == mock_config_env_vars["MATERIALFETCHER_MP_COLLECTIONS_PREFIX"]
    )
    assert (
        config.db_conn_str
        == f"user={mock_config_env_vars['MATERIALFETCHER_DB_USER']} password={mock_config_env_vars['MATERIALFETCHER_DB_PASSWORD']} dbname={mock_config_env_vars['MATERIALFETCHER_DB_NAME']} sslmode=disable"
    )

    # default values
    assert config.log_dir == "./logs"
    assert config.max_retries == 3
    assert config.num_workers == 2
    assert config.page_limit == 10
    assert config.retry_delay == 2


@pytest.fixture
def mock_missing_config_env_vars(monkeypatch):
    """Fixture to set up test environment variables"""
    test_env_vars = {
        "MATERIALFETCHER_DB_USER": "testuser",
        "MATERIALFETCHER_DB_PASSWORD": "testpass",
        "MATERIALFETCHER_DB_NAME": "testdb",
        "MATERIALFETCHER_TABLE_NAME": "test_table",
        "MATERIALFETCHER_MP_BUCKET_NAME": "test_bucket",
        "MATERIALFETCHER_MP_BUCKET_PREFIX": "test_prefix",
        "MATERIALFETCHER_MP_COLLECTIONS_PREFIX": "test_collections",
    }
    for key, value in test_env_vars.items():
        monkeypatch.setenv(key, value)
    return test_env_vars


def test_missing_required_vars(mock_missing_config_env_vars):
    """Test behavior when required environment variables are missing"""
    from material_fetcher.utils.config import load_config

    with pytest.raises(ValueError) as exc_info:
        load_config()
    assert "MATERIALFETCHER_API_BASE_URL is not set" in str(exc_info.value)
