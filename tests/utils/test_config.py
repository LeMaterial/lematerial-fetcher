# Copyright 2025 Entalpic
import os
from unittest.mock import patch

import dotenv
import pytest
from dotenv import load_dotenv

from lematerial_fetcher.utils.config import load_fetcher_config, load_transformer_config


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
    with patch("lematerial_fetcher.utils.config.load_dotenv") as mock_load_dotenv:
        mock_load_dotenv.return_value = {}

        # Simulate how Click would pass environment variables as parameters
        # Click converts all environment variables to parameters before calling the command function
        config_kwargs = {
            "base_url": mock_config_env_vars["LEMATERIALFETCHER_API_BASE_URL"],
            "db_user": mock_config_env_vars["LEMATERIALFETCHER_DB_USER"],
            "db_name": mock_config_env_vars["LEMATERIALFETCHER_DB_NAME"],
            "table_name": mock_config_env_vars["LEMATERIALFETCHER_TABLE_NAME"],
            "log_dir": mock_config_env_vars["LEMATERIALFETCHER_LOG_DIR"],
            "max_retries": int(mock_config_env_vars["LEMATERIALFETCHER_MAX_RETRIES"]),
            "num_workers": int(mock_config_env_vars["LEMATERIALFETCHER_NUM_WORKERS"]),
            "retry_delay": int(mock_config_env_vars["LEMATERIALFETCHER_RETRY_DELAY"]),
            "log_every": int(mock_config_env_vars["LEMATERIALFETCHER_LOG_EVERY"]),
            "mp_bucket_name": mock_config_env_vars["LEMATERIALFETCHER_MP_BUCKET_NAME"],
            "mp_bucket_prefix": mock_config_env_vars[
                "LEMATERIALFETCHER_MP_BUCKET_PREFIX"
            ],
        }

        config = load_fetcher_config(**config_kwargs)

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
    assert config.page_limit == int(
        mock_config_env_vars["LEMATERIALFETCHER_PAGE_LIMIT"]
    )
    assert config.page_offset == int(
        mock_config_env_vars["LEMATERIALFETCHER_PAGE_OFFSET"]
    )

    # Test database connection string
    expected_db_conn = (
        f"host=localhost "  # Default value
        f"user={mock_config_env_vars['LEMATERIALFETCHER_DB_USER']} "
        f"password={mock_config_env_vars['LEMATERIALFETCHER_DB_PASSWORD']} "
        f"dbname={mock_config_env_vars['LEMATERIALFETCHER_DB_NAME']} "
        "sslmode=disable"
    )
    assert config.db_conn_str == expected_db_conn


@pytest.fixture
def mock_transformer_env_vars(monkeypatch):
    """Fixture to set up test environment variables for transformer config"""
    test_env_vars = {
        # Base config vars
        "LEMATERIALFETCHER_LOG_DIR": "./logs",
        "LEMATERIALFETCHER_MAX_RETRIES": "3",
        "LEMATERIALFETCHER_NUM_WORKERS": "2",
        "LEMATERIALFETCHER_RETRY_DELAY": "2",
        "LEMATERIALFETCHER_LOG_EVERY": "1000",
        # Source database vars
        "LEMATERIALFETCHER_DB_USER": "source_user",
        "LEMATERIALFETCHER_DB_PASSWORD": "source_pass",
        "LEMATERIALFETCHER_DB_HOST": "source.host",
        "LEMATERIALFETCHER_DB_NAME": "source_db",
        "LEMATERIALFETCHER_TABLE_NAME": "source_table",
        # Destination table (but no destination database credentials)
        "LEMATERIALFETCHER_DEST_TABLE_NAME": "dest_table",
        # Other transformer vars
        "LEMATERIALFETCHER_BATCH_SIZE": "500",
        "LEMATERIALFETCHER_OFFSET": "0",
        "LEMATERIALFETCHER_TASK_TABLE_NAME": "task_table",
    }
    for key, value in test_env_vars.items():
        monkeypatch.setenv(key, value)
    return test_env_vars


@pytest.fixture
def mock_transformer_env_vars_with_dest(monkeypatch):
    """Fixture to set up test environment variables for transformer config with explicit destination DB"""
    test_env_vars = {
        # Base config vars
        "LEMATERIALFETCHER_LOG_DIR": "./logs",
        "LEMATERIALFETCHER_MAX_RETRIES": "3",
        "LEMATERIALFETCHER_NUM_WORKERS": "2",
        "LEMATERIALFETCHER_RETRY_DELAY": "2",
        "LEMATERIALFETCHER_LOG_EVERY": "1000",
        # Source database vars
        "LEMATERIALFETCHER_DB_USER": "source_user",
        "LEMATERIALFETCHER_DB_PASSWORD": "source_pass",
        "LEMATERIALFETCHER_DB_HOST": "source.host",
        "LEMATERIALFETCHER_DB_NAME": "source_db",
        "LEMATERIALFETCHER_TABLE_NAME": "source_table",
        # Destination database vars
        "LEMATERIALFETCHER_DEST_DB_USER": "dest_user",
        "LEMATERIALFETCHER_DEST_DB_PASSWORD": "dest_pass",
        "LEMATERIALFETCHER_DEST_DB_HOST": "dest.host",
        "LEMATERIALFETCHER_DEST_DB_NAME": "dest_db",
        "LEMATERIALFETCHER_DEST_TABLE_NAME": "dest_table",
        # Other transformer vars
        "LEMATERIALFETCHER_BATCH_SIZE": "500",
        "LEMATERIALFETCHER_OFFSET": "0",
        "LEMATERIALFETCHER_TASK_TABLE_NAME": "task_table",
    }
    for key, value in test_env_vars.items():
        monkeypatch.setenv(key, value)
    return test_env_vars


def test_load_transformer_config_with_fallback(mock_transformer_env_vars):
    """Test loading transformer config with source DB credentials as fallback for destination"""
    with patch("lematerial_fetcher.utils.config.load_dotenv") as mock_load_dotenv:
        mock_load_dotenv.return_value = {}

        # Simulate how Click would pass environment variables as parameters
        config_kwargs = {
            "log_dir": mock_transformer_env_vars["LEMATERIALFETCHER_LOG_DIR"],
            "max_retries": int(
                mock_transformer_env_vars["LEMATERIALFETCHER_MAX_RETRIES"]
            ),
            "num_workers": int(
                mock_transformer_env_vars["LEMATERIALFETCHER_NUM_WORKERS"]
            ),
            "retry_delay": int(
                mock_transformer_env_vars["LEMATERIALFETCHER_RETRY_DELAY"]
            ),
            "log_every": int(mock_transformer_env_vars["LEMATERIALFETCHER_LOG_EVERY"]),
            "db_user": mock_transformer_env_vars["LEMATERIALFETCHER_DB_USER"],
            "db_host": mock_transformer_env_vars["LEMATERIALFETCHER_DB_HOST"],
            "db_name": mock_transformer_env_vars["LEMATERIALFETCHER_DB_NAME"],
            "table_name": mock_transformer_env_vars["LEMATERIALFETCHER_TABLE_NAME"],
            "dest_table_name": mock_transformer_env_vars[
                "LEMATERIALFETCHER_DEST_TABLE_NAME"
            ],
            "batch_size": int(
                mock_transformer_env_vars["LEMATERIALFETCHER_BATCH_SIZE"]
            ),
            "task_source_table_name": mock_transformer_env_vars[
                "LEMATERIALFETCHER_TASK_TABLE_NAME"
            ],
        }

        config = load_transformer_config(**config_kwargs)

    # Test base config values
    assert config.log_dir == mock_transformer_env_vars["LEMATERIALFETCHER_LOG_DIR"]
    assert config.max_retries == int(
        mock_transformer_env_vars["LEMATERIALFETCHER_MAX_RETRIES"]
    )
    assert config.num_workers == int(
        mock_transformer_env_vars["LEMATERIALFETCHER_NUM_WORKERS"]
    )
    assert config.retry_delay == int(
        mock_transformer_env_vars["LEMATERIALFETCHER_RETRY_DELAY"]
    )
    assert config.log_every == int(
        mock_transformer_env_vars["LEMATERIALFETCHER_LOG_EVERY"]
    )

    # Test source database connection string
    expected_source_db_conn = (
        f"host={mock_transformer_env_vars['LEMATERIALFETCHER_DB_HOST']} "
        f"user={mock_transformer_env_vars['LEMATERIALFETCHER_DB_USER']} "
        f"password={mock_transformer_env_vars['LEMATERIALFETCHER_DB_PASSWORD']} "
        f"dbname={mock_transformer_env_vars['LEMATERIALFETCHER_DB_NAME']} "
        f"sslmode=disable"
    )
    assert config.source_db_conn_str == expected_source_db_conn

    # Test destination database connection string uses source credentials
    expected_dest_db_conn = (
        f"host={mock_transformer_env_vars['LEMATERIALFETCHER_DB_HOST']} "
        f"user={mock_transformer_env_vars['LEMATERIALFETCHER_DB_USER']} "
        f"password={mock_transformer_env_vars['LEMATERIALFETCHER_DB_PASSWORD']} "
        f"dbname={mock_transformer_env_vars['LEMATERIALFETCHER_DB_NAME']} "
        f"sslmode=disable"
    )
    assert config.dest_db_conn_str == expected_dest_db_conn

    # Test table names
    assert (
        config.source_table_name
        == mock_transformer_env_vars["LEMATERIALFETCHER_TABLE_NAME"]
    )
    assert (
        config.dest_table_name
        == mock_transformer_env_vars["LEMATERIALFETCHER_DEST_TABLE_NAME"]
    )
    assert (
        config.mp_task_table_name
        == mock_transformer_env_vars["LEMATERIALFETCHER_TASK_TABLE_NAME"]
    )
    assert config.batch_size == int(
        mock_transformer_env_vars["LEMATERIALFETCHER_BATCH_SIZE"]
    )


def test_load_transformer_config_with_explicit_dest(
    mock_transformer_env_vars_with_dest,
):
    """Test loading transformer config with explicit destination database credentials"""
    with patch("lematerial_fetcher.utils.config.load_dotenv") as mock_load_dotenv:
        mock_load_dotenv.return_value = {}

        # Simulate how we would pass the parameters to the command function
        config_kwargs = {
            "log_dir": mock_transformer_env_vars_with_dest["LEMATERIALFETCHER_LOG_DIR"],
            "max_retries": int(
                mock_transformer_env_vars_with_dest["LEMATERIALFETCHER_MAX_RETRIES"]
            ),
            "num_workers": int(
                mock_transformer_env_vars_with_dest["LEMATERIALFETCHER_NUM_WORKERS"]
            ),
            "retry_delay": int(
                mock_transformer_env_vars_with_dest["LEMATERIALFETCHER_RETRY_DELAY"]
            ),
            "log_every": int(
                mock_transformer_env_vars_with_dest["LEMATERIALFETCHER_LOG_EVERY"]
            ),
            "db_user": mock_transformer_env_vars_with_dest["LEMATERIALFETCHER_DB_USER"],
            "db_host": mock_transformer_env_vars_with_dest["LEMATERIALFETCHER_DB_HOST"],
            "db_name": mock_transformer_env_vars_with_dest["LEMATERIALFETCHER_DB_NAME"],
            "table_name": mock_transformer_env_vars_with_dest[
                "LEMATERIALFETCHER_TABLE_NAME"
            ],
            "dest_db_user": mock_transformer_env_vars_with_dest[
                "LEMATERIALFETCHER_DEST_DB_USER"
            ],
            "dest_db_host": mock_transformer_env_vars_with_dest[
                "LEMATERIALFETCHER_DEST_DB_HOST"
            ],
            "dest_db_name": mock_transformer_env_vars_with_dest[
                "LEMATERIALFETCHER_DEST_DB_NAME"
            ],
            "dest_table_name": mock_transformer_env_vars_with_dest[
                "LEMATERIALFETCHER_DEST_TABLE_NAME"
            ],
            "batch_size": int(
                mock_transformer_env_vars_with_dest["LEMATERIALFETCHER_BATCH_SIZE"]
            ),
            "task_source_table_name": mock_transformer_env_vars_with_dest[
                "LEMATERIALFETCHER_TASK_TABLE_NAME"
            ],
        }

        config = load_transformer_config(**config_kwargs)

    # Test source database connection string
    expected_db_conn = (
        f"host={mock_transformer_env_vars_with_dest['LEMATERIALFETCHER_DB_HOST']} "
        f"user={mock_transformer_env_vars_with_dest['LEMATERIALFETCHER_DB_USER']} "
        f"password={mock_transformer_env_vars_with_dest['LEMATERIALFETCHER_DB_PASSWORD']} "
        f"dbname={mock_transformer_env_vars_with_dest['LEMATERIALFETCHER_DB_NAME']} "
        f"sslmode=disable"
    )
    assert config.source_db_conn_str == expected_db_conn

    # Test destination database connection string uses explicit destination credentials
    expected_dest_db_conn = (
        f"host={mock_transformer_env_vars_with_dest['LEMATERIALFETCHER_DEST_DB_HOST']} "
        f"user={mock_transformer_env_vars_with_dest['LEMATERIALFETCHER_DEST_DB_USER']} "
        f"password={mock_transformer_env_vars_with_dest['LEMATERIALFETCHER_DEST_DB_PASSWORD']} "
        f"dbname={mock_transformer_env_vars_with_dest['LEMATERIALFETCHER_DEST_DB_NAME']} "
        f"sslmode=disable"
    )
    assert config.dest_db_conn_str == expected_dest_db_conn


def test_load_transformer_config_from_click():
    """Test that transformer config loads correctly when passed directly from Click"""
    # Set required environment variables for passwords
    os.environ["LEMATERIALFETCHER_DB_PASSWORD"] = "source_pass"

    # When Click calls the command function with --source-db-user source_user etc.
    # It collects all the parameters (including defaults) and passes them to the command function
    # The command function then passes these to load_transformer_config as config_kwargs
    config_kwargs = {
        # Base config with their defaults from Click options
        "log_dir": "./test_logs",  # From --log-dir
        "max_retries": 5,  # From --max-retries
        "num_workers": 3,  # From --num-workers
        "retry_delay": 1,  # From --retry-delay
        "log_every": 500,  # From --log-every
        "offset": 0,  # From --offset
        # CLI option values (as if passed on command line)
        "db_user": "source_user",  # From --source-db-user
        "db_host": "source.host",  # From --source-db-host
        "db_name": "source_db",  # From --source-db-name
        "table_name": "source_table",  # From --source-table-name
        "dest_table_name": "dest_table",  # From --dest-table-name
        "batch_size": 100,  # From --batch-size
        "task_source_table_name": "task_table",  # From --task-source-table-name
    }

    # This is what happens in the CLI command function
    config = load_transformer_config(**config_kwargs)

    # Verify base config
    assert config.log_dir == "./test_logs"
    assert config.max_retries == 5
    assert config.num_workers == 3
    assert config.retry_delay == 1
    assert config.log_every == 500

    # Verify source database connection
    assert "host=source.host" in config.source_db_conn_str
    assert "user=source_user" in config.source_db_conn_str
    assert "password=source_pass" in config.source_db_conn_str
    assert "dbname=source_db" in config.source_db_conn_str

    # Verify destination uses source credentials
    assert "host=source.host" in config.dest_db_conn_str
    assert "user=source_user" in config.dest_db_conn_str
    assert "password=source_pass" in config.dest_db_conn_str
    assert "dbname=source_db" in config.dest_db_conn_str

    # Verify other settings
    assert config.source_table_name == "source_table"
    assert config.dest_table_name == "dest_table"
    assert config.mp_task_table_name == "task_table"
    assert config.batch_size == 100


def test_load_transformer_config_with_explicit_dest_from_click():
    """Test that transformer config loads correctly with explicit destination database when passed from Click"""
    # Set required environment variables for passwords
    os.environ["LEMATERIALFETCHER_DB_PASSWORD"] = "source_pass"
    os.environ["LEMATERIALFETCHER_DEST_DB_PASSWORD"] = "dest_pass"

    # When Click calls the command function with all CLI arguments
    # It collects all the parameters (including defaults) and passes them to the command function
    # The command function then passes these to load_transformer_config as config_kwargs
    config_kwargs = {
        # Base config with their defaults from Click options
        "log_dir": "./test_logs",
        "max_retries": 5,
        "num_workers": 3,
        "retry_delay": 1,
        "log_every": 500,
        "offset": 0,
        # Source DB options (as if passed on command line)
        "db_user": "source_user",
        "db_host": "source.host",
        "db_name": "source_db",
        "table_name": "source_table",
        # Destination DB options (as if passed on command line)
        "dest_db_user": "dest_user",
        "dest_db_host": "dest.host",
        "dest_db_name": "dest_db",
        "dest_table_name": "dest_table",
        # Other options
        "batch_size": 100,
        "task_source_table_name": "task_table",
    }

    # This is what happens in the CLI command function
    config = load_transformer_config(**config_kwargs)

    # Verify source database connection
    assert "host=source.host" in config.source_db_conn_str
    assert "user=source_user" in config.source_db_conn_str
    assert "password=source_pass" in config.source_db_conn_str
    assert "dbname=source_db" in config.source_db_conn_str

    # Verify destination uses explicit destination credentials
    assert "host=dest.host" in config.dest_db_conn_str
    assert "user=dest_user" in config.dest_db_conn_str
    assert "password=dest_pass" in config.dest_db_conn_str
    assert "dbname=dest_db" in config.dest_db_conn_str


def test_load_transformer_config_with_mixed_env_and_cli():
    """Test loading transformer config with a mix of environment vars and CLI options, like in real usage"""
    # Setup environment variables as if set in .env file
    os.environ["LEMATERIALFETCHER_LOG_DIR"] = "./env_logs"
    os.environ["LEMATERIALFETCHER_NUM_WORKERS"] = "4"
    os.environ["LEMATERIALFETCHER_DB_USER"] = "env_user"
    os.environ["LEMATERIALFETCHER_DB_PASSWORD"] = "env_pass"
    os.environ["LEMATERIALFETCHER_DB_NAME"] = "env_db"

    # When Click processes a command, it first looks for environment variables.
    # If those exist, it uses them as defaults. Then it applies any CLI options
    # that were provided, overriding the environment defaults.

    # In this scenario, we're simulating:
    # --log-dir=./cli_logs --source-db-host=cli.host --source-table-name=cli_source --dest-table-name=cli_table
    # being passed on the command line, which override the environment values

    config_kwargs = {
        # These would come from CLI and override env vars
        "log_dir": "./cli_logs",
        "db_host": "cli.host",
        "table_name": "cli_source",
        "dest_table_name": "cli_table",
        # These would be the defaults from environment (via Click's auto_envvar_prefix)
        # Click would have already loaded these values before calling the command
        "num_workers": 4,
        "db_user": "env_user",
        "db_name": "env_db",
        # These would be Click's hardcoded defaults where no env var or CLI option exists
        "max_retries": 3,  # Click default
        "retry_delay": 2,  # Click default
        "log_every": 1000,  # Click default
        "offset": 0,  # Click default
        "batch_size": 500,  # Click default
    }

    # This is what happens in the CLI command function
    config = load_transformer_config(**config_kwargs)

    # Verify config prioritizes CLI options over environment variables
    assert config.log_dir == "./cli_logs"  # CLI value used
    assert config.num_workers == 4  # Environment value used
    assert config.max_retries == 3  # Default value used

    # Verify source database uses a mix of CLI and environment values
    assert "host=cli.host" in config.source_db_conn_str  # CLI value
    assert "user=env_user" in config.source_db_conn_str  # Environment value
    assert "password=env_pass" in config.source_db_conn_str  # Environment value
    assert "dbname=env_db" in config.source_db_conn_str  # Environment value

    # Verify destination database falls back to source values
    assert "host=cli.host" in config.dest_db_conn_str  # Fallback from source
    assert "user=env_user" in config.dest_db_conn_str  # Fallback from source
    assert "password=env_pass" in config.dest_db_conn_str  # Fallback from source
    assert "dbname=env_db" in config.dest_db_conn_str  # Fallback from source

    # Verify CLI-provided table names are used
    assert config.source_table_name == "cli_source"  # CLI value
    assert config.dest_table_name == "cli_table"  # CLI value
