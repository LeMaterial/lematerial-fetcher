# Copyright 2025 Entalpic
import os
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from lematerial_fetcher.cli import cli
from lematerial_fetcher.utils.config import (
    FetcherConfig,
    TransformerConfig,
)


# Use pytest fixture to properly patch load_dotenv
@pytest.fixture(autouse=True)
def mock_load_dotenv():
    """Prevent dotenv from loading environment variables during tests"""
    with patch("lematerial_fetcher.cli.load_dotenv"):
        yield


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


def test_help_output():
    """Test that the CLI produces expected help output"""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "A CLI tool to fetch materials from various sources" in result.output
    assert "mp" in result.output
    assert "alexandria" in result.output
    assert "oqmd" in result.output


def test_cli_subcommands():
    """Test that subcommands are correctly registered and can be invoked"""
    runner = CliRunner()

    # Test MP subcommand help
    result = runner.invoke(cli, ["mp", "--help"])
    assert result.exit_code == 0
    assert "Commands for fetching data from Materials Project" in result.output
    assert "fetch" in result.output
    assert "transform" in result.output

    # Test Alexandria subcommand help
    result = runner.invoke(cli, ["alexandria", "--help"])
    assert result.exit_code == 0
    assert "Commands for fetching data from Alexandria" in result.output
    assert "fetch" in result.output
    assert "transform" in result.output

    # Test OQMD subcommand help
    result = runner.invoke(cli, ["oqmd", "--help"])
    assert result.exit_code == 0
    assert "Commands for fetching data from OQMD" in result.output
    assert "fetch" in result.output
    assert "transform" in result.output


@patch("lematerial_fetcher.cli.MPFetcher")
@patch("lematerial_fetcher.cli.load_fetcher_config")
def test_mp_fetch_passes_cli_args_to_config(mock_load_config, mock_fetcher):
    """Test that MP fetch command passes CLI args to the config loader"""
    # Setup config mock
    mock_config = FetcherConfig(
        log_dir="./logs",
        max_retries=3,
        num_workers=2,
        retry_delay=2,
        log_every=1000,
        page_offset=0,
        page_limit=10,
        base_url="https://api.test.com",
        table_name="test_table",
        db_conn_str="db_conn_string",
        mp_bucket_name="test-bucket",
        mp_bucket_prefix="test/prefix",
    )
    # Make the config loader return the mock config
    mock_load_config.return_value = mock_config

    # Control the called fetcher instance
    mock_fetcher_instance = mock_fetcher.return_value

    # Run the CLI command with override arguments
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "mp",
            "fetch",
            "--db-user",
            "cli_user",
            "--table-name",
            "cli_table",
            "--num-workers",
            "5",
        ],
    )

    # Verify the command ran successfully
    assert result.exit_code == 0

    # Verify config was loaded with correct arguments
    mock_load_config.assert_called_once()
    call_kwargs = mock_load_config.call_args[1]
    assert call_kwargs["db_user"] == "cli_user"
    assert call_kwargs["table_name"] == "cli_table"
    assert call_kwargs["num_workers"] == 5

    # Verify fetcher was created and run
    mock_fetcher.assert_called_once_with(config=mock_config, debug=False)
    mock_fetcher_instance.fetch.assert_called_once()


@patch("lematerial_fetcher.cli.MPTrajectoryTransformer")
@patch("lematerial_fetcher.cli.load_transformer_config")
def test_mp_transform_passes_cli_args_to_config(mock_load_config, mock_transformer):
    """Test that MP transform command passes CLI args to the config loader"""
    # Setup config mock
    mock_config = TransformerConfig(
        log_dir="./logs",
        max_retries=3,
        num_workers=2,
        retry_delay=2,
        log_every=1000,
        page_offset=0,
        page_limit=10,
        source_db_conn_str="source_conn_string",
        dest_db_conn_str="dest_conn_string",
        source_table_name="source_table",
        dest_table_name="dest_table",
        batch_size=500,
    )
    mock_load_config.return_value = mock_config

    # Setup transformer mock
    mock_transformer_instance = mock_transformer.return_value

    # Run the CLI command
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "mp",
            "transform",
            "--traj",  # Monkey patch the trajectory transformer
            "--db-user",
            "cli_src_user",
            "--table-name",
            "cli_src_table",
            "--dest-db-user",
            "cli_dest_user",
            "--dest-table-name",
            "cli_dest_table",
            "--batch-size",
            "100",
        ],
    )

    # Verify the command ran successfully
    assert result.exit_code == 0

    # Verify config was loaded with correct arguments
    mock_load_config.assert_called_once()
    call_kwargs = mock_load_config.call_args[1]
    assert call_kwargs["db_user"] == "cli_src_user"
    assert call_kwargs["table_name"] == "cli_src_table"
    assert call_kwargs["dest_table_name"] == "cli_dest_table"
    assert call_kwargs["batch_size"] == 100

    mock_transformer.assert_called_once_with(config=mock_config, debug=False)
    mock_transformer_instance.transform.assert_called_once()


def test_cli_args_override_env_vars():
    """Test that CLI arguments take precedence over environment variables"""

    os.environ["LEMATERIALFETCHER_LOG_DIR"] = "./env_logs"
    os.environ["LEMATERIALFETCHER_NUM_WORKERS"] = "2"
    os.environ["LEMATERIALFETCHER_SOURCE_DB_HOST"] = "env.host"
    os.environ["LEMATERIALFETCHER_SOURCE_DB_USER"] = "env_user"
    os.environ["LEMATERIALFETCHER_SOURCE_DB_PASSWORD"] = "env_pass"
    os.environ["LEMATERIALFETCHER_SOURCE_DB_NAME"] = "env_db"
    os.environ["LEMATERIALFETCHER_SOURCE_TABLE_NAME"] = "env_table"
    os.environ["LEMATERIALFETCHER_DEST_TABLE_NAME"] = "env_dest_table"

    with patch("lematerial_fetcher.cli.load_transformer_config") as mock_load_config:
        # Mock the transformer to avoid actually running it
        with patch("lematerial_fetcher.cli.MPTransformer"):
            # Run the CLI command with a mix of CLI args and relying on env vars
            runner = CliRunner()
            result = runner.invoke(
                cli,
                [
                    "mp",
                    "transform",
                    "--log-dir",
                    "./cli_logs",  # Should override LEMATERIALFETCHER_LOG_DIR
                    "--num-workers",
                    "5",  # Should override LEMATERIALFETCHER_NUM_WORKERS
                    "--db-host",
                    "cli.host",  # Should override LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_HOST
                ],
            )

            assert result.exit_code == 0

            # CLI arguments should override environment variables
            call_kwargs = mock_load_config.call_args[1]
            assert call_kwargs["log_dir"] == "./cli_logs"
            assert call_kwargs["num_workers"] == 5
            assert call_kwargs["db_host"] == "cli.host"


def test_env_vars_pass_to_config():
    """Test that environment variables are passed to the config loader"""

    with patch("lematerial_fetcher.cli.load_transformer_config") as mock_load_config:
        with patch("lematerial_fetcher.cli.MPTransformer"):
            runner = CliRunner()
            # Pass the environment variables directly instead of expecting Click to read from os.environ
            env = {
                "LEMATERIALFETCHER_DB_USER": "src_user",
                "LEMATERIALFETCHER_DB_PASSWORD": "src_pass",
                "LEMATERIALFETCHER_DB_HOST": "src.host",
                "LEMATERIALFETCHER_DB_NAME": "src_db",
                "LEMATERIALFETCHER_TABLE_NAME": "src_table",
                "LEMATERIALFETCHER_DEST_TABLE_NAME": "dest_table",
            }
            result = runner.invoke(
                cli,
                ["mp", "transform"],
                env=env,
                auto_envvar_prefix="LEMATERIALFETCHER",
            )

            assert result.exit_code == 0

            mock_load_config.assert_called_once()
            call_kwargs = mock_load_config.call_args[1]
            assert call_kwargs["db_user"] == "src_user"
            assert call_kwargs["table_name"] == "src_table"
            assert call_kwargs["dest_table_name"] == "dest_table"
