# Copyright 2025 Entalpic
"""
Source code for the ``materialfetcher`` Command-Line Interface (CLI).

Learn how to use with:

.. code-block:: bash

    $ materialfetcher --help
"""

import os
from pathlib import Path

import click
from dotenv import load_dotenv

from lematerial_fetcher.fetcher.alexandria.fetch import (
    AlexandriaFetcher,
    AlexandriaTrajectoryFetcher,
)
from lematerial_fetcher.fetcher.alexandria.transform import (
    AlexandriaTrajectoryTransformer,
    AlexandriaTransformer,
)
from lematerial_fetcher.fetcher.mp.fetch import MPFetcher
from lematerial_fetcher.fetcher.mp.transform import (
    MPTrajectoryTransformer,
    MPTransformer,
)
from lematerial_fetcher.fetcher.oqmd.fetch import OQMDFetcher
from lematerial_fetcher.fetcher.oqmd.transform import (
    OQMDTrajectoryTransformer,
    OQMDTransformer,
)
from lematerial_fetcher.push import Push
from lematerial_fetcher.utils.cli import (
    add_common_options,
    add_fetch_options,
    add_mp_fetch_options,
    add_mysql_options,
    add_push_options,
    add_transformer_options,
    get_default_mp_bucket_name,
)
from lematerial_fetcher.utils.config import (
    load_fetcher_config,
    load_push_config,
    load_transformer_config,
)
from lematerial_fetcher.utils.logging import logger

load_dotenv(override=True)

_ALEXANDRIA_BASE_URL = {
    "pbesol": "https://alexandria.icams.rub.de/pbesol/v1/structures",
    "pbe": "https://alexandria.icams.rub.de/pbe/v1/structures",
    "scan": "https://alexandria.icams.rub.de/scan/v1/structures",
}
_ALEXANDRIA_TRAJECTORY_BASE_URL = {
    "pbe": "https://alexandria.icams.rub.de/data/pbe/geo_opt_paths/",
    "pbesol": "https://alexandria.icams.rub.de/data/pbesol/geo_opt_paths/",
}
_OQMD_BASE_URL = "https://oqmd.org/download/"


@click.group()
@click.option(
    "--debug",
    is_flag=True,
    help="Run all operations in the main process for debugging purposes.",
)
@click.option(
    "--cache-dir",
    type=str,
    help="Directory to store temporary data. If not provided, will use ~/.cache/lematerial_fetcher.",
)
@click.pass_context
def cli(ctx, debug, cache_dir):
    """A CLI tool to fetch materials from various sources.

    All options can be set via environment variables with the prefix LEMATERIALFETCHER_.
    For example, --debug can be set via LEMATERIALFETCHER_DEBUG=true.
    """
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug
    if cache_dir:
        ctx.obj["cache_dir"] = cache_dir
        os.environ["LEMATERIALFETCHER_CACHE_DIR"] = cache_dir
    else:
        os.environ["LEMATERIALFETCHER_CACHE_DIR"] = str(
            (Path.home() / ".cache" / "lematerial_fetcher").resolve()
        )


@click.group(name="mp")
@click.pass_context
def mp_cli(ctx):
    """Commands for fetching data from Materials Project."""
    pass


@click.group(name="alexandria")
@click.pass_context
def alexandria_cli(ctx):
    """Commands for fetching data from Alexandria."""
    pass


@click.group(name="oqmd")
@click.pass_context
def oqmd_cli(ctx):
    """Commands for fetching data from OQMD."""
    pass


cli.add_command(mp_cli)
cli.add_command(alexandria_cli)
cli.add_command(oqmd_cli)

# ------------------------------------------------------------------------------
# MP commands
# ------------------------------------------------------------------------------


@mp_cli.command(name="fetch")
@click.pass_context
@click.option(
    "--tasks",
    is_flag=True,
    help="Fetch task data from Materials Project. If false, fetch structure data.",
)
@add_common_options
@add_fetch_options
@add_mp_fetch_options
def mp_fetch(ctx, tasks, **config_kwargs):
    """Fetch materials from Materials Project.

    This command fetches raw materials data from Materials Project and stores them in a database.
    Options can be provided via command line arguments or environment variables.
    See individual option help for corresponding environment variables.
    """

    # Set default bucket name and prefix if not provided in either the CLI or the environment
    default_mp_bucket_name, default_mp_bucket_prefix = get_default_mp_bucket_name(tasks)
    if "mp_bucket_name" not in config_kwargs or not config_kwargs["mp_bucket_name"]:
        config_kwargs["mp_bucket_name"] = default_mp_bucket_name
    if "mp_bucket_prefix" not in config_kwargs or not config_kwargs["mp_bucket_prefix"]:
        config_kwargs["mp_bucket_prefix"] = default_mp_bucket_prefix

    config_kwargs["base_url"] = "DUMMY_BASE_URL"  # Not needed for MP

    config = load_fetcher_config(**config_kwargs)
    try:
        fetcher = MPFetcher(config=config, debug=ctx.obj["debug"])
        fetcher.fetch()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


@mp_cli.command(name="transform")
@click.option(
    "--traj",
    is_flag=True,
    help="Transform trajectory data from Material Project.",
)
@click.pass_context
@add_common_options
@add_transformer_options
def mp_transform(ctx, traj, **config_kwargs):
    """Transform materials from Material Project.

    This command processes materials from Material Project to store them in a clean format.
    Options can be provided via command line arguments or environment variables.
    See individual option help for corresponding environment variables.
    """
    config = load_transformer_config(**config_kwargs)
    try:
        if traj:
            transformer = MPTrajectoryTransformer(config=config, debug=ctx.obj["debug"])
        else:
            transformer = MPTransformer(config=config, debug=ctx.obj["debug"])
        transformer.transform()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


# ------------------------------------------------------------------------------
# Alexandria commands
# ------------------------------------------------------------------------------


@alexandria_cli.command(name="fetch")
@click.pass_context
@click.option(
    "--traj",
    is_flag=True,
    help="Fetch trajectory data from Alexandria.",
)
@click.option(
    "--base-url",
    type=str,
    help="Base URL for Alexandria to fetch data from. Can be set via LEMATERIALFETCHER_API_BASE_URL environment variable.",
)
@click.option(
    "--functional",
    type=str,
    default="pbe",
    help="Functional to fetch data from. Can be set via LEMATERIALFETCHER_FUNCTIONAL environment variable.",
)
@add_common_options
@add_fetch_options
def alexandria_fetch(ctx, traj, base_url, functional, **config_kwargs):
    """Fetch materials from Alexandria.

    This command fetches materials from Alexandria and stores them in a database.
    Options can be provided via command line arguments or environment variables.
    See individual option help for corresponding environment variables.
    """
    if not base_url:
        if traj:
            assert functional in _ALEXANDRIA_TRAJECTORY_BASE_URL, (
                f"Functional {functional} not supported for trajectory data. Must be one of {_ALEXANDRIA_TRAJECTORY_BASE_URL.keys()}."
            )
            config_kwargs["base_url"] = _ALEXANDRIA_TRAJECTORY_BASE_URL[functional]
            logger.info(
                f"Using Alexandria trajectory base URL: {config_kwargs['base_url']}. You can change this by setting the --base-url option."
            )
        else:
            assert functional in _ALEXANDRIA_BASE_URL, (
                f"Functional {functional} not supported for structure data. Must be one of {_ALEXANDRIA_BASE_URL.keys()}."
            )
            config_kwargs["base_url"] = _ALEXANDRIA_BASE_URL[functional]
            logger.info(
                f"Using Alexandria structure base URL: {config_kwargs['base_url']}. You can change this by setting the --base-url option."
            )

    config = load_fetcher_config(**config_kwargs)
    try:
        if traj:
            fetcher = AlexandriaTrajectoryFetcher(config=config, debug=ctx.obj["debug"])
        else:
            fetcher = AlexandriaFetcher(config=config, debug=ctx.obj["debug"])
        fetcher.fetch()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


@alexandria_cli.command(name="transform")
@click.pass_context
@click.option(
    "--traj",
    is_flag=True,
    help="Transform trajectory data from Alexandria.",
)
@add_common_options
@add_transformer_options
def alexandria_transform(ctx, traj, **config_kwargs):
    """Transform materials from Alexandria.

    This command processes materials from Alexandria to store them in a clean format.
    Options can be provided via command line arguments or environment variables.
    See individual option help for corresponding environment variables.
    """
    config = load_transformer_config(**config_kwargs)
    try:
        if traj:
            transformer = AlexandriaTrajectoryTransformer(
                config=config, debug=ctx.obj["debug"]
            )
        else:
            transformer = AlexandriaTransformer(config=config, debug=ctx.obj["debug"])
        transformer.transform()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


# ------------------------------------------------------------------------------
# OQMD commands
# ------------------------------------------------------------------------------


@oqmd_cli.command(name="fetch")
@click.pass_context
@click.option(
    "--base-url",
    type=str,
    help="Base URL for OQMD to fetch data from. Can be set via LEMATERIALFETCHER_API_BASE_URL environment variable.",
)
@add_common_options
@add_fetch_options
@add_mysql_options
def oqmd_fetch(ctx, base_url, **config_kwargs):
    """Fetch materials from OQMD.

    This command fetches materials from OQMD and stores them in a database.
    Options can be provided via command line arguments or environment variables.
    See individual option help for corresponding environment variables.
    """
    if not base_url:
        config_kwargs["base_url"] = _OQMD_BASE_URL
        logger.info(
            f"Using OQMD base URL: {config_kwargs['base_url']}. You can change this by setting the --base-url option."
        )

    config = load_fetcher_config(**config_kwargs)
    try:
        fetcher = OQMDFetcher(config=config, debug=ctx.obj["debug"])
        fetcher.fetch()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


@oqmd_cli.command(name="transform")
@click.pass_context
@click.option(
    "--traj",
    is_flag=True,
    help="Transform trajectory data from OQMD.",
)
@add_common_options
@add_transformer_options
@add_mysql_options
def oqmd_transform(ctx, traj, **config_kwargs):
    """Transform materials from OQMD.

    This command transforms materials from OQMD.
    Options can be provided via command line arguments or environment variables.
    See individual option help for corresponding environment variables.
    """
    config = load_transformer_config(**config_kwargs)
    try:
        if traj:
            transformer = OQMDTrajectoryTransformer(
                config=config, debug=ctx.obj["debug"]
            )
        else:
            transformer = OQMDTransformer(config=config, debug=ctx.obj["debug"])
        transformer.transform()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


# ------------------------------------------------------------------------------
# Push commands
# ------------------------------------------------------------------------------


@cli.command(name="push")
@click.pass_context
@click.option(
    "--data-type",
    type=str,
    default="optimade",
    help="Type of data to push, one of ['optimade', 'trajectories', 'any'].",
)
@add_common_options
@add_push_options
def push(ctx, data_type, **config_kwargs):
    """Push materials to Hugging Face.

    This command pushes data from a database to a Hugging Face repository.
    Options can be provided via command line arguments or environment variables.
    See individual option help for corresponding environment variables.
    """
    try:
        config = load_push_config(**config_kwargs)

        push = Push(
            config=config,
            data_type=data_type,
            debug=ctx.obj["debug"],
        )

        push.push()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------


def main():
    """Run the CLI.

    This maps arguments to the following environment variables:
    - --db-user maps to LEMATERIALFETCHER_DB_USER
    - --num-workers maps to LEMATERIALFETCHER_NUM_WORKERS

    This lets Click handle environment variables consistently before they're
    passed to the configuration system.
    """

    # Click's auto_envvar_prefix feature can be used to automatically map CLI options
    # to environment variables with the prefix LEMATERIALFETCHER_.
    # However, it also propagates the prefix of the command, which we don't want currently.

    cli()


if __name__ == "__main__":
    logger.set_level("INFO")
    main()
