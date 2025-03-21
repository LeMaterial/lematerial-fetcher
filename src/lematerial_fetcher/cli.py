# Copyright 2025 Entalpic
"""
Source code for the ``materialfetcher`` Command-Line Interface (CLI).

Learn how to use with:

.. code-block:: bash

    $ materialfetcher --help
"""

import click

from lematerial_fetcher.fetcher.alexandria.fetch import AlexandriaFetcher
from lematerial_fetcher.fetcher.alexandria.transform import AlexandriaTransformer
from lematerial_fetcher.fetcher.mp.fetch import MPFetcher
from lematerial_fetcher.fetcher.mp.transform import (
    MPTrajectoryTransformer,
    MPTransformer,
)
from lematerial_fetcher.fetcher.oqmd.fetch import OQMDFetcher
from lematerial_fetcher.utils.logging import logger


@click.group()
@click.option(
    "--debug",
    is_flag=True,
    help="Run all operations in the main process for debugging purposes.",
)
@click.pass_context
def cli(ctx, debug):
    """A CLI tool to fetch materials from various sources."""
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug


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


@mp_cli.command(name="fetch")
@click.pass_context
def mp_fetch(ctx):
    """Fetch materials from Materials Project."""
    try:
        fetcher = MPFetcher(debug=ctx.obj["debug"])
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
def mp_transform(ctx, traj):
    """Transform materials from Material Project."""
    try:
        if traj:
            transformer = MPTrajectoryTransformer(debug=ctx.obj["debug"])
        else:
            transformer = MPTransformer(debug=ctx.obj["debug"])
        transformer.transform()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


@alexandria_cli.command(name="fetch")
@click.pass_context
def alexandria_fetch(ctx):
    """Fetch materials from Alexandria."""
    try:
        fetcher = AlexandriaFetcher(debug=ctx.obj["debug"])
        fetcher.fetch()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


@alexandria_cli.command(name="transform")
@click.pass_context
def alexandria_transform(ctx):
    """Transform materials from Alexandria."""
    try:
        transformer = AlexandriaTransformer(debug=ctx.obj["debug"])
        transformer.transform()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


@oqmd_cli.command(name="fetch")
@click.pass_context
def oqmd_fetch(ctx):
    """Fetch materials from OQMD."""
    try:
        fetcher = OQMDFetcher(debug=ctx.obj["debug"])
        fetcher.fetch()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


def main():
    """Run the CLI."""
    cli(auto_envvar_prefix="LEMATERIAL_FETCHER")


if __name__ == "__main__":
    logger.set_level("INFO")
    main()
