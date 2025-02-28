# Copyright 2025 Entalpic
"""
Source code for the ``materialfetcher`` Command-Line Interface (CLI).

Learn how to use with:

.. code-block:: bash

    $ materialfetcher --help
"""

import click

from material_fetcher.fetcher.mp.fetch import fetch as fetch_mp
from material_fetcher.fetcher.mp.transform import (
    filter_mp_structure,
    transform_mp_structure,
)
from material_fetcher.transform.transform import transform
from material_fetcher.utils.logging import logger


@click.group()
def cli():
    """A CLI tool to fetch materials from various sources."""
    pass


@click.group(name="mp")
def mp_cli():
    """Commands for Material Project API interactions."""
    pass


@click.group(name="alexandria")
def alexandria_cli():
    """Commands for Alexandria API interactions."""
    pass


cli.add_command(mp_cli)
cli.add_command(alexandria_cli)


@mp_cli.command(name="fetch")
def mp_fetch():
    """Fetch materials from Material Project."""
    try:
        fetch_mp()
    except KeyboardInterrupt:
        logger.abort("\nAborted.", exit=1)


@mp_cli.command(name="transform")
def mp_transform():
    """Transform materials from Material Project."""
    try:
        transform(transform_mp_structure, filter_mp_structure)
    except KeyboardInterrupt:
        logger.abort("\nAborted.", exit=1)


@alexandria_cli.command(name="fetch")
def alexandria_fetch():
    """Fetch materials from Alexandria."""
    pass


def main():
    """Run the CLI."""
    cli(auto_envvar_prefix="MATERIAL_FETCHER")


if __name__ == "__main__":
    logger.set_level("INFO")
    main()
