# Copyright 2025 Entalpic
"""
Source code for the ``materialfetcher`` Command-Line Interface (CLI).

Learn how to use with:

.. code-block:: bash

    $ materialfetcher --help
"""

import click

from material_fetcher.fetcher.alexandria.fetch import AlexandriaFetcher
from material_fetcher.fetcher.mp.fetch import MPFetcher
from material_fetcher.fetcher.mp.transform import MPTransformer
from material_fetcher.utils.logging import logger


@click.group()
def cli():
    """A CLI tool to fetch materials from various sources."""
    pass


@click.group(name="mp")
def mp_cli():
    """Commands for fetching data from Materials Project."""
    pass


@click.group(name="alexandria")
def alexandria_cli():
    """Commands for fetching data from Alexandria."""
    pass


cli.add_command(mp_cli)
cli.add_command(alexandria_cli)


@mp_cli.command(name="fetch")
def mp_fetch():
    """Fetch materials from Materials Project."""
    try:
        fetcher = MPFetcher()
        fetcher.fetch()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


@mp_cli.command(name="transform")
def mp_transform():
    """Transform materials from Material Project."""
    try:
        transformer = MPTransformer()
        transformer.transform()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


@alexandria_cli.command(name="fetch")
def alexandria_fetch():
    """Fetch materials from Alexandria."""
    try:
        fetcher = AlexandriaFetcher()
        fetcher.fetch()
    except KeyboardInterrupt:
        logger.fatal("\nAborted.", exit=1)


def main():
    """Run the CLI."""
    cli(auto_envvar_prefix="MATERIAL_FETCHER")


if __name__ == "__main__":
    logger.set_level("INFO")
    main()
