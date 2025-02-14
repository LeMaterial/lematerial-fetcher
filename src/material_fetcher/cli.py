# Copyright 2025 Entalpic
"""
Source code for the ``materialfetcher`` Command-Line Interface (CLI).

Learn how to use with:

.. code-block:: bash

    $ materialfetcher --help
"""

from cyclopts import App

from material_fetcher.fetcher.mp.fetch import fetch as fetch_mp
from material_fetcher.utils.logging import logger

_app = App(
    help="A CLI tool to fetch materials from various sources.",
)
""":py:class:`cyclopts.App`: The main CLI application."""

mp_app = App(name="mp", help="Commands for Material Project API interactions.")
alexandria_app = App(
    name="alexandria", help="Commands for Alexandria API interactions."
)

_app.command(mp_app)
_app.command(alexandria_app)


def app():
    """Run the CLI."""
    try:
        _app()
    except KeyboardInterrupt:
        logger.abort("\nAborted.", exit=1)


@mp_app.command(name="fetch")
def mp_fetch():
    """Fetch materials from Material Project."""
    fetch_mp()


@alexandria_app.command
def fetch():
    """Fetch materials from Alexandria."""
    pass


if __name__ == "__main__":
    logger.set_level("INFO")
    app()
