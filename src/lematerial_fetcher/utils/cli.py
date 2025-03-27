import click


def add_db_options(f):
    """Add database connection options to a command."""
    decorators = [
        click.option("--table-name", type=str, help="Table name."),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f
