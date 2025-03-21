import click


def add_db_options(f):
    """Add database connection options to a command."""
    decorators = [
        click.option("--db-user", type=str, help="Database user."),
        click.option("--db-password", type=str, help="Database password."),
        click.option("--db-host", type=str, help="Database host."),
        click.option("--db-port", type=int, help="Database port."),
        click.option("--db-name", type=str, help="Database name."),
        click.option("--table-name", type=str, help="Table name."),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f
