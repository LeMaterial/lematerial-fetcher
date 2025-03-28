import click


def add_common_options(f):
    """Add common options to a command."""
    decorators = [
        # Database options
        click.option(
            "--db-conn-str",
            type=str,
            help="Database connection string. If provided, overrides individual DB credentials.",
        ),
        click.option(
            "--db-user",
            type=str,
            help="Database username. Password must be set via the LEMATERIALFETCHER_DB_PASSWORD environment variable.",
        ),
        click.option("--db-host", type=str, default="localhost", help="Database host."),
        click.option("--db-name", type=str, help="Database name."),
        # General options
        click.option(
            "--num-workers",
            type=int,
            help="Number of workers to use for parallel processing.",
        ),
        click.option(
            "--log-dir",
            type=str,
            default="./logs",
            help="Directory where log files will be stored.",
        ),
        click.option(
            "--max-retries",
            type=int,
            default=3,
            help="Maximum number of retry attempts.",
        ),
        click.option(
            "--retry-delay",
            type=int,
            default=2,
            help="Delay between retry attempts in seconds.",
        ),
        click.option(
            "--log-every", type=int, default=1000, help="Log progress every N items."
        ),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


def add_fetch_options(f):
    """Add fetch options to a command."""
    decorators = [
        click.option(
            "--offset",
            type=int,
            default=0,
            help="Offset in the source table (ordered by id) to start fetching data from.",
        ),
        click.option(
            "--table-name", type=str, help="Table name to dump the fetched data."
        ),
        click.option(
            "--limit",
            type=int,
            default=10,
            help="Number of items to fetch per request.",
        ),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


def add_transformer_options(f):
    """Add transformer options to a command."""
    decorators = [
        click.option("--traj", is_flag=True, help="Transform trajectory data."),
        # Source database options
        click.option(
            "--table-name", type=str, help="Table name to dump the fetched data."
        ),
        # Destination database options
        click.option(
            "--dest-db-host",
            type=str,
            default="localhost",
            help="Destination database host.",
        ),
        click.option(
            "--dest-db-user",
            type=str,
            help="Destination database username. Password must be set via the LEMATERIALFETCHER_TRANSFORMER_DEST_DB_PASSWORD environment variable.",
        ),
        click.option("--dest-db-name", type=str, help="Destination database name."),
        click.option(
            "--dest-table-name",
            type=str,
            help="Target table name where to store the transformed data.",
        ),
        # Processing options
        click.option(
            "--batch-size",
            type=int,
            default=500,
            help="Batch size to fetch and transform data.",
        ),
        click.option(
            "--offset",
            type=int,
            default=0,
            help="Offset in the source table (ordered by id) to start fetching and transforming data from.",
        ),
        click.option(
            "--task-source-table-name",
            type=str,
            help="Alternative source table name that is needed for some transformations (e.g. MP).",
        ),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


def add_mysql_options(f):
    """Add MySQL options to a command."""
    decorators = [
        click.option("--mysql-host", type=str, default="localhost", help="MySQL host."),
        click.option(
            "--mysql-user",
            type=str,
            help="MySQL user. Password must be set via the LEMATERIALFETCHER_MYSQL_PASSWORD environment variable.",
        ),
        click.option(
            "--mysql-database", type=str, default="lematerial", help="MySQL database."
        ),
        click.option(
            "--mysql-cert-path",
            type=str,
            help="MySQL certificate path. Optional, only needed for SSL connections.",
        ),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


def get_default_mp_bucket_name(tasks: bool = False) -> str:
    default_mp_bucket_name = (
        "materialsproject-parsed" if tasks else "materialsproject-build"
    )
    default_mp_bucket_prefix = "tasks" if tasks else "collections"

    return default_mp_bucket_name, default_mp_bucket_prefix


def add_mp_fetch_options(f):
    """Add MP fetch options to a command."""
    decorators = [
        click.option(
            "--mp-bucket-name",
            type=str,
            help="MP bucket name. For tasks, use 'materialsproject-parsed', for structures use 'materialsproject-build'. If not specified, will use the defaults based on --tasks.",
        ),
        click.option(
            "--mp-bucket-prefix",
            type=str,
            help="MP bucket prefix. For tasks, use 'tasks', for structures use 'structures'. If not specified, will use the defaults based on --tasks.",
        ),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f
