import click


def add_common_options(f):
    """Add common options to a command."""
    decorators = [
        click.option("--db-conn-str", type=str, help="Database connection string."),
        click.option(
            "--num-workers",
            type=int,
            help="Number of workers to use for parallel processing.",
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
            help="Offset in the source table (ordered by id) to start fetching data from.",
        ),
        click.option(
            "--table-name",
            type=str,
            help="Table name to dump the fetched data.",
        ),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


def add_transformer_options(f):
    """Add transformer options to a command."""
    decorators = [
        click.option("--traj", is_flag=True, help="Transform trajectory data."),
        click.option(
            "--source-table-name",
            type=str,
            help="Source table name containing the fetched data.",
        ),
        click.option(
            "--dest-table-name",
            type=str,
            help="Target table name where to store the transformed data.",
        ),
        click.option(
            "--batch-size", type=int, help="Batch size to fetch and transform data."
        ),
        click.option(
            "--offset",
            type=int,
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
        click.option("--mysql-host", type=str, help="MySQL host."),
        click.option("--mysql-user", type=str, help="MySQL user."),
        click.option("--mysql-password", type=str, help="MySQL password."),
        click.option("--mysql-database", type=str, help="MySQL database."),
        click.option("--mysql-cert-path", type=str, help="MySQL certificate path."),
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
