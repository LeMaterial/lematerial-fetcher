# Copyright 2025 Entalpic
import click


def add_common_options(f):
    """Add common options to a command."""
    decorators = [
        # Database options
        click.option(
            "--db-conn-str",
            type=str,
            envvar="LEMATERIALFETCHER_DB_CONN_STR",
            help="Database connection string. If provided, overrides individual DB credentials.",
        ),
        click.option(
            "--db-user",
            type=str,
            envvar="LEMATERIALFETCHER_DB_USER",
            help="Database username. Password must be set via the LEMATERIALFETCHER_DB_PASSWORD environment variable.",
        ),
        click.option(
            "--db-host",
            type=str,
            default="localhost",
            envvar="LEMATERIALFETCHER_DB_HOST",
            help="Database host.",
        ),
        click.option(
            "--db-name",
            type=str,
            default="lematerial",
            envvar="LEMATERIALFETCHER_DB_NAME",
            help="Database name.",
        ),
        # General options
        click.option(
            "--num-workers",
            type=int,
            envvar="LEMATERIALFETCHER_NUM_WORKERS",
            help="Number of workers to use for parallel processing.",
        ),
        click.option(
            "--log-dir",
            type=str,
            default="./logs",
            envvar="LEMATERIALFETCHER_LOG_DIR",
            help="Directory where log files will be stored.",
        ),
        click.option(
            "--max-retries",
            type=int,
            default=3,
            envvar="LEMATERIALFETCHER_MAX_RETRIES",
            help="Maximum number of retry attempts.",
        ),
        click.option(
            "--retry-delay",
            type=int,
            default=2,
            envvar="LEMATERIALFETCHER_RETRY_DELAY",
            help="Delay between retry attempts in seconds.",
        ),
        click.option(
            "--log-every",
            type=int,
            default=1000,
            envvar="LEMATERIALFETCHER_LOG_EVERY",
            help="Log progress every N items.",
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
            envvar="LEMATERIALFETCHER_OFFSET",
            help="Offset in the source table (ordered by id) to start fetching data from.",
        ),
        click.option(
            "--table-name",
            type=str,
            envvar="LEMATERIALFETCHER_TABLE_NAME",
            help="Table name to dump the fetched data.",
        ),
        click.option(
            "--limit",
            type=int,
            default=500,
            envvar="LEMATERIALFETCHER_LIMIT",
            help="Number of items to fetch per request.",
        ),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


def add_transformer_options(f):
    """Add transformer options to a command."""
    decorators = [
        click.option(
            "--traj",
            is_flag=True,
            envvar="LEMATERIALFETCHER_TRAJ",
            help="Transform trajectory data.",
        ),
        # Source database options
        click.option(
            "--table-name",
            type=str,
            envvar="LEMATERIALFETCHER_TABLE_NAME",
            help="Table name to dump the fetched data.",
        ),
        # Destination database options
        click.option(
            "--dest-db-host",
            type=str,
            default="localhost",
            envvar="LEMATERIALFETCHER_DEST_DB_HOST",
            help="Destination database host.",
        ),
        click.option(
            "--dest-db-user",
            type=str,
            envvar="LEMATERIALFETCHER_DEST_DB_USER",
            help="Destination database username. Password must be set via the LEMATERIALFETCHER_DEST_DB_PASSWORD environment variable.",
        ),
        click.option(
            "--dest-db-name",
            type=str,
            default="lematerial",
            envvar="LEMATERIALFETCHER_DEST_DB_NAME",
            help="Destination database name.",
        ),
        click.option(
            "--dest-table-name",
            type=str,
            envvar="LEMATERIALFETCHER_DEST_TABLE_NAME",
            help="Target table name where to store the transformed data.",
        ),
        # Processing options
        click.option(
            "--batch-size",
            type=int,
            default=500,
            envvar="LEMATERIALFETCHER_BATCH_SIZE",
            help="Batch size to fetch and transform data. Use a larger batch size to speed up the transformation here.",
        ),
        click.option(
            "--db-fetch-batch-size",
            type=int,
            default=10,
            envvar="LEMATERIALFETCHER_DB_FETCH_BATCH_SIZE",
            help="Batch size to fetch data from the database. Use a smaller batch size to avoid memory issues.",
        ),
        click.option(
            "--offset",
            type=int,
            default=0,
            envvar="LEMATERIALFETCHER_OFFSET",
            help="Offset in the source table (ordered by id) to start fetching and transforming data from.",
        ),
        click.option(
            "--max-offset",
            type=int,
            envvar="LEMATERIALFETCHER_MAX_OFFSET",
            help="Maximum index in the source table to process up to. If not provided, all items will be processed.",
        ),
        click.option(
            "--task-source-table-name",
            type=str,
            envvar="LEMATERIALFETCHER_TASK_SOURCE_TABLE",
            help="Alternative source table name that is needed for some transformations (e.g. MP).",
        ),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


def add_mysql_options(f):
    """Add MySQL options to a command."""
    decorators = [
        click.option(
            "--mysql-host",
            type=str,
            default="localhost",
            envvar="LEMATERIALFETCHER_MYSQL_HOST",
            help="MySQL host.",
        ),
        click.option(
            "--mysql-user",
            type=str,
            envvar="LEMATERIALFETCHER_MYSQL_USER",
            help="MySQL user. Password must be set via the LEMATERIALFETCHER_MYSQL_PASSWORD environment variable.",
        ),
        click.option(
            "--mysql-database",
            type=str,
            default="lematerial",
            envvar="LEMATERIALFETCHER_MYSQL_DATABASE",
            help="MySQL database.",
        ),
        click.option(
            "--mysql-cert-path",
            type=str,
            envvar="LEMATERIALFETCHER_MYSQL_CERT_PATH",
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
    default_mp_bucket_prefix = "tasks_atomate2" if tasks else "collections"

    return default_mp_bucket_name, default_mp_bucket_prefix


def add_mp_fetch_options(f):
    """Add MP fetch options to a command."""
    decorators = [
        click.option(
            "--mp-bucket-name",
            type=str,
            envvar="LEMATERIALFETCHER_MP_BUCKET_NAME",
            help="MP bucket name. For tasks, use 'materialsproject-parsed', for structures use 'materialsproject-build'. If not specified, will use the defaults based on --tasks.",
        ),
        click.option(
            "--mp-bucket-prefix",
            type=str,
            envvar="LEMATERIALFETCHER_MP_BUCKET_PREFIX",
            help="MP bucket prefix. For tasks, use 'tasks', for structures use 'structures'. If not specified, will use the defaults based on --tasks.",
        ),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


def add_push_options(f):
    """Add push options to a command."""
    decorators = [
        click.option(
            "--table-name",
            type=str,
            envvar="LEMATERIALFETCHER_TABLE_NAME",
            multiple=True,
            help="Table name to push data from.",
        ),
        click.option(
            "--hf-repo-id",
            type=str,
            envvar="LEMATERIALFETCHER_HF_REPO_ID",
            help="Hugging Face repository ID.",
        ),
        click.option(
            "--hf-token",
            type=str,
            envvar="LEMATERIALFETCHER_HF_TOKEN",
            help="Hugging Face token.",
        ),
        click.option(
            "--data-dir",
            type=str,
            envvar="LEMATERIALFETCHER_DATA_DIR",
            help="Directory to store temporary data. If not provided, will use the cache directory.",
        ),
        click.option(
            "--chunk-size",
            type=int,
            default=1000,
            envvar="LEMATERIALFETCHER_CHUNK_SIZE",
            help="Number of rows to export from the database at a time.",
        ),
        click.option(
            "--max-rows",
            type=int,
            default=-1,
            envvar="LEMATERIALFETCHER_MAX_ROWS",
            help="Maximum number of rows to push. Will shuffle the data with a deterministic seed. If -1 (default), all rows will be pushed.",
        ),
        click.option(
            "--force-refresh",
            is_flag=True,
            envvar="LEMATERIALFETCHER_FORCE_REFRESH",
            help="Force refresh the cache.",
        ),
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f
