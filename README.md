# lematerial-fetcher

`lematerial-fetcher` is designed to fetch data from a specified OPTIMADE's compatible JSON-API, process it, and store it in a PostgreSQL database. It is highly concurrent, to handle data fetching and processing efficiently.

The objective is to retrieve information from various OPTIMADE sources and establish a local database. This database will enable us to process and utilize the data according to our specific requirements, which can then be uploaded to an online and easily accessible place like Hugging Face.

## Prerequisites

- Python 3.11 or later
- PostgreSQL database
- Environment variables set for configuration

## Configuration

lematerial-fetcher can be configured in two ways:

1. Using command-line options when running the tool. These take precedence over environment variables.
2. Using environment variables (prefixed with `LEMATERIALFETCHER_`)

### Environment Variable Handling

The tool uses Click's automatic environment variable processing to convert between command-line options and environment variables:

1. Every CLI option has a corresponding environment variable
2. The environment variable is the CLI option name converted to uppercase, with hyphens replaced by underscores, and prefixed with `LEMATERIALFETCHER_`
3. Command-line options always take precedence over environment variables

For example:
- CLI option `--db-user` corresponds to environment variable `LEMATERIALFETCHER_DB_USER`
- CLI option `--num-workers` corresponds to environment variable `LEMATERIALFETCHER_NUM_WORKERS`

### Authentication to databases

Passwords are never accepted as command-line arguments. 
Instead, they must always be provided through environment variables:

- `LEMATERIALFETCHER_DB_PASSWORD` - Main database password
- `LEMATERIALFETCHER_MYSQL_PASSWORD` - MySQL database password
- `LEMATERIALFETCHER_SOURCE_DB_PASSWORD` - Source database password
- `LEMATERIALFETCHER_DEST_DB_PASSWORD` - Destination database password (used for transformers)

This prevents passwords from being visible in command history or process listings.

A template `.env.example` file is provided in the repository that you can copy to `.env` and customize according to your needs.

### Database Configuration

You can configure database connections in two ways:

1. **Using individual parameters**: Set user, host, and database name as CLI options; password must be set via environment variable
   ```bash
   # Set password in environment
   export LEMATERIALFETCHER_DB_PASSWORD=mypassword
   # Then run command
   lematerial-fetcher mp fetch --db-user username --db-name database_name
   ```

2. **Using a connection string**: Provide a complete PostgreSQL connection string
   ```bash
   lematerial-fetcher mp fetch --db-conn-str="host=localhost user=username password=password dbname=database_name sslmode=disable"
   ```

If both are provided, the connection string takes precedence.

## Common Environment Variables

### Base Configuration
- `LEMATERIALFETCHER_LOG_DIR`: Directory for storing logs (default: `./logs`)
- `LEMATERIALFETCHER_MAX_RETRIES`: Maximum number of retry attempts (default: `3`)
- `LEMATERIALFETCHER_NUM_WORKERS`: Number of parallel workers (default: `2`)
- `LEMATERIALFETCHER_RETRY_DELAY`: Delay between retry attempts in seconds (default: `2`)
- `LEMATERIALFETCHER_PAGE_LIMIT`: Number of items to fetch per page (default: `10`)
- `LEMATERIALFETCHER_PAGE_OFFSET`: Starting page offset (default: `0`)

### Database Configuration
- `LEMATERIALFETCHER_DB_USER`: PostgreSQL database username **(Required)**
- `LEMATERIALFETCHER_DB_HOST`: PostgreSQL database host (default: `localhost`)
- `LEMATERIALFETCHER_DB_NAME`: PostgreSQL database name **(Required)**
- `LEMATERIALFETCHER_TABLE_NAME`: Table name to store fetched data **(Required)**

### API Configuration
- `LEMATERIALFETCHER_API_BASE_URL`: Base URL for the API endpoint **(Required)**

### Materials Project Configuration
- `LEMATERIALFETCHER_MP_BUCKET_NAME`: MP bucket name (default depends on `--tasks` flag)
- `LEMATERIALFETCHER_MP_BUCKET_PREFIX`: MP bucket prefix (default depends on `--tasks` flag)

### MySQL Configuration (for OQMD)
- `LEMATERIALFETCHER_MYSQL_HOST`: MySQL host (default: `localhost`)
- `LEMATERIALFETCHER_MYSQL_USER`: MySQL username
- `LEMATERIALFETCHER_MYSQL_DATABASE`: MySQL database name (default: `lematerial`)
- `LEMATERIALFETCHER_MYSQL_CERT_PATH`: Path to MySQL SSL certificate

### Transformer Configuration

Transformer operations require both source and destination database configurations. There are two approaches:

1. **Using individual parameters** for source and destination databases:
   ```bash
   # Source database
   LEMATERIALFETCHER_DB_USER=source_username
   LEMATERIALFETCHER_DB_PASSWORD=source_password
   LEMATERIALFETCHER_DB_HOST=localhost
   LEMATERIALFETCHER_DB_NAME=source_database
   
   # Destination database (optional - if not provided, source credentials will be used)
   LEMATERIALFETCHER_DEST_DB_USER=dest_username
   LEMATERIALFETCHER_DEST_DB_PASSWORD=dest_password
   LEMATERIALFETCHER_DEST_DB_HOST=localhost
   LEMATERIALFETCHER_DEST_DB_NAME=dest_database
   ```

   If the destination database parameters are not provided, the source database parameters will be used as fallbacks. This is useful when transforming data within the same database.

2. **Using connection strings**:
   ```bash
   LEMATERIALFETCHER_SOURCE_DB_CONN_STR=host=localhost user=username password=password dbname=source_db sslmode=disable
   LEMATERIALFETCHER_DEST_DB_CONN_STR=host=localhost user=username password=password dbname=dest_db sslmode=disable
   ```

Additional transformer settings:
- `LEMATERIALFETCHER_SOURCE_TABLE_NAME`: Source table name (required)
- `LEMATERIALFETCHER_DEST_TABLE_NAME`: Destination table name (required)
- `LEMATERIALFETCHER_TASK_TABLE_NAME`: Task table name (for MP transformations)
- `LEMATERIALFETCHER_BATCH_SIZE`: Batch size for transformations (default: `500`)
- `LEMATERIALFETCHER_OFFSET`: Starting offset for transformations (default: `0`)
- `LEMATERIALFETCHER_LOG_EVERY`: Log frequency during transformation (default: `1000`)

## Installation

1. Clone the repository:

   ```bash
   git clone git@github.com:LeMaterial/lematerial-fetcher.git
   cd lematerial-fetcher
   ```

2. Set up your environment variables. Copy the provided template and customize it:

   ```bash
   cp .env.example .env
   vim .env
   ```

   Example configurations:

   For Alexandria:
   ```bash
   LEMATERIALFETCHER_API_BASE_URL=https://alexandria.icams.rub.de/pbe/v1/structures
   LEMATERIALFETCHER_DB_NAME=alexandria
   LEMATERIALFETCHER_DB_USER=myuser
   LEMATERIALFETCHER_DB_PASSWORD=mypassword
   LEMATERIALFETCHER_TABLE_NAME=structures_pbe
   LEMATERIALFETCHER_LOG_DIR=./logs
   ```

   For the `lematerial-fetcher mp fetch` command, you need to set the following environment variables:

   ```bash
   LEMATERIALFETCHER_MP_BUCKET_NAME=materialsproject-build
   LEMATERIALFETCHER_MP_BUCKET_PREFIX=collections
   LEMATERIALFETCHER_MP_COLLECTIONS_PREFIX=materials
   ```

3. Build the program:
   ```bash
   # Either
    $ uv add git+https://github.com/lematerial/lematerial-fetcher.git
    # Or
    $ uv pip install git+https://github.com/lematerial/lematerial-fetcher.git
    # Or
    $ pip install git+https://github.com/lematerial/lematerial-fetcher.git
   ```

## Usage

You can use the CLI tool with explicit options or rely on environment variables:

```bash
# Using CLI options
lematerial-fetcher mp fetch --table-name mp_structures --num-workers 4

# Using environment variables (set these before running the command either in the .env file or in the environment)
export LEMATERIALFETCHER_TABLE_NAME=mp_structures
export LEMATERIALFETCHER_NUM_WORKERS=4
lematerial-fetcher mp fetch

# View help for any command
lematerial-fetcher --help
lematerial-fetcher mp fetch --help
```

## Available Commands

- `lematerial-fetcher mp fetch`: Fetch materials from Materials Project
- `lematerial-fetcher mp transform`: Transform Materials Project data
- `lematerial-fetcher alexandria fetch`: Fetch materials from Alexandria
- `lematerial-fetcher alexandria transform`: Transform Alexandria data
- `lematerial-fetcher oqmd fetch`: Fetch materials from OQMD
- `lematerial-fetcher oqmd transform`: Transform OQMD data
- `lematerial-fetcher push`: Push data from a database to Hugging Face

## Logging

Failed URL attempts are logged in a file named `<table_name>_failed_urls.log` within the specified log directory.

## License and copyright

This code base is the property of Entalpic.

```text
Copyright 2025 Entalpic
```
