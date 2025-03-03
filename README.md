# lematerial-fetcher

`lematerial-fetcher` is designed to fetch data from a specified OPTIMADE's compatible JSON-API, process it, and store it in a PostgreSQL database. It is highly concurrent, to handle data fetching and processing efficiently.

As Entalpic, the objective is to retrieve information from various OPTIMADE sources and establish a local database. This database will enable us to process and utilize the data according to our specific requirements.

## Prerequisites

- Python 3.11 or later
- PostgreSQL database
- Environment variables set for configuration

## Environment Variables

LeMaterial Fetcher uses the following environment variables for configuration:

- `LEMATERIAL_FETCHER_API_BASE_URL`: The base URL of the API to fetch data from. **(Required)**
- `LEMATERIAL_FETCHER_DB_USER`: The username for the PostgreSQL database. **(Required)**
- `LEMATERIAL_FETCHER_DB_PASSWORD`: The password for the PostgreSQL database. **(Required)**
- `LEMATERIAL_FETCHER_DB_NAME`: The name of the PostgreSQL database. **(Required)**
- `LEMATERIAL_FETCHER_TABLE_NAME`: The name of the table in the database where data will be stored. **(Required)**
- `LEMATERIAL_FETCHER_LOG_DIR`: The directory where log files will be stored. Defaults to `./logs`.

## Installation

1. Clone the repository:

   ```bash
   git clone git@github.com:LeMaterial/lematerial-fetcher.git
   cd lematerial-fetcher
   ```

2. Set up your environment variables. You can use a `.env` file or export them directly in your shell. For example:

   ```bash
   LEMATERIAL_FETCHER_API_BASE_URL=https://alexandria.icams.rub.de/pbe/v1/structures
   LEMATERIAL_FETCHER_DB_NAME=alexandria
   LEMATERIAL_FETCHER_DB_USER=myus€r
   LEMATERIAL_FETCHER_DB_PASSWORD=mypa$$word
   LEMATERIAL_FETCHER_TABLE_NAME=structures_pbe
   LEMATERIAL_FETCHER_LOG_DIR=./logs
   ```

   For the `lematerial-fetcher mp fetch` command, you need to set the following environment variables:

   ```bash
   LEMATERIAL_FETCHER_MP_BUCKET_NAME=materialsproject-build
   LEMATERIAL_FETCHER_MP_BUCKET_PREFIX=collections
   LEMATERIAL_FETCHER_MP_COLLECTIONS_PREFIX=materials
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

Run the program using the following command:

```bash
./lematerial-fetcher
```

## Logging

Failed URL attempts are logged in a file named `<table_name>_failed_urls.log` within the specified log directory.

## License and copyright

This code base is the property of Entalpic.

```text
Copyright 2025 Entalpic
```
