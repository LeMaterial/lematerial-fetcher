# lematerial-fetcher

`lematerial-fetcher` is designed to fetch data from a specified OPTIMADE's compatible JSON-API, process it, and store it in a PostgreSQL database. It is highly concurrent, to handle data fetching and processing efficiently.

As Entalpic, the objective is to retrieve information from various OPTIMADE sources and establish a local database. This database will enable us to process and utilize the data according to our specific requirements.

## Data Credits

This project relies entirely on the valuable contributions of several materials science database projects:

- [**Materials Project**](https://materialsproject.org/) - A comprehensive database of computed materials properties funded by the U.S. Department of Energy
- [**Alexandria Library**](https://alexandria.icams.rub.de/) - A quantum-accurate materials library developed by ICAMS at Ruhr University Bochum
- [**Open Quantum Materials Database (OQMD)**](https://oqmd.org/) - An extensive collection of DFT calculated properties maintained by researchers at Northwestern University

We gratefully acknowledge these projects and their dedication to open materials science data. Our tool is built entirely on the foundation of their well-maintained databases and research efforts.

## Prerequisites

- Python 3.11 or later
- PostgreSQL database
- Environment variables set for configuration

## Environment Variables

LeMaterial Fetcher uses the following environment variables for configuration:

- `LEMATERIALFETCHER_API_BASE_URL`: The base URL of the API to fetch data from. **(Required)**
- `LEMATERIALFETCHER_DB_USER`: The username for the PostgreSQL database. **(Required)**
- `LEMATERIALFETCHER_DB_PASSWORD`: The password for the PostgreSQL database. **(Required)**
- `LEMATERIALFETCHER_DB_NAME`: The name of the PostgreSQL database. **(Required)**
- `LEMATERIALFETCHER_TABLE_NAME`: The name of the table in the database where data will be stored. **(Required)**
- `LEMATERIALFETCHER_LOG_DIR`: The directory where log files will be stored. Defaults to `./logs`.

## Installation

1. Clone the repository:

   ```bash
   git clone git@github.com:LeMaterial/lematerial-fetcher.git
   cd lematerial-fetcher
   ```

2. Set up your environment variables. You can use a `.env` file or export them directly in your shell. For example:

   ```bash
   LEMATERIALFETCHER_API_BASE_URL=https://alexandria.icams.rub.de/pbe/v1/structures
   LEMATERIALFETCHER_DB_NAME=alexandria
   LEMATERIALFETCHER_DB_USER=myus€r
   LEMATERIALFETCHER_DB_PASSWORD=mypa$$word
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

Run the program using the following command:

```bash
./lematerial-fetcher
```

## Logging

Failed URL attempts are logged in a file named `<table_name>_failed_urls.log` within the specified log directory.

## Acknowledgements

This project leverages data from several established materials databases. Please see [ACKNOWLEDGEMENTS.md](./ACKNOWLEDGEMENTS.md) for complete information about the data sources used and proper citations for academic use.

## License and copyright

This code base is the property of Entalpic.

```text
Copyright 2025 Entalpic
```
