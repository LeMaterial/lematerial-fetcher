# LeMaterial-Fetcher

`lematerial-fetcher` is designed to fetch data from a specified OPTIMADE's compatible JSON-API, process it, and store it in a PostgreSQL database. It is highly concurrent, to handle data fetching and processing efficiently.

The objective is to retrieve information from various OPTIMADE sources and establish a local database. This database will enable us to process and utilize the data according to our specific requirements, which can then be uploaded to an online and easily accessible place like Hugging Face.

**Explore the datasets built with this tool on [Hugging Face](https://huggingface.co/LeMaterial)** 🤗:

👉 [LeMat-Bulk](https://huggingface.co/datasets/LeMaterial/LeMat-Bulk)

👉 [LeMat-Traj](https://huggingface.co/datasets/LeMaterial/LeMat-Traj)

## Data Credits

This project relies entirely on the valuable contributions of several materials science database projects:

- [**Materials Project**](https://materialsproject.org/) - A comprehensive database of computed materials properties funded by the U.S. Department of Energy and developed by the Lawrence Berkeley National Laboratory in collaboration with several other laboratories and universities
- [**Alexandria Library**](https://alexandria.icams.rub.de/) - A quantum-accurate materials library developed by ICAMS at Ruhr University Bochum
- [**Open Quantum Materials Database (OQMD)**](https://oqmd.org/) - An extensive collection of DFT calculated properties maintained by researchers at Northwestern University

We gratefully acknowledge these projects and their dedication to open materials science data. Our tool is built entirely on the foundation of their well-maintained databases and research efforts.

## Prerequisites

- Python 3.11 or later
- PostgreSQL database
- Environment variables set for configuration

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

3. Install the package:
   ```bash
   # Using uv (recommended)
   uv add git+https://github.com/LeMaterial/lematerial-fetcher.git
   # or
   uv pip install git+https://github.com/LeMaterial/lematerial-fetcher.git
   
   # Or using pip
   pip install git+https://github.com/LeMaterial/lematerial-fetcher.git
   ```

## Configuration

The tool can be configured in two ways:

1. **Command-line arguments** (recommended for most options)
2. **Environment variables** (preferred for sensitive information)

### Environment Variables

For sensitive information like passwords, use environment variables with the `LEMATERIALFETCHER_` prefix:

```bash
# Database credentials
export LEMATERIALFETCHER_DB_PASSWORD=your_password
export LEMATERIALFETCHER_MYSQL_PASSWORD=your_mysql_password

# Hugging Face credentials
export LEMATERIALFETCHER_HF_TOKEN=your_huggingface_token
```

A template `.env.example` file is provided that you can copy to `.env` and customize.

## Usage

The CLI provides several commands for different data sources and operations. Here's a comprehensive guide:

### Basic Structure

```bash
lematerial-fetcher [GLOBAL_OPTIONS] COMMAND [COMMAND_OPTIONS]
```

### Global Options

- `--debug`: Run operations in main process for debugging
- `--cache-dir DIR`: Directory for temporary data (default: ~/.cache/lematerial_fetcher)

### Available Commands

1. **Materials Project (MP)**
   ```bash
   # Fetch structures
   lematerial-fetcher mp fetch --table-name mp_structures --num-workers 4
   
   # Fetch tasks
   lematerial-fetcher mp fetch --tasks --table-name mp_tasks
   
   # Transform data
   lematerial-fetcher mp transform --table-name source_table --dest-table-name dest_table
   ```

2. **Alexandria**
   ```bash
   # Fetch structures
   lematerial-fetcher alexandria fetch --table-name alex_structures --functional pbe
   
   # Fetch trajectories
   lematerial-fetcher alexandria fetch --traj --table-name alex_trajectories
   
   # Transform data
   lematerial-fetcher alexandria transform --table-name source_table --dest-table-name dest_table
   ```

3. **OQMD**
   ```bash
   # Fetch data
   lematerial-fetcher oqmd fetch --table-name oqmd_structures
   
   # Transform data
   lematerial-fetcher oqmd transform --table-name source_table --dest-table-name dest_table
   ```

4. **Push to Hugging Face**
   ```bash
   lematerial-fetcher push --table-name my_table --hf-repo-id my-repo
   ```

### Common Options

These options are available across most commands:

#### Database Options
- `--db-conn-str STR`: Complete database connection string
- `--db-user USER`: Database username
- `--db-host HOST`: Database host (default: localhost)
- `--db-name NAME`: Database name (default: lematerial)

#### Processing Options
- `--num-workers N`: Number of parallel workers
- `--log-dir DIR`: Directory for logs (default: ./logs)
- `--max-retries N`: Maximum retry attempts (default: 3)
- `--retry-delay N`: Delay between retries in seconds (default: 2)
- `--log-every N`: Log frequency (default: 1000)

#### Fetch Options
- `--offset N`: Starting offset (default: 0)
- `--table-name NAME`: Target table name
- `--limit N`: Items per API request (default: 500)

#### Transformer Options
- `--batch-size N`: Batch processing size (default: 500)
- `--dest-table-name NAME`: Destination table name
- `--traj`: Transform trajectory data

### Examples

1. **Fetch from Materials Project with custom configuration**:
   ```bash
   lematerial-fetcher mp fetch \
     --table-name mp_structures \
     --num-workers 4 \
     --db-host localhost \
     --db-name materials \
     --log-dir ./mp_logs
   ```

2. **Transform Alexandria data with source and destination databases**:
   ```bash
   lematerial-fetcher alexandria transform \
     --table-name source_table \
     --dest-table-name dest_table \
     --batch-size 1000 \
     --db-host source_host \
     --dest-db-host dest_host
   ```

3. **Push to Hugging Face with custom chunk size**:
   ```bash
   lematerial-fetcher push \
     --table-name my_table \
     --hf-repo-id my-repo \
     --chunk-size 2000 \
     --max-rows 10000
   ```

## Database Configuration

### PostgreSQL Configuration

You can configure the database connection in two ways:

1. **Using individual parameters**:
   ```bash
   # Set password in environment
   export LEMATERIALFETCHER_DB_PASSWORD=your_password
   
   # Run command
   lematerial-fetcher mp fetch --db-user username --db-name database_name
   ```

2. **Using a connection string**:
   ```bash
   lematerial-fetcher mp fetch --db-conn-str="host=localhost user=username password=password dbname=database_name sslmode=disable"
   ```

### MySQL Configuration (for OQMD)

MySQL-specific options:
- `--mysql-host HOST`: MySQL host (default: localhost)
- `--mysql-user USER`: MySQL username
- `--mysql-database NAME`: MySQL database name (default: lematerial)
- `--mysql-cert-path PATH`: Path to MySQL SSL certificate

## Acknowledgements

This project leverages data from several established materials databases. Please see [ACKNOWLEDGEMENTS.md](./ACKNOWLEDGEMENTS.md) for complete information about the data sources used and proper citations for academic use.

## License and copyright

This code base is the property of Entalpic and is licensed under the Apache License, Version 2.0 (the "License").

```text
Copyright 2025 Entalpic
```
