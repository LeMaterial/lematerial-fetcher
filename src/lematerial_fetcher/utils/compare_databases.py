"""Compare the databases of the Hugging Face and the local Postgres database.

Lazy Loading: DuckDB streams the Parquet data. It never loads 6 million rows into Python objects.

Vectorized: Comparisons happen in C++, making it 10-100x faster than Pandas merge.

Disk-Based: If the join is too big for RAM, DuckDB automatically spills to disk instead of crashing.
"""
import duckdb

# 1. Connect to DuckDB and attach Postgres
con = duckdb.connect()
con.install_extension("postgres")
con.load_extension("postgres")

# Attach your local Postgres DB
con.sql("ATTACH 'dbname=lematerial user=lematerial host=localhost' AS local_db (TYPE POSTGRES)")

# 2. Define the path to your downloaded Parquet files
# Note: DuckDB supports glob patterns (*) to read all files in a folder at once
# TODO(dts): make this an .env variable you load.
# TODO(dts): LeMat-bulk has different folders, compatible_pbe, compatible_pbesol, should
# we compare against all of them for alexandria vs HF?
hf_parquet_path = "/home/dts/hf_data/LeMat-Bulk/compatible_pbe/*.parquet" # Adjust path based on download

# 3. The Comparison Query
# "Find IDs in HF that are NOT in my local Postgres"
print("Comparing databases... (this runs out-of-core, safe for RAM)")

# We create a VIEW for the parquet files so we can query it like a table
con.sql(f"CREATE OR REPLACE VIEW hf_data AS SELECT * FROM read_parquet('{hf_parquet_path}')")

# Example: Count rows in both
count_hf = con.sql("SELECT count(*) FROM hf_data").fetchone()[0]
count_pg = con.sql("SELECT count(*) FROM local_db.alexandria_source").fetchone()[0]

print(f"Hugging Face Rows: {count_hf}")
print(f"Local Postgres Rows: {count_pg}")

# Example: Find missing IDs (IDs in HF that are missing locally)
# Assuming 'immutable_id' is the common key
missing_df = con.sql("""
    SELECT h.immutable_id
    FROM hf_data h
    LEFT JOIN local_db.alexandria_source p ON h.immutable_id = p.immutable_id
    WHERE p.immutable_id IS NULL
""").df()

print(f"Found {len(missing_df)} IDs present in HF but missing locally.")
if len(missing_df) > 0:
    print(missing_df.head())
    missing_df.to_csv("missing_ids.csv", index=False)

