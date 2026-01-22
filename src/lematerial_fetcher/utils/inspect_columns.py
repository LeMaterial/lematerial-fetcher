import duckdb
import glob

# Path to your HF data
PARQUET_PATH = "/home/dts/hf_data/LeMat-Bulk/compatible_pbe/*.parquet"

# 1. Find the first actual file (DuckDB needs a concrete file to describe schema quickly)
files = glob.glob(PARQUET_PATH)

if not files:
    print(f"❌ No files found at: {PARQUET_PATH}")
    exit(1)

first_file = files[0]
print(f"🔍 Inspecting columns in: {first_file}\n")

# 2. Use DuckDB to describe the schema
con = duckdb.connect()
con.sql(f"DESCRIBE SELECT * FROM read_parquet('{first_file}')").show()