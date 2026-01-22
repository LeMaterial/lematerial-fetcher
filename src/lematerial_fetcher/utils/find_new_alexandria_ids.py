"""
Find IDs that exist in the local Postgres database but NOT in the Hugging Face dataset.
(i.e., New data that you fetched which isn't in the static HF archive yet).
"""
import duckdb

# 1. Setup connection
con = duckdb.connect()
con.install_extension("postgres")
con.load_extension("postgres")
con.sql("ATTACH 'dbname=lematerial user=lematerial host=localhost' AS local_db (TYPE POSTGRES)")

# 2. Point to HF Data
hf_parquet_path = "/home/dts/hf_data/LeMat-Bulk/compatible_pbe/*.parquet"

print("Comparing databases to find NEW local entries...")

# 3. Create Filtered View of HF (Alexandria Only)
# We filter HF to only look at Alexandria IDs so we compare apples to apples.
con.sql(f"""
    CREATE OR REPLACE VIEW hf_alexandria AS 
    SELECT immutable_id FROM read_parquet('{hf_parquet_path}')
    WHERE immutable_id LIKE 'agm%' OR immutable_id LIKE 'alexandria%'
""")

# 4. The "Reverse" Comparison
# SELECT from Local LEFT JOIN HF where HF is NULL
extra_df = con.sql("""
    SELECT p.id
    FROM local_db.alexandria_source p
    LEFT JOIN hf_alexandria h ON p.id = h.immutable_id
    WHERE h.immutable_id IS NULL
""").df()

print(f"Found {len(extra_df)} IDs in Local DB that are NOT in Hugging Face.")

if len(extra_df) > 0:
    print("Example new IDs:", extra_df.head())
    output_file = "extra_ids_local.csv"
    extra_df.to_csv(output_file, index=False)
    print(f"Saved list of new IDs to {output_file}")
else:
    print("Your local database is a subset of Hugging Face (no new unique data found).")