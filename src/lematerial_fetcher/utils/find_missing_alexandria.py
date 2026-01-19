import duckdb

# 1. Setup
con = duckdb.connect()
con.install_extension("postgres")
con.load_extension("postgres")
con.sql("ATTACH 'dbname=lematerial user=lematerial host=localhost' AS local_db (TYPE POSTGRES)")

# 2. Point to HF Data
hf_parquet_path = "/home/dts/hf_data/LeMat-Bulk/compatible_pbe/*.parquet"

print("Filtering for Alexandria IDs (agm-*) only...")

# 3. Create Filtered View
# We STRICTLY select only IDs starting with 'agm' or 'alexandria'
con.sql(f"""
    CREATE OR REPLACE VIEW hf_alexandria AS 
    SELECT * FROM read_parquet('{hf_parquet_path}')
    WHERE immutable_id LIKE 'agm%' OR immutable_id LIKE 'alexandria%'
""")

# 4. Compare
print("Calculating difference...")
missing_df = con.sql("""
    SELECT h.immutable_id
    FROM hf_alexandria h
    LEFT JOIN local_db.alexandria_source p ON h.immutable_id = p.id
    WHERE p.id IS NULL
""").df()

print(f"Found {len(missing_df)} Alexandria IDs missing locally.")

if len(missing_df) > 0:
    print("Example missing IDs:", missing_df.head())
    output_file = "missing_alexandria_ids.csv"
    missing_df.to_csv(output_file, index=False)
    print(f"Saved missing IDs to {output_file}")
else:
    print("No missing Alexandria IDs found! Your database is up to date.")

