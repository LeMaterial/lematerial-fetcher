import duckdb
import os

# --- Configuration ---
# Path to your Hugging Face Parquet files
HF_PARQUET_PATH = "/home/dts/hf_data/LeMat-Bulk/compatible_pbe/*.parquet"
# Connection string to your Local Postgres
LOCAL_DB_CONN_STR = "dbname=lematerial user=lematerial host=localhost"

def main():
    print("🚀 Starting Full Comparison: Local Postgres vs. Hugging Face Parquet\n")
    
    # 1. Setup DuckDB Connection
    con = duckdb.connect()
    try:
        con.install_extension("postgres")
        con.load_extension("postgres")
        con.sql(f"ATTACH '{LOCAL_DB_CONN_STR}' AS local_db (TYPE POSTGRES)")
    except Exception as e:
        print(f"❌ Failed to attach to Postgres: {e}")
        return

    # ---------------------------------------------------------
    # PART 1: ID Comparison (The "Metadata" Check)
    # ---------------------------------------------------------
    print("--- 📊 Part 1: ID Comparison (immutable_id) ---")
    
    # View for HF IDs (Filter only for Alexandria data to be fair)
    con.sql(f"""
        CREATE OR REPLACE VIEW hf_ids AS 
        SELECT immutable_id 
        FROM read_parquet('{HF_PARQUET_PATH}')
        WHERE immutable_id LIKE 'agm%' OR immutable_id LIKE 'alexandria%'
    """)

    # View for Local IDs
    con.sql("""
        CREATE OR REPLACE VIEW local_ids AS
        SELECT id AS immutable_id
        FROM local_db.optimade_structures
        WHERE source = 'alexandria'
    """)

    # Find IDs in Local NOT in HF
    new_ids_df = con.sql("""
        SELECT l.immutable_id
        FROM local_ids l
        LEFT JOIN hf_ids h ON l.immutable_id = h.immutable_id
        WHERE h.immutable_id IS NULL
    """).df()

    id_count = len(new_ids_df)
    print(f"👉 Local IDs missing from HF: {id_count}")

    if id_count > 0:
        csv_name = "new_ids.csv"
        print(f"   Saving first 1000 to {csv_name}...")
        new_ids_df.to_csv(csv_name, index=False)
    
    print("-" * 50 + "\n")

    # ---------------------------------------------------------
    # PART 2: Fingerprint Comparison (The "Geometry" Check)
    # ---------------------------------------------------------
    print("--- 🧬 Part 2: Fingerprint Comparison (Structure Hash) ---")

    # View for HF Fingerprints (entalpic_fingerprint)
    # Note: We don't strictly filter by ID here because if the geometry exists 
    # under a different source/ID in HF, it is NOT physically new.
    con.sql(f"""
        CREATE OR REPLACE VIEW hf_hashes AS 
        SELECT entalpic_fingerprint AS fp 
        FROM read_parquet('{HF_PARQUET_PATH}')
        WHERE entalpic_fingerprint IS NOT NULL
    """)

    # View for Local Fingerprints (bawl_fingerprint)
    con.sql("""
        CREATE OR REPLACE VIEW local_hashes AS
        SELECT bawl_fingerprint AS fp, id
        FROM local_db.optimade_structures
        WHERE source = 'alexandria' 
        AND bawl_fingerprint IS NOT NULL
    """)

    # Find Hashes in Local NOT in HF
    new_hashes_df = con.sql("""
        SELECT l.id, l.fp
        FROM local_hashes l
        LEFT JOIN hf_hashes h ON l.fp = h.fp
        WHERE h.fp IS NULL
    """).df()

    hash_count = len(new_hashes_df)
    print(f"👉 Unique Structures (Hashes) missing from HF: {hash_count}")

    if hash_count > 0:
        csv_name = "new_unique_structures.csv"
        print(f"   Saving first 1000 to {csv_name}...")
        new_hashes_df.to_csv(csv_name, index=False)
    else:
        print("   (All geometries in your local DB already exist in HF)")

    print("\n✅ Comparison Complete.")
    con.close()

if __name__ == "__main__":
    main()
