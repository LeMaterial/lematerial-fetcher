import duckdb
import os

# --- Configuration ---
HF_PARQUET_PATH = "/home/dts/hf_data/LeMat-Bulk/compatible_pbe/*.parquet"
LOCAL_DB_CONN_STR = "dbname=lematerial user=lematerial host=localhost"

def main():
    print("🚀 Starting Full Comparison (With Prefix Fix)\n")
    
    con = duckdb.connect()
    try:
        con.install_extension("postgres")
        con.load_extension("postgres")
        con.sql(f"ATTACH '{LOCAL_DB_CONN_STR}' AS local_db (TYPE POSTGRES)")
    except Exception as e:
        print(f"❌ Failed to attach to Postgres: {e}")
        return

    # ---------------------------------------------------------
    # PART 0: INSPECTION (Sanity Check)
    # ---------------------------------------------------------
    print("--- 🔍 Part 0: Data Preview (Check for Prefixes) ---")
    print("👉 Local DB ID (Raw from Postgres):")
    try:
        con.sql("SELECT immutable_id FROM local_db.optimade_structures WHERE source='alexandria' LIMIT 3").show()
    except:
        print("   (Could not read Local DB)")

    print("👉 HF Parquet ID (Raw from File):")
    try:
        con.sql(f"SELECT immutable_id FROM read_parquet('{HF_PARQUET_PATH}') WHERE immutable_id LIKE 'agm%' LIMIT 3").show()
    except:
        print("   (Could not read Parquet)")

    # ---------------------------------------------------------
    # PART 1: ID Comparison (Stripping Prefix)
    # ---------------------------------------------------------
    print("\n--- 📊 Part 1: ID Comparison ---")
    
    con.sql(f"""
        CREATE OR REPLACE VIEW hf_ids AS 
        SELECT immutable_id 
        FROM read_parquet('{HF_PARQUET_PATH}')
        WHERE immutable_id LIKE 'agm%' OR immutable_id LIKE 'alexandria%'
    """)

    # ✅ THE FIX: We use REPLACE to remove 'alexandria:' from the local ID
    con.sql("""
        CREATE OR REPLACE VIEW local_ids AS
        SELECT 
            REPLACE(immutable_id, 'alexandria:', '') as immutable_id
        FROM local_db.optimade_structures
        WHERE source = 'alexandria'
    """)

    new_ids_df = con.sql("""
        SELECT l.immutable_id
        FROM local_ids l
        LEFT JOIN hf_ids h ON l.immutable_id = h.immutable_id
        WHERE h.immutable_id IS NULL
    """).df()

    id_count = len(new_ids_df)
    print(f"👉 New IDs found (Present in Local, Missing in HF): {id_count}")

    if id_count > 0:
        csv_name = "new_ids_fixed.csv"
        print(f"   Saving first 1000 to {csv_name}...")
        new_ids_df.head(1000).to_csv(csv_name, index=False)
    
    # ---------------------------------------------------------
    # PART 2: Fingerprint Comparison
    # ---------------------------------------------------------
    print("\n--- 🧬 Part 2: Fingerprint Comparison ---")

    con.sql(f"""
        CREATE OR REPLACE VIEW hf_hashes AS 
        SELECT entalpic_fingerprint AS fp 
        FROM read_parquet('{HF_PARQUET_PATH}')
        WHERE entalpic_fingerprint IS NOT NULL
    """)

    con.sql("""
        CREATE OR REPLACE VIEW local_hashes AS
        SELECT bawl_fingerprint AS fp, immutable_id as id
        FROM local_db.optimade_structures
        WHERE source = 'alexandria' 
        AND bawl_fingerprint IS NOT NULL
    """)

    new_hashes_df = con.sql("""
        SELECT l.id, l.fp
        FROM local_hashes l
        LEFT JOIN hf_hashes h ON l.fp = h.fp
        WHERE h.fp IS NULL
    """).df()

    hash_count = len(new_hashes_df)
    print(f"👉 Unique Geometries missing from HF: {hash_count}")

    if hash_count > 0:
        csv_name = "new_unique_structures.csv"
        print(f"   Saving first 1000 to {csv_name}...")
        new_hashes_df.head(1000).to_csv(csv_name, index=False)

    print("\n✅ Done.")
    con.close()

if __name__ == "__main__":
    main()
