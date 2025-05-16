#!/bin/bash
set -e

echo "WARNING: Make sure that your .env file is not interfering with the Docker's environment variables."

# Start PostgreSQL and MariaDB services
echo "Starting PostgreSQL service..."
service postgresql start

echo "Starting MariaDB service..."
service mariadb start

echo "Starting the full pipeline..."
source /app/.venv/bin/activate

# Materials Project Pipeline
echo "Fetching Materials Project structures..."
lematerial-fetcher mp fetch --table-name mp_structures --num-workers 12

echo "Fetching Materials Project tasks..."
lematerial-fetcher mp fetch --tasks --table-name mp_tasks --num-workers 12

echo "Transforming Materials Project data..."
lematerial-fetcher mp transform --traj --task-source-table-name mp_tasks --table-name mp_structures --dest-table-name transformed_mp_structures --num-workers 12 --batch-size 1000 --db-fetch-batch-size 10

# UNCOMMENT FOR ALEXANDRIA

# # Alexandria Pipeline
# echo "Fetching Alexandria trajectories..."
# lematerial-fetcher alexandria fetch --traj --table-name alex_structures --functional pbe --num-workers 10
# lematerial-fetcher alexandria fetch --traj --table-name alex_structures --functional pbesol --num-workers 10

# echo "Transforming Alexandria data..."
# lematerial-fetcher alexandria transform --traj --table-name alex_structures --dest-table-name transformed_alex_structures --num-workers 10 --batch-size 1000 --db-fetch-batch-size 10

# UNCOMMENT FOR OQMD

# # OQMD Pipeline
# echo "Fetching OQMD structures..."
# lematerial-fetcher oqmd fetch --table-name oqmd_structures

# echo "Transforming OQMD data..."
# lematerial-fetcher oqmd transform --traj --table-name oqmd_structures --dest-table-name transformed_oqmd_structures --num-workers 10 --batch-size 1000 --db-fetch-batch-size 10

# Push to Hugging Face (if HF_TOKEN is set)
if [ ! -z "$LEMATERIALFETCHER_HF_TOKEN" ]; then
    echo "Pushing data to Hugging Face..."
    lematerial-fetcher push --table-name transformed_mp_structures \
        --hf-repo-id LeMat-Traj \
        # UNCOMMENT FOR ALEXANDRIA and OQMD
        # --table-name transformed_alex_structures \
        # --table-name transformed_oqmd_structures \
        --chunk-size 1000000 \
        --num-workers 12 \
        --hf-token $LEMATERIALFETCHER_HF_TOKEN
else
    echo "Skipping Hugging Face push (HF_TOKEN not set)"
fi

echo "Pipeline completed successfully!" 
