#!/bin/bash
set -e

echo "Starting the full pipeline..."

# Materials Project Pipeline
echo "Fetching Materials Project structures..."
lematerial-fetcher mp fetch --table-name mp_structures --num-workers 4

echo "Fetching Materials Project tasks..."
lematerial-fetcher mp fetch --tasks --table-name mp_tasks

echo "Transforming Materials Project data..."
lematerial-fetcher mp transform --table-name mp_structures --dest-table-name transformed_mp_structures

# Alexandria Pipeline
echo "Fetching Alexandria structures..."
lematerial-fetcher alexandria fetch --table-name alex_structures --functional pbe

echo "Fetching Alexandria trajectories..."
lematerial-fetcher alexandria fetch --traj --table-name alex_trajectories

echo "Transforming Alexandria data..."
lematerial-fetcher alexandria transform --table-name alex_structures --dest-table-name transformed_alex_structures
lematerial-fetcher alexandria transform --table-name alex_trajectories --dest-table-name transformed_alex_trajectories --traj

# OQMD Pipeline
echo "Fetching OQMD structures..."
lematerial-fetcher oqmd fetch --table-name oqmd_structures

echo "Transforming OQMD data..."
lematerial-fetcher oqmd transform --table-name oqmd_structures --dest-table-name transformed_oqmd_structures

# Push to Hugging Face (if HF_TOKEN is set)
if [ ! -z "$LEMATERIALFETCHER_HF_TOKEN" ]; then
    echo "Pushing data to Hugging Face..."
    lematerial-fetcher push --table-name transformed_mp_structures --hf-repo-id LeMaterial/LeMat-Bulk
    lematerial-fetcher push --table-name transformed_alex_trajectories --hf-repo-id LeMaterial/LeMat-Traj
else
    echo "Skipping Hugging Face push (HF_TOKEN not set)"
fi

echo "Pipeline completed successfully!" 
