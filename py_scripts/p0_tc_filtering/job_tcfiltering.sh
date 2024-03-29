#!/bin/bash
#SBATCH -p thin
#SBATCH -n 1
#SBATCH -t 01:00:00
#SBATCH --mem=40G

module load 2021
module load Anaconda3/2021.05
source activate postprocess_GTSM

echo "Starting filtering of TC events based on distance to land"
python filter_distance2land_v2.py
echo "Filtering done"
