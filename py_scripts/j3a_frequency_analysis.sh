#!/bin/bash
#SBATCH -p thin
#SBATCH -n 1
#SBATCH --time=00:10:00
#SBATCH --mem=20G

module load 2021
module load Anaconda3/2021.05

source activate postprocess

python p6b_frequency_analysis.py



