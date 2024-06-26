#!/bin/bash
#SBATCH -p thin
#SBATCH -n 1
#SBATCH --time=00:20:00
#SBATCH --mem=220G

module load 2021
module load Anaconda3/2021.05
source activate postprocess

python p6b_worst_events.py
python p6b_worst_events_build_perCOUNTRY_goodzonal_v2.py
python p6b_worst_events_pop.py
python p6b_worst_events_pop_perCOUNTRY_goodzonal_v2.py

python p6b_totalwl.py
python p6b_surge_residuals.py

