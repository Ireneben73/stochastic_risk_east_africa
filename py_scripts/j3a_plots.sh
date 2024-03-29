#!/bin/bash
#SBATCH -p thin
#SBATCH -n 1
#SBATCH --time=00:05:00
#SBATCH --mem=20G

module load 2021
module load Anaconda3/2021.05
source activate postprocess_paper2_test

python p6a_RPs_ANNUAL_allarea_prl_withEVENTnrs.py
python p6a_RPs_ANNUAL_perCOUNTRY_withEVENTnrs.py
python p6a_RPs_EVENT_allarea_prl.py
python p6a_RPs_EVENT_flood_extent.py
python p6a_RPs_EVENT_perCOUNTRY.py
python p6a_RPs_EVENT_storm_surge.py
python p6a_RPs_EVENT_water_level.py
python p6a_plots_fig2.py
