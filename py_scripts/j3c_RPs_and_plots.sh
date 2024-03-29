#!/bin/bash
#SBATCH -p thin
#SBATCH -n 1
#SBATCH --time=00:05:00
#SBATCH --mem=20G

module load 2021
module load Anaconda3/2021.05
source activate postprocess_paper2_test

echo "Calculate the different return periods"
python p6c_RPs_ANNUAL_allarea_prl_withEVENTnrs.py
python p6c_RPs_ANNUAL_perCOUNTRY_withEVENTnrs.py
python p6c_RPs_EVENT_allarea_prl.py
python p6c_RPs_EVENT_flood_extent.py
python p6c_RPs_EVENT_perCOUNTRY.py
python p6c_RPs_EVENT_storm_surge.py
python p6c_RPs_EVENT_water_level.py

echo "General all the result plots"
python p6c_plots_fig1.py
python p6c_plots_fig2.py
python p6c_plots_fig3.py
