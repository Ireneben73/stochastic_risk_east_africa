#!/bin/bash
#SBATCH -p thin
#SBATCH -n 1
#SBATCH --time=00:10:00
#SBATCH --mem=20G

module load 2021
module load Anaconda3/2021.05

source activate postprocess_paper2_test
#source activate hydromt-sfincs_latest

echo 'Starting Frequency analysis'
## TO RUN MODELS  220G, 15h
# for multiple submodels
#for submodel_id in $(seq "$1" "$2"); do
#    python p6b_frequency_analysis.py "$submodel_id" &
#    echo "Started processing submodel_id: $submodel_id"
#done

## Wait for all background processes to complete
#wait


# for 1 submodel alone
#python p6b_frequency_analysis.py "$1"

## TO PLOT RESULTS
#python p6b_plots_fig4.py
python p6b_plots_fig4_optionb.py





