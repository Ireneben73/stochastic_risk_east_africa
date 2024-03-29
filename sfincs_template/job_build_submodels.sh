#!/bin/bash
#SBATCH -p fat
#SBATCH -n 1
#SBATCH -t 00:10:00
#SBATCH --mem=80G

module load 2021
module load Anaconda3/2021.05
source activate hydromt-sfincs_latest
#source activate hydromt-sfincs

#python build_sfincs_submodels_gisnotrot.py
python build_sfincs_submodels_gisnotrot.py