#!/bin/bash
#SBATCH -p thin
#SBATCH -n 1
#SBATCH --time=00:05:00
#SBATCH --mem=20G

module load 2021
module load Anaconda3/2021.05
source activate postprocess_paper2_test

echo 'Starting plotting RPs' 

#python p6a_plots_fig5.py
#python p6a_plots_fig5_optionb.py
#python p6a_plots_fig5_optionc.py
python p6a_plots_fig5_optiond.py
#python p6a_subplots_countries_SI_fig6.py