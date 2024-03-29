#!/bin/bash
#SBATCH -p thin
#SBATCH -n 1
#SBATCH --time=00:20:00
#SBATCH --mem=220G

module load 2021
module load Anaconda3/2021.05
source activate hydromt-sfincs_latest

echo 'Starting Worst events' 
#python p6c_worst_events.py
#python p6c_worst_events_pop.py
python p6c_worst_event_plot.py

#220GB for later & 12h for python p6c_worst_events.py