#!/bin/bash
#SBATCH -p thin
#SBATCH -n 1
#SBATCH --time=1-00:00:00
#SBATCH --mem=40G

module load 2021
module load Anaconda3/2021.05
#source activate hydromt-sfincs_rot
source activate hydromt-sfincs_latest


yr="$1" # yr*1000 from STORM
echo "year" $yr
first_event="$2" #1 # first event to run in the batch for the specified 1000-yr
last_event="$3" #2000 # last event to run in the batch for the specified 1000-yr
echo "first event" $first_event
echo "last event" $last_event

outputdir_gtsm="/projects/0/einf2224/paper2/scripts/model_runs/gtsm/YEARS_${yr}/"
file_pattern="gtsm_stormid_*.nc"

# Function to extract storm_id from the file name
extract_storm_id() {
  filename="$1"
  echo "$filename" | grep -oE 'gtsm_stormid_[0-9]+' | awk -F'_' '{print $3}'
}

# Find unique storm_id values in files within the output directory
unique_storm_ids=$(find "$outputdir_gtsm" -type f -name "$file_pattern" | while read -r file; do extract_storm_id "$file"; done | sort -u)
#echo "UNIQUE IDS: $unique_storm_ids"

counter=0
# Loop over the unique STORM_id values
for storm_id in $(echo "$unique_storm_ids" | sed -n "${first_event},${last_event}p"); do
    ((counter++))
    echo "Event number: $counter"
    echo "-----Starting running SFINCS for STORM ID: $storm_id -----"
    export storm_id=$storm_id
    
    #---------------------------------------------------------------------------------------------------------------------------------------------------
    # 1. Preprocess SFINCS
    #---------------------------------------------------------------------------------------------------------------------------------------------------
    # Directory containing the SFINCS event folders and subfolders for each submodel
    #tmp_sfincs_folder="/projects/0/einf2224/paper2/scripts/model_runs/sfincs_tmp/${storm_id}/"
    tmp_sfincs_folder="$TMPDIR/${storm_id}/"
    echo "$tmp_sfincs_folder"    
    # Allocate submodels to each event & prepare each submodel with HydroMT
    python p4_preprocess_SFINCS.py "$storm_id" "$outputdir_gtsm" "$tmp_sfincs_folder"
    echo "HydroMT done"


    #---------------------------------------------------------------------------------------------------------------------------------------------------
    # 2. Run SFINCS for each storm id & submodel & Postprocess the results to obtain hmax
    #---------------------------------------------------------------------------------------------------------------------------------------------------
    
    # Check if tmp_sfincs_folder is empty. This means that no submodel was activated
    if [ -z "$(ls -A "$tmp_sfincs_folder")" ]; then
        echo "No submodel was activated by $storm_id"
        echo "Deleting $tmp_sfincs_folder"
        rm -r "$tmp_sfincs_folder"
    else
        echo "One or several submodels activated"
        
        # Iterate over each sfincs_submodel in the temporary folder
        for sfincs_submodel in "$tmp_sfincs_folder"/*; do
            if [[ -d "$sfincs_submodel" ]]; then
            
                # Change directory to the current sfincs_submodel folder
                cd "$sfincs_submodel"
                sfincs_submodel_nr=$(basename "$sfincs_submodel" | awk -F/ '{print $NF}')
                echo "Starting SFINCS SUBMODEL $sfincs_submodel_nr"
    
                # Run Singularity container and redirect output logs to sfincs_log.txt
                singularity run /gpfs/home4/benitoli/flood_modelling/hydromt-sfincs/sfincs-cpu_latest.sif > sfincs_log.txt
                
                # Check if sfincs_log.txt exists. This means that SFINCS was actually ran
                if [[ -f "sfincs_log.txt" ]]; then
                    # Look for "Simulation finished" in sfincs_log.txt. This means the simulation was successful
                    if grep -q "Simulation finished" sfincs_log.txt; then
                        echo "Simulation finished found"
                        echo "$storm_id DONE!" >> "/projects/0/einf2224/paper2/scripts/py_scripts/log_sfincssubmodel_${sfincs_submodel_nr}.txt" 
                        # Postprocess SFINCS - calculate the hmax & downscale - & perform IMPACT modelling
                        echo "Start postprocess & impact modelling of event $storm_id - submodel $sfincs_submodel_nr"
                        cd "/projects/0/einf2224/paper2/scripts/py_scripts/"
                        python p5_postprocess_SFINCS_run_IMPACT.py "$sfincs_submodel" "$sfincs_submodel_nr" "$storm_id" "$yr" "$first_event"
                        # If simulations for specific event finished correctly (SFINCS simulation), delete the temporary folder
                        echo "Simulation successful. Deleting $sfincs_submodel"
                        rm -rf "$sfincs_submodel" 
                    # If "Simulation finished" is not found, the run did not finish correctly
                    else
                        echo "Simulation finished not found"
                        echo "$storm_id FAILED!" >> "/projects/0/einf2224/paper2/scripts/py_scripts/log_sfincssubmodel_${sfincs_submodel_nr}.txt"
                    fi
                else
                    # Simulation did not generate any sfincs_log.txt. Could be because the run did not start..
                    echo "sfincs_log.txt not found"
                    echo "$storm_id FAILED!" >> "/projects/0/einf2224/paper2/scripts/py_scripts/log_sfincssubmodel_${sfincs_submodel_nr}.txt"
                fi
    
                echo "SFINCS finished for SFINCS submodel $sfincs_submodel_nr"
                
                # Postprocess SFINCS
                # Calculate the hmax, downscale & delete the files that are not necessary!
                #echo "Start postprocess & impact modelling of event $storm_id - submodel $sfincs_submodel_nr"
                #cd "/projects/0/einf2224/paper2/scripts/py_scripts/"
                #p5_postprocess_SFINCS_run_IMPACT.py "$sfincs_submodel" "$sfincs_submodel_nr" "$storm_id"
            fi
        done
        # If simulations for specific event finished correctly, delete the temporary folder
        echo "$tmp_sfincs_folder is empty. Deleting it"
        rmdir --ignore-fail-on-non-empty "$tmp_sfincs_folder"
    fi
    
done
