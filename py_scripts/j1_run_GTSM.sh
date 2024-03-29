#! /bin/bash
#SBATCH -p thin
#SBATCH --nodes=1
#SBATCH --tasks-per-node=32
#SBATCH --ntasks=32
#SBATCH --job-name=newsingularity
#SBATCH --time=4-12:00:00
#SBATCH --mem=40G
#SBATCH --export=ALL

# tasks per node: #SBATCH --tasks-per-node=32
# purpose of run
export purpose="GTSMv4.1 - STORM"
echo "========================================================================="
echo "Submitting Dflow-FM run $name in  $PWD"
echo "Purpose: $purpose"
echo "Starting on $SLURM_NTASKS domains, SLURM_NNODES nodes"
echo "Wall-clock-limit set to $maxwalltime"
echo "========================================================================="

# stop after an error occured
set -e

# load modules
module purge
module load 2021
module load intel/2021a

source activate postprocess_GTSM

# Data to modify when running
#yr=0 # yr*1000 from STORM
yr="$1" # yr*1000 from STORM
echo "year" $yr
first_event="$2" #1 # first event to run in the batch for the specified 1000-yr
last_event="$3" #2000 # last event to run in the batch for the specified 1000-yr
echo "first event" $first_event
echo "last event" $last_event

# Restart folder 
restart_path="/projects/0/einf2224/paper2/scripts/gtsm_template/restart_files"
#restart_list=$(ls $restart_path)
restart_list=$(ls $restart_path/gtsm_fine_0000_* 2>/dev/null)
#echo "$restart_list"

# Read the TXT file of STORM
# Define the file name and column number
storm_filename="/projects/0/einf2224/paper2/data/input/meteo/STORMV4/STORM_DATA_IBTRACS_SI_1000_YEARS_${yr}_filtered.txt"
#storm_filename="/projects/0/einf2224/paper2/data/input/meteo/STORMV4/STORM_DATA_IBTRACS_SI_1000_YEARS_MULTIPLE_beira_100events.txt"
column_storm_id=14 # column where the storm_ids are

unique_values=$(awk -F ',' '{sub(/\r$/,"",$'$column_storm_id'); print $'$column_storm_id'}' $storm_filename | sort | uniq)

# create output folder for GTSM
outputdir_gtsm="/projects/0/einf2224/paper2/scripts/model_runs/gtsm/YEARS_${yr}/"
if [ ! -d "$outputdir_gtsm" ]; then
    mkdir -p "$outputdir_gtsm"
else 
    echo "Directory $outputdir_gtsm already exists!"
fi

# Loop over the unique STORM_id values
#echo "$unique_values"

for storm_id in $(echo "$unique_values" | sed -n "${first_event},${last_event}p"); do
    
    echo "-----Starting running STORM ID: $storm_id -----"
    export storm_id=$storm_id
    
    storm_model_folder="/projects/0/einf2224/paper2/scripts/model_runs/gtsm_tmp/${storm_id}/" 
    gtsmoutfile="/projects/0/einf2224/paper2/scripts/model_runs/gtsm/YEARS_${yr}/gtsm_stormid_${storm_id}.nc"
    
    #if [ ! -d "$storm_model_folder" ]; then
    if [ ! -f "$gtsmoutfile" ]; then
        # create the folder if it doesn't exist
        mkdir -p "$storm_model_folder"
        echo "Folder from STORM event created successfully."
    
        # Take a random restart file date and time:     
        restart_random_file=$(echo $restart_list | tr " " "\n" | shuf | head -n 1) # Use shuf to select a random file from the list
        echo "RESTART RANDOM FILE $restart_random_file"
        refdate=$(echo "$restart_random_file" | awk -F '_' '{print $6}')
        refhour=$(echo "$restart_random_file" | cut -d '_' -f 7 | sed 's/0000$//')
        echo "REF DATE: $refdate"
        echo "REF HOUR: $refhour"    
        
        # Holland model
        echo "Start running Holland model"
        python /projects/0/einf2224/paper2/scripts/py_scripts/p1_holland_model/p1a_create_single_spw_ibl.py "$refdate" "$refhour" "$storm_id" "$storm_filename" "$storm_model_folder" &&
        touch "holland_$storm_id.done"
        if [ ! -f "holland_$storm_id.done" ]; then
            echo "$storm_id FAILED!" >> holland_log.txt
        else        
            echo "Finished running Holland model"
            rm "holland_$storm_id.done" 
        fi
        
        echo "Editing GTSM mdu" 
        python p2_preprocess_GTSM.py "$refdate" "$refhour" "$storm_id" "$storm_filename" "$storm_model_folder" &&
        
        # Run GTSM
        modelFolder=$storm_model_folder
        singularityFolder=/gpfs/home4/benitoli/delft3dfm_containers/delft3dfm_2022.04/
        mduFile=gtsm_fine.mdu        
        
        # Partition model
        #$singularityFolder/execute_singularity_snellius.sh $modelFolder run_dflowfm.sh --partition:ndomains=$SLURM_NTASKS:icgsolver=6 $mduFile
        
        # Run GTSM model
        #timeout 1200 $singularityFolder/execute_singularity_snellius.sh $modelFolder run_dflowfm.sh -m $mduFile #--savenet #$dimrFile
        #touch $storm_id.done
        
        if timeout 3600 $singularityFolder/execute_singularity_snellius_IBL.sh $modelFolder run_dflowfm.sh -m $mduFile; then        
            echo "All runs for STORM: $storm_id have finished"
            echo "$storm_id DONE!" >> storms_log_${yr}.txt
            # postprocess GTSM, compressing and copying output file in the outputdir
            python p3_postprocess_GTSM.py "$storm_model_folder" "$storm_id" "$outputdir_gtsm" &&
            # delete the temporary storm id folder of GTSM
            rm -rf "$storm_model_folder"      
        else
            echo "GTSM for $storm_id did NOT finish successfully"
            echo "$storm_id FAILED!" >> storms_log_${yr}.txt
        fi          

        
        if ! timeout 1200 $singularityFolder/execute_singularity_snellius.sh $modelFolder run_dflowfm.sh -m $mduFile; then
            echo "GTSM for $storm_id did NOT finish successfully"
            echo "$storm_id FAILED!" >> storms_log_${yr}.txt
            continue
        fi
        echo "All runs for STORM: $storm_id have finished"
        echo "$storm_id DONE!" >> storms_log_${yr}.txt
        # postprocess GTSM, compressing and copying output file in the outputdir
        python p3_postprocess_GTSM.py "$storm_model_folder" "$storm_id" "$outputdir_gtsm" &&
        # detele the temporary storm id folder of GTSM
        rm -rf "$storm_model_folder"
        rm $storm_id.done
        
        # Postprocess GTSM! Delete with done. Delete also slurm of the job!
        if [ -f "$storm_id.done" ]; then
            # if the file exists, print a message indicating that the runs are finished
            echo "All runs for STORM: $storm_id have finished"
            echo "$storm_id DONE!" >> storms_log_${yr}.txt
            # postprocess GTSM, compressing and copying output file in the outputdir
            python p3_postprocess_GTSM.py "$storm_model_folder" "$storm_id" "$outputdir_gtsm" &&
            # detele the temporary storm id folder of GTSM
            rm -rf "$storm_model_folder"
            rm $storm_id.done
        else
            echo "GTSM for $storm_id did NOT finish successfully"
            echo "$storm_id FAILED!" >> storms_log_${yr}.txt
        fi

    else
        # if the folder already exists, print a message and do nothing else
        echo "Storm $storm_id was already executed."
    fi    
done
echo "Whole run has finished!!"
