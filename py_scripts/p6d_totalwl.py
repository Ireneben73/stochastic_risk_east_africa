import os
import xarray as xr
import pandas as pd
import csv
import sys


def calculate_max_wl(file_path):
    dataset = xr.open_dataset(file_path)
    max_waterlevel = (dataset.waterlevel.max(dim='time')).max()
    dataset.close()
    return max_waterlevel.values

yr=sys.argv[1]
# Directory where the files are located
files_directory = '/projects/0/einf2224/paper2/scripts/model_runs/gtsm/YEARS_' + str(yr) 

# List all the files in the directory
files = os.listdir(files_directory)

# Output file to save event IDs that exceed 10
output_file = '/projects/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6d_water_level_perEVENT.csv'

# Events that give errors
wrong_events_path = "/projects/0/einf2224/paper2/data/input/impact/gtsm_wrong_events.txt"
with open(wrong_events_path, 'r') as wrong_events_file:
    gtsm_wrong_events = set(wrong_events_file.read().splitlines())
    
print('gtsm_wrong_events:', gtsm_wrong_events)

# Loop over the files and calculate maximum water level for each event
with open(output_file, 'a', newline='') as csvfile:
    fieldnames = ['eventid', 'max_wl']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    # Write the header row
    writer.writeheader()
    for file in files:
        if file.startswith('gtsm_stormid_') and file.endswith('.nc'):
            eventid = file.replace('gtsm_stormid_', '').replace('.nc', '')
            print('eventid:', eventid)
            print('eventid:', str(eventid))
            # Check if the eventid is in the list
            if eventid in gtsm_wrong_events:
                print(f"Event ID {eventid} is in the list of wrong events.")
            else:
                file_path = os.path.join(files_directory, file)
                max_wl = calculate_max_wl(file_path)
                if max_wl is not None:  # Only write if max_wl is not None
                    writer.writerow({'eventid': 'stormid_' + str(eventid), 'max_wl': max_wl})