import os
import xarray as xr
import pandas as pd
import csv

gtsm_tides=xr.open_dataset(r'/projects/0/einf2224/paper2/scripts/gtsm_template/restart/output_restart_final/gtsm_fine_0000_his.nc')
january_data = gtsm_tides.sel(time=slice('2019-01-01', '2019-01-31'))
print(january_data.time.max())
february_data = january_data.copy(deep=True).sel(time=slice('2019-01-01', '2019-01-28'))
february_data['time'] = pd.date_range(start='2019-02-01', end='2019-02-28T23:50:00.000000000', freq='10T')
march_data = january_data.copy(deep=True)#.sel(time=slice('2019-01-01', '2019-01-28'))
march_data['time'] = pd.date_range(start='2019-03-01', end='2019-03-31T23:50:00.000000000', freq='10T')
extended_gtsm_tides = xr.concat([january_data, february_data, march_data], dim='time')


def calculate_max_surge(file_path):
    dataset = xr.open_dataset(file_path)
    storm_tide=dataset['waterlevel']
    gtsm_tides_sametime = extended_gtsm_tides.sel(time=storm_tide.time)['waterlevel']
    surge = storm_tide - gtsm_tides_sametime
    #max_surge = surge.max(dim='time').max()
    max_surge = surge.max()
    #max_surge = (surge.max(dim='time')).max()
    dataset.close()
    return max_surge.values


# Directory where the files are located
files_directory = '/projects/0/einf2224/paper2/scripts/model_runs/gtsm/YEARS_9'

# List all the files in the directory
files = os.listdir(files_directory)

# Output file to save event IDs that exceed 10
output_file = '/projects/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6b_surge_perEVENT_v2.csv'

# Events that give errors
wrong_events_path = "/projects/0/einf2224/paper2/data/input/impact/gtsm_wrong_events.txt"
with open(wrong_events_path, 'r') as wrong_events_file:
    gtsm_wrong_events = set(wrong_events_file.read().splitlines())
    
print('gtsm_wrong_events:', gtsm_wrong_events)

# Loop over the files and calculate maximum water level for each event
with open(output_file, 'a', newline='') as csvfile:
    fieldnames = ['eventid', 'max_surge']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    # Write the header row
    writer.writeheader()
    for file in files:
        if file.startswith('gtsm_stormid_') and file.endswith('.nc'):
            eventid = file.replace('gtsm_stormid_', '').replace('.nc', '')
            print('eventid:', eventid, flush=True)

            # Check if the eventid is in the list
            if eventid in gtsm_wrong_events:
                print(f"Event ID {eventid} is in the list of wrong events.")
            else:
                file_path = os.path.join(files_directory, file)
                max_surge = calculate_max_surge(file_path)
                print('MAX SURGE:', max_surge, flush=True)
                if max_surge is not None:  # Only write if max_surge is not None
                    writer.writerow({'eventid': 'stormid_' + str(eventid), 'max_surge': max_surge})
