import hydromt
import xarray as xr
import numpy as np
import pandas as pd
import geopandas as gpd
import re
import os
import sys
import glob
from collections import defaultdict

import dask
import dask.array as da
import dask.dataframe as dd
from dask import delayed, compute
#from dask.distributed import Client, LocalCluster


# FUNCTIONS
# Convert floats to integers to speed up calculations
def convert_to_integers(dataset):
  for variable in dataset.data_vars:
      replaced_array = dataset[variable].where(dataset[variable] > 0.0, other=0)
      dataset[variable] = replaced_array.astype("int32")
  return dataset
  
# Drop gtsm events that were unstable and activated all the submodels
def drop_events(dataset):
    new_dataset = dataset.copy()
    with open("/projects/0/einf2224/paper2/data/input/impact/gtsm_wrong_events.txt", "r") as f:
        lines = f.readlines()

    variables_to_drop = []
    for line in lines:
        number = line.strip()
        variables_to_drop.append(str(number))

    for variable in variables_to_drop:
        variable_name = "stormid_" + variable
        if variable_name in new_dataset.data_vars:
            new_dataset = new_dataset.drop(variable_name)
        
    return new_dataset


#---------------------------------------------------------------------------------------------------------------------------------------------------------------
## WORST EVENTS FOR THE WHOLE REGION
#---------------------------------------------------------------------------------------------------------------------------------------------------------------

submodel_id=sys.argv[1]
# open shapefile with provinces for the study area
country_file = r"/gpfs/work2/0/einf2224/paper2/data/input/impact/admin_units/countries_studyarea.shp"
country_gdf = gpd.read_file(country_file)


sfincs_submodels='/projects/0/einf2224/paper2/data/input/flood/sfincs_submodels.shp'
sfincs_submodels = gpd.read_file(sfincs_submodels)


# Create an empty DataFrame to store the summed values
#df_sum = pd.DataFrame(columns=['Variable', 'Sum'])
#sum_dict = {}
# SORT EVENTS BASED ON MAGNITUDE:
(print('SUBMODEL:', submodel_id, flush=True))

pattern = r'/projects/0/einf2224/paper2/scripts/model_runs/impact/submodel_' + str(submodel_id) + '_damage_build_*.nc'

# Check if files matching the pattern exist
matching_files = [file for file in glob.glob(pattern) if os.path.isfile(file)]

if matching_files:
    with xr.open_mfdataset(r'/projects/0/einf2224/paper2/scripts/model_runs/sfincs/submodel_' + str(submodel_id) + '_hmax_*.nc', chunks='auto') as submodel_hmax: 
        submodel_hmax = drop_events(submodel_hmax) 
          
        if not submodel_hmax.data_vars:
            # If it's empty, exit or skip the current block of code
            print("No data variables found in SUBMODEL", submodel_id)
            #submodel_dbuild=None
        else:
            # Mask datasets with mask!
            mask_file = r'/projects/0/einf2224/paper2/data/input/impact/masks/mask_submodel_' + str(submodel_id) + '.tif'
          
            # Check if the mask file exists
            if os.path.exists(mask_file):
                # Load the mask file as an Xarray DataArray
                mask = xr.open_dataarray(mask_file)
                mask=mask.squeeze(drop=True)
                # Transpose the mask to match the dimensions of the dataset
                transposed_mask = mask.transpose(*submodel_hmax.dims)
                transposed_mask=transposed_mask.chunk(submodel_hmax.chunks)
                transposed_mask = transposed_mask.assign_coords(y=submodel_hmax.y, x=submodel_hmax.x)#, band=submodel_hmax.band) #apparently they are not exactly the same
          
                submodel_hmax = submodel_hmax.where(transposed_mask != 0, np.nan)
          
            else:
                print("Mask file does not exist.")
            #print('MASKING WITH FABDEM SUBMODEL:', submodel_id, flush=True)    
            # mask also values where FABDEM==0
            fabdem_file = r'/projects/0/einf2224/paper2/scripts/sfincs_template/fabdem_submodels_notrot/fabdem_submodel_' + str(submodel_id) + '.tif'
            fabdem = xr.open_dataarray(fabdem_file)
            submodel_hmax = submodel_hmax.where(fabdem != 0, np.nan)
            #print('submodel_hmax:', submodel_hmax.compute())
            # flood extents
            floodext = xr.where(submodel_hmax > 0.05, 1, 0)
            #print('floodext:', floodext.compute())
            #print('ZONAL STATISTICS SUBMODEL:', submodel_id, flush=True)    
            # Calculate total damages per country, summing damages of each country
            floodext_country = floodext.raster.zonal_stats(country_gdf, 'sum')

            #print('DAMAGE COUNTRY:', submodel_id, damage_country)
            submodel_hmax=None

            #print('floodext_country:', floodext_country)
            #floodext_country = convert_to_integers(floodext_country)
            #print('floodext_country:', floodext_country)
            #total_damages_computed = total_damages.compute()
            #print('total_damages_computed:', total_damages_computed)
            
            #sum_dict={}
            #sum_dict = defaultdict(int)
            #total_damages_append=[]
            for i,index in enumerate(floodext_country.index.values):
                #sum_damage_country_arr = sum_damage_country_msk.sel(index=index).compute().to_array().values
                stacked_data = floodext_country.sel(index=index).to_array(dim='Variable')
                # Convert the stacked DataArray to a pandas DataFrame
                floodext_country_df = stacked_data.to_dataframe(name='Sum')
                
                floodext_country_df['Submodel']=submodel_id
                floodext_country_df=floodext_country_df[floodext_country_df['Sum'] != 0]
                #print('floodext_country_df:', floodext_country_df)
                floodext_country_df_reset = floodext_country_df.reset_index()
                floodext_country_df_reset.rename(columns={'Variable': 'Variable'}, inplace=True)
                floodext_country_df_reset.drop(columns=['band'], inplace=True)
                floodext_country_df_reset.drop(columns=['spatial_ref'], inplace=True)
                print('floodext_country_df_reset:', submodel_id, floodext_country_df_reset)
                output_file='/projects/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6a_event_floodext_perCOUNTRY.csv'
                if os.path.exists(output_file):
                    # Append the DataFrame to the existing CSV file
                    floodext_country_df_reset.to_csv(output_file, mode='a', header=False, index=False) # instead of df_sum
                else:
                    # If the file doesn't exist, create a new CSV file
                    floodext_country_df_reset.to_csv(output_file, index=False)
            submodel_hmax=None

else:
    print(f"No matching files found for submodel {submodel_id}.")
