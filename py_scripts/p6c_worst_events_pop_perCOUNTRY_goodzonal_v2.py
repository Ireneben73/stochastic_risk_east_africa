import hydromt
import xarray as xr
import numpy as np
import pandas as pd
import geopandas as gpd
import re
import os
import sys
import glob
import csv
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

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
var1=sys.argv[2]
var2=sys.argv[3]
print(' VAR1:' , var1)

(print('SUBMODEL:', submodel_id, flush=True))
# open shapefile with provinces for the study area
shapefile_path = r"/gpfs/work2/0/einf2224/paper2/data/input/impact/admin_units/provinces_studyarea.shp"
province_gdf = gpd.read_file(shapefile_path)


sfincs_submodels='/projects/0/einf2224/paper2/data/input/flood/sfincs_submodels.shp'
sfincs_submodels = gpd.read_file(sfincs_submodels)

region_file = r"/gpfs/work2/0/einf2224/paper2/data/input/impact/admin_units/countries_studyarea.shp"
region_gdf = gpd.read_file(region_file)
#selected_country = gpd.GeoDataFrame([region_gdf.iloc[0]], crs=region_gdf.crs) #gpd.GeoDataFrame([1], geometry='geometry', crs=region_gdf.crs)
#print('selected_country:', selected_country)
# Create an empty DataFrame to store the summed values
#df_sum = pd.DataFrame(columns=['Variable', 'Sum'])
sum_dict = {}
#for submodel_id in sfincs_submodels.FID.unique().tolist()[7:8]:
    # SORT EVENTS BASED ON MAGNITUDE:
#    (print('SUBMODEL:', submodel_id, flush=True))
 
    #submodel_dpop=xr.open_mfdataset(r'/projects/0/einf2224/paper2/scripts/model_runs/impact/submodel_' + str(submodel_id) + '_damage_pop_*.nc', drop_variables='depth')
pattern = r'/projects/0/einf2224/paper2/scripts/model_runs/impact/submodel_' + str(submodel_id) + '_damage_pop_*.nc'
matching_files = [file for file in glob.glob(pattern) if os.path.isfile(file)]

if matching_files:
    with xr.open_mfdataset(r'/projects/0/einf2224/paper2/scripts/model_runs/impact/submodel_' + str(submodel_id) + '_damage_pop_*.nc', chunks='auto', drop_variables='depth') as submodel_dpop_whole_orig:
        #print('submodel_dpop_whole:', submodel_dpop_whole)
        for i, row in region_gdf.iterrows():
        #for i, row in list(region_gdf.iterrows())[6:7]:
            selected_country = gpd.GeoDataFrame([row], geometry='geometry', crs=region_gdf.crs)
            print('selected_country:', selected_country)

            try:
              submodel_dpop_whole=submodel_dpop_whole_orig
              # drop events that activated all submodels
              submodel_dpop_whole = drop_events(submodel_dpop_whole) 
              
              if not submodel_dpop_whole.data_vars:
                  # If it's empty, exit or skip the current block of code
                  print("No data variables found in SUBMODEL", submodel_id)
                  # You can include additional code here if needed
              else:
                  # convert to integers
                  submodel_dpop_whole = convert_to_integers(submodel_dpop_whole)
                  #print('submodel_dpop_whole:', submodel_dpop_whole)
                  submodel_dpop_whole=submodel_dpop_whole.squeeze(drop=True)
                  #print('submodel_dpop_whole:', submodel_dpop_whole)
                  #submodel_dpop_whole=submodel_dpop_whole['stormid_385005']
                  variable_names = list(submodel_dpop_whole.data_vars.keys())
                  selected_variables = variable_names[int(var1):int(var2)]
                  submodel_dpop_whole = submodel_dpop_whole[selected_variables]
                  submodel_dpop=submodel_dpop_whole.raster.clip_geom(selected_country, mask=True)
                  # Mask datasets with mask
                  mask_file = r'/projects/0/einf2224/paper2/data/input/impact/masks/mask_submodel_' + str(submodel_id) + '.tif'
              
                  # Check if the mask file exists
                  if os.path.exists(mask_file):
                      # Load the mask file as an Xarray DataArray
                      mask = xr.open_dataarray(mask_file).raster.clip_geom(selected_country, mask=True)
                      #print('mask:', mask)
                      mask=mask.squeeze(drop=True)
                      #print('mask:', mask)
                      # Transpose the mask to match the dimensions of the dataset
                      transposed_mask = mask.transpose(*submodel_dpop.dims)
                      transposed_mask=transposed_mask.chunk(submodel_dpop.chunks)
                      transposed_mask = transposed_mask.assign_coords(y=submodel_dpop.y, x=submodel_dpop.x)#, band=submodel_dpop.band) #apparently they are not exactly the same
              
                      submodel_dpop = submodel_dpop.where(transposed_mask != 0, np.nan)
              
                  else:
                      print("Mask file does not exist.")
                      
                  # mask also values where FABDEM==0
                  fabdem_file = r'/projects/0/einf2224/paper2/scripts/sfincs_template/fabdem_submodels_notrot/fabdem_submodel_' + str(submodel_id) + '.tif'
                  fabdem = xr.open_dataarray(fabdem_file).raster.clip_geom(selected_country, mask=True)
                  submodel_dpop = submodel_dpop.where(fabdem != 0, np.nan)
                  
                  total_damages = submodel_dpop.sum(dim=('y', 'x'))#, 'band'))    
                  #print('total_damages CLIP MASK FALSE:', total_damages.compute())
                  
                  for variable_name in total_damages.data_vars:
                      variable_data = total_damages[variable_name]
          
                      # Check if there's valid data for the current variable
                      #if not np.isnan(variable_data):
                      if not np.isnan(variable_data) and variable_data != 0:
                          print(f"{i}: {variable_name}: {variable_data.values}")
                          
                          output_file='/projects/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_event_popaffected_perCOUNTRY_goodzonal_new.csv'
                          if not os.path.exists(output_file):
                            with open(output_file, 'w', encoding='utf-8') as csvfile:
                              writer = csv.writer(csvfile, delimiter=',')
                              writer.writerow(['Variable', 'index', 'Sum', 'Submodel'])
                          
                          with open(output_file, 'a', encoding='utf-8') as csvfile:
                            writer = csv.writer(csvfile, delimiter=',')
                            writer.writerow([variable_name, i, variable_data.values[0], submodel_id])
                              
  
  
                      else:
                          print(f"{i}: {variable_name}: No valid data")
                  
                  submodel_dpop=None
                  
            except Exception as e:
                print("No country:", e)
                continue

else:
    print(f"No matching files found for submodel {submodel_id}.")
    