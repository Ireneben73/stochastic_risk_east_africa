import hydromt
import xarray as xr
import numpy as np
import pandas as pd
import geopandas as gpd
import re
import os


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

# open shapefile with provinces for the study area
shapefile_path = r"/gpfs/work2/0/einf2224/paper2/data/input/impact/admin_units/provinces_studyarea.shp"
province_gdf = gpd.read_file(shapefile_path)


sfincs_submodels='/projects/0/einf2224/paper2/data/input/flood/sfincs_submodels.shp'
sfincs_submodels = gpd.read_file(sfincs_submodels)


# Create an empty DataFrame to store the summed values
#df_sum = pd.DataFrame(columns=['Variable', 'Sum'])
sum_dict = {}
for submodel_id in sfincs_submodels.FID.unique().tolist():
    # SORT EVENTS BASED ON MAGNITUDE:
    (print('SUBMODEL:', submodel_id, flush=True))
 
    submodel_dpop=xr.open_mfdataset(r'/projects/0/einf2224/paper2/scripts/model_runs/impact/submodel_' + str(submodel_id) + '_damage_pop_*.nc', drop_variables='depth')
    #submodel_dpop=xr.open_mfdataset(r'/projects/0/einf2224/paper2/scripts/model_runs/impact/submodel_' + str(submodel_id) + '_damage_pop_*.nc', drop_variables='depth')
    
    # drop events that activated all submodels
    submodel_dpop = drop_events(submodel_dpop) 
    
    if not submodel_dpop.data_vars:
        # If it's empty, exit or skip the current block of code
        print("No data variables found in SUBMODEL", submodel_id)
        # You can include additional code here if needed
    else:
        # convert to integers
        submodel_dpop = convert_to_integers(submodel_dpop)
        print('submodel_dpop:', submodel_dpop)
        submodel_dpop=submodel_dpop.squeeze(drop=True)
        print('submodel_dpop:', submodel_dpop)
        # Mask datasets with mask
        mask_file = r'/projects/0/einf2224/paper2/data/input/impact/masks/mask_submodel_' + str(submodel_id) + '.tif'
    
        # Check if the mask file exists
        if os.path.exists(mask_file):
            # Load the mask file as an Xarray DataArray
            mask = xr.open_dataarray(mask_file)
            print('mask:', mask)
            mask=mask.squeeze(drop=True)
            print('mask:', mask)
            # Transpose the mask to match the dimensions of the dataset
            transposed_mask = mask.transpose(*submodel_dpop.dims)
            transposed_mask=transposed_mask.chunk(submodel_dpop.chunks)
            transposed_mask = transposed_mask.assign_coords(y=submodel_dpop.y, x=submodel_dpop.x)#, band=submodel_dpop.band) #apparently they are not exactly the same
    
            submodel_dpop = submodel_dpop.where(transposed_mask != 0, np.nan)
    
        else:
            print("Mask file does not exist.")
            
        # mask also values where FABDEM==0
        fabdem_file = r'/projects/0/einf2224/paper2/scripts/sfincs_template/fabdem_submodels_notrot/fabdem_submodel_' + str(submodel_id) + '.tif'
        fabdem = xr.open_dataarray(fabdem_file)
        submodel_dpop = submodel_dpop.where(fabdem != 0, np.nan)
    
        total_damages = submodel_dpop.sum(dim=('y', 'x'))#, 'band'))    
    
        '''
        # Iterate over the variables and add their sums to the DataFrame
        for var in total_damages.data_vars:
            if var in df_sum['Variable'].values:
                df_sum.loc[df_sum['Variable'] == var, 'Sum'] += float(total_damages[var])
            else:
                df_sum = pd.concat([df_sum, pd.DataFrame({'Variable': [var], 'Sum': [float(total_damages[var])]})])
        '''
    
        # Stack the dataset along a new dimension named 'Variable'
        stacked_data = total_damages.to_array(dim='Variable')
        # Convert the stacked DataArray to a pandas DataFrame
        total_damages_df = stacked_data.to_dataframe(name='Value')
        
        # Accumulate the summed values in the dictionary
        #for _, row in sum_variables.iterrows():
        for variable, row in total_damages_df.iterrows():
            variable_value = variable  # Get the value of 'Variable'
            sum_value = row['Value']
            if variable_value in sum_dict:
                sum_dict[variable_value] += sum_value
            else:
                sum_dict[variable_value] = sum_value

# Create the DataFrame from the accumulated sums in the dictionary
df_sum = pd.DataFrame(list(sum_dict.items()), columns=['Variable', 'Sum'])
df_sum.to_csv('/projects/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6b_event_damages_pop_submodel.csv', index=False)

# Find the five variables with the largest sums
#largest_vars = df_sum.nlargest(5, 'Sum')

# Print the variables with the largest sums
print('WORST EVENTS:', largest_vars)
