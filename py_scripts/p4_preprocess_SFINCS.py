# -*- coding: utf-8 -*-
"""
Created on Mon Mar  6 09:28:22 2023

@author: ibe202
"""

import numpy as np
import xarray as xr
import geopandas as gpd
import pandas as pd
import os
import sys
import subprocess
import shutil
from distutils.dir_util import copy_tree
import datetime
import hydromt
from hydromt_sfincs import SfincsModel
from os.path import join

# load all the data
storm_id = sys.argv[1]
gtsm_file=os.path.join(sys.argv[2], 'gtsm_stormid_' + sys.argv[1] + '.nc')
his_gtsm_month_tides=xr.open_dataset('/projects/0/einf2224/paper2/scripts/gtsm_template/restart/output_restart_final/gtsm_fine_0000_his.nc').load() 
sfincs_submodels='/projects/0/einf2224/paper2/data/input/flood/sfincs_submodels.shp'
#print(his_gtsm_month_tides.bedlevel)

# storm surge maximum water level
his_gtsm_event = xr.open_dataset(gtsm_file).load() 


# --------------------------------------------------------------------------------------------------------------------------------------------
### 1. Process GTSM output to cleanup dry cells
# --------------------------------------------------------------------------------------------------------------------------------------------

# Remove stations where cells fall dry: if any waterlevel equals the bedlevel (bedlevel info in the tides file..)

# Mask surges that are equal to bedlevel
mask_surges= his_gtsm_event.waterlevel == his_gtsm_month_tides.bedlevel
his_gtsm_nodry=his_gtsm_event.where(~(mask_surges).any(dim='time'), drop=True) # masking for stations where storm surges have bed level values at some point
his_gtsm_month_tides_nodrysurge = his_gtsm_month_tides.where(~(mask_surges).any(dim='time'), drop=True)

# calculate the maximum water levels
his_gtsm_nodry_max = his_gtsm_nodry.max('time', skipna=True)
start_time_tides = np.datetime64('2019-01-01')
end_time_tides = np.datetime64('2019-01-31T23:50')
tides_timeslice = his_gtsm_month_tides_nodrysurge.sel(time=slice(start_time_tides, end_time_tides))
#print('his_gtsm_month_tides_timeslice:', tides_timeslice)
tides_max = tides_timeslice.max('time', skipna=True)


# --------------------------------------------------------------------------------------------------------------------------------------------
### 2. Selection of SFINCS submodels
# --------------------------------------------------------------------------------------------------------------------------------------------
#print('his_gtsm_nodry_max',his_gtsm_nodry_max)

#Substract the maximum tide to the storm tide to obtain only certain water levels above highest tide
max_skewed_surge = his_gtsm_nodry_max.waterlevel - tides_max.waterlevel
#print('max_skewed_surge', max_skewed_surge)

max_skewed_surge_ds = xr.Dataset(
    {
        'waterlevel': max_skewed_surge,
    },
    coords={
        'station_x_coordinate': his_gtsm_nodry_max.station_x_coordinate,
        'station_y_coordinate': his_gtsm_nodry_max.station_y_coordinate,
        'stations': his_gtsm_nodry_max.stations,
        'station_name': his_gtsm_nodry_max.station_name,
    }
)

#max_skewed_surge = his_gtsm_steady_notnull.waterlevel - tides_max_notnull.waterlevel
surge_mask=max_skewed_surge_ds.where(max_skewed_surge_ds.waterlevel>0.1, drop=True)
surge_mask_gdf = gpd.GeoDataFrame(
    surge_mask.waterlevel, geometry=gpd.points_from_xy(surge_mask.station_x_coordinate,surge_mask.station_y_coordinate))

# read submodels of sfincs
sfincs_submodels = gpd.read_file(sfincs_submodels)
sfincs_submodels_flooding = gpd.sjoin(sfincs_submodels,surge_mask_gdf.set_crs('epsg:4326'), how='inner')#['savedindex'] #Find the polygons that intersect. Keep savedindex as a series
print('Flood models:', sfincs_submodels_flooding.FID.unique().tolist())
#print('Flood models:', ' '.join(map(str, sfincs_submodels_flooding.FID.unique().tolist())))

if sfincs_submodels_flooding.FID.unique().tolist():
    with open('submodels_sfincs.txt', 'a') as file:
        # Redirect print statements to the file
        print(storm_id, ' '.join(map(str, sfincs_submodels_flooding.FID.unique().tolist())), file=file)
    
# --------------------------------------------------------------------------------------------------------------------------------------------
### 3. Copy and modify the SFINCS submodels
# --------------------------------------------------------------------------------------------------------------------------------------------

# Get start and end dates from GTSM output timeseries
tstart_np = his_gtsm_nodry.time[0].values
tstart_dt = datetime.datetime.utcfromtimestamp(tstart_np.astype(int) * 1e-9)
tstart = tstart_dt.strftime('%Y%m%d %H%M%S')
tstop_np = his_gtsm_nodry.time[-1].values
tstop_dt = datetime.datetime.utcfromtimestamp(tstop_np.astype(int) * 1e-9)
#print('TSTART:', tstart_dt)
#print('TSTOP:', tstop_dt)

# submodel templates folder
sfincs_submodel_templates = '/projects/0/einf2224/paper2/scripts/sfincs_template/'
# temporary destination folder
#tmp_sfincs_folder = '/gpfs/home4/benitoli/papers/paper2/model_runs/sfincs_tmp/' + str(storm_id)
#tmp_sfincs_folder = '/projects/0/einf2224/paper2/scripts/model_runs/sfincs_tmp/' + str(storm_id)
tmp_sfincs_folder = sys.argv[3]

# create a temporary folder for each event
if not os.path.exists(tmp_sfincs_folder):
    os.makedirs(tmp_sfincs_folder)

# loop over each submodel and modify it
for submodel_id in sfincs_submodels_flooding.FID.unique().tolist():
    #os.chdir(root)
    print('SUBMODEL ID:', submodel_id)
    
    # copy each submodel into a subfolder for this specific event & submodel
    #shutil.copytree(sfincs_submodel_templates + 'submodel_' + str(submodel_id), tmp_sfincs_folder)
    root = tmp_sfincs_folder + '/' + str(submodel_id)
    copy_tree(sfincs_submodel_templates + 'submodel_' + str(submodel_id), root)#,symlinks=False,ignore=None)
    
    # Set SFINCS model root
    mod0 = SfincsModel(
        data_libs=["/projects/0/einf2224/paper2/scripts/sfincs_template/data_catalog.yml"],
        root=root,
        mode="r+",
    )
    config=mod0.config.copy()
    
    # Modify config file accordingly
    config.update({
        'tref': tstart, 
        'tstart': tstart, 
        'tstop': tstop_dt, 
        #'outputformat': 'bin',  # TO DO = Explore this option
        'bzsfile': 'sfincs.bzs',
        'bndfile': 'sfincs.bnd',
        #'inifile': 'sfincs.zsini'
    })
        
    mod0._write_gis = False
    mod0.setup_config(**config)
    
    mod0.write_config()#(f'{basename}')
    
    # Optain the boundary point coordinates from GTSM
    bnd = gpd.GeoDataFrame(
        index=np.atleast_1d(his_gtsm_nodry['stations'].values),
        geometry=gpd.points_from_xy(
            np.atleast_1d(his_gtsm_nodry['station_x_coordinate'].values), 
            np.atleast_1d(his_gtsm_nodry['station_y_coordinate'].values)
        ),
        crs=4326
    ).to_crs(mod0.crs)
    
    # Create a pandas dataframe to create the water level forcing   
    #df_timeseries=pd.DataFrame(index=data.time, columns=bnd.index, data=data.waterlevel)
    df_timeseries=pd.DataFrame(index=his_gtsm_nodry.time, columns=bnd.index, data=his_gtsm_nodry.waterlevel)
    #print(df_timeseries)
    #print(df_timeseries.index.values)
    #print(bnd)
    mod0.setup_waterlevel_forcing(
        timeseries=df_timeseries,
        locations=bnd,
        offset="dtu10mdt",
        merge=False,
    )

    mod0.write_forcing()
    

