#! /usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 20 17:25:23 2023
@author: benitoli
"""

import os
from datetime import datetime
import xarray as xr
import numpy as np
import sys
import glob

# Rename station_x, station_y
#gtsm_netfile='/projects/0/einf2224/paper2/scripts/model_runs/000001/gtsm/output/gtsm_fine_0000_his.nc'
gtsm_netfile=os.path.join(sys.argv[1], 'output', 'gtsm_fine_0000_his.nc')
dataset=xr.open_dataset(gtsm_netfile); dataset.close()

outfile=os.path.join(sys.argv[3], 'gtsm_stormid_' + sys.argv[2] + '.nc')
#outpath='/projects/0/einf2224/paper2/scripts/model_runs/000001/gtsm/output/test6.nc'

def attrib(dataset):
    dataset.attrs={'title': '10-minute timeseries of total water levels forced with synthetic TC tracks', 
              'summary': 'This dataset has been produced with the Global Tide and Surge Model (GTSM) version 4.1. GTSM was forced with wind a pressure fields derived from the tropical cyclone tracks from STORM in combination with the Holland model', 
              'date_created': str(datetime.utcnow()) + ' UTC', 
              'contact': 'i.benito.lazaro@vu.nl',
              'institution': 'IVM - VU Amsterdam', #include partners later on., 
              'source': 'GTSMv4.1 forced with STORM',
              'keywords': 'tropical cyclone; STORM; water level; storm surge; tides; global tide and surge model;', 
              'geospatial_lat_min': dataset.station_y_coordinate.min().round(3).astype(str).item(), 
              'geospatial_lat_max': dataset.station_y_coordinate.max().round(3).astype(str).item(), 
              'geospatial_lon_min': dataset.station_x_coordinate.min().round(3).astype(str).item(), 
              'geospatial_lon_max': dataset.station_x_coordinate.max().round(3).astype(str).item(), 
              'geospatial_lat_units': 'degrees_north',
              'geospatial_lat_resolution': 'point',
              'geospatial_lon_units': 'degrees_east', 
              'geospatial_lon_resolution': 'point',
              'geospatial_vertical_min': dataset['waterlevel'].min().round(3).astype(str).item(), 
              'geospatial_vertical_max': dataset['waterlevel'].max().round(3).astype(str).item(),
              'geospatial_vertical_units': 'm', 
              'geospatial_vertical_positive': 'up',
              'time_coverage_start': str(dataset.time.min().dt.strftime('%Y-%m-%d %H:%M:%S').item()), 
              'time_coverage_end': str(dataset.time.max().dt.strftime('%Y-%m-%d %H:%M:%S').item())}
    return dataset

#print('Dataset VARIABLES', dataset.variables.keys())
#print('Dataset ATTRIBUTES', dataset.attrs.keys())

dataset=dataset.drop_vars(['station_name', 'station_geom', 'station_geom_node_count', 'timestep', 'wgs84'])
dataset=dataset.drop_dims(['station_geom_nNodes'])

dataset.waterlevel.attrs = {'long_name': 'sea_surface_height_above_mean_sea_level',
                            'units': 'm',
                            'short_name': 'waterlevel',
                            'description': 'Total water level resulting from the combination of barotropic tides and surges and mean sea-level'}

dataset.station_y_coordinate.attrs = {'units': 'degrees_north',
                                 'short_name': 'latitude',
                                 'long_name': 'latitude'}

dataset.station_x_coordinate.attrs = {'units': 'degrees_east',
                                      'short_name': 'longitude',
                                      'long_name': 'longitude'}

dataset.time.attrs = {'axis': 'T',
                      'long_name': 'time',
                      'short_name': 'time'}
dataset['waterlevel'] = dataset.waterlevel.round(3)

dataset = dataset.assign_coords({'stations': dataset.stations})

encoding={'stations':{'dtype': 'uint16', 'complevel': 3, 'zlib': True},
          'station_y_coordinate': {'dtype': 'int32', 'scale_factor': 0.001, '_FillValue': -999, 'complevel': 3, 'zlib': True},
          'station_x_coordinate': {'dtype': 'int32', 'scale_factor': 0.001, '_FillValue': -999, 'complevel': 3, 'zlib': True},
          'waterlevel': {'dtype': 'int16', 'scale_factor': 0.001, '_FillValue': -999, 'complevel': 3, 'zlib': True}}

dataset=attrib(dataset)

dataset.attrs['geospatial_vertical_min'] = dataset.waterlevel.min().round(3).astype(str).item()
dataset.attrs['geospatial_vertical_max'] = dataset.waterlevel.max().round(3).astype(str).item()


#dataset.attrs['source'] = 'GTSMv3 forced with {} dataset'.format(raw_data[scenario]['fname'].split('_')[0])

#dataset.attrs['id'] = 'GTSMv3_totalwaterlevel'; dataset.attrs['title'] = '10-minute timeseries of total water levels'



#print(dataset)
#print(dataset.stations)


dataset.to_netcdf(outfile, encoding=encoding)
