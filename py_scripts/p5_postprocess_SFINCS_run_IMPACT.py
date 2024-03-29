import sys
import os
from os.path import join

import numpy as np
import xarray as xr
import pandas as pd
import netCDF4 as nc

from hydromt_sfincs import SfincsModel, utils
import hydromt

#--------------------------------------------------------------------------------------------------------------------------------------------------------------
## POSTPROCESS SFINCS
#--------------------------------------------------------------------------------------------------------------------------------------------------------------

sfincs_root = sys.argv[1]
sfincs_submodel = sys.argv[2]
storm_id = sys.argv[3]
yr = sys.argv[4]
first_event = sys.argv[5]

mod = SfincsModel(
    data_libs=[r"/projects/0/einf2224/paper2/scripts/sfincs_template/data_catalog.yml"],
    root=sfincs_root,
    mode="r",
)

# grid to which the flood maps will be rotated to
dep_submodel = xr.open_dataarray('/projects/0/einf2224/paper2/scripts/sfincs_template/fabdem_submodels_notrot/fabdem_submodel_' + sfincs_submodel + '.tif')  
dep_submodel = dep_submodel.squeeze('band')       
#dep_subgrid = mod.data_catalog.get_rasterdataset(join(sfincs_root, "subgrid", "dep_subgrid.tif"))

# get max water levels
zsmax = mod.results["zsmax"]
# compute the maximum over all time steps
zsmax = zsmax.max(dim='timemax')

# mask minimum flood depth
hmin = 0.05

# GSWO dataset used to mask permanent water 
# permanent water where water occurence > 5%
gswo = mod.data_catalog.get_rasterdataset("gswo", geom=mod.region, buffer=10)
gswo_mask = gswo.raster.reproject_like(mod.grid, method="max") <= 5
# get overland flood depth with GSWO and set minimum flood depth
zsmax_fld = zsmax.where(gswo_mask).where(zsmax > hmin)

# downscale the floodmap to the submodel predefined grid (based on original fabdem)
hmax = utils.downscale_floodmap(
    zsmax=zsmax_fld,
    dep=dep_submodel,
    #dep=dep_subgrid,
    hmin=hmin,
    #gdf_mask=gdf_osm,
    #floodmap_fn=join(sfincs_root, "submodel_" + sfincs_submodel + "_maxfloodmap.tif") 
)

# Create a new NetCDF file
outfile = join('/projects/0/einf2224/paper2/scripts/model_runs/sfincs/', "submodel_" + sfincs_submodel + "_hmax_" + yr + "_" + first_event + ".nc") 
variable_name="stormid_" + str(storm_id)
# Define a variable name for each storm id
hmax_stormid=hmax.to_dataset(name=variable_name)
hmax_stormid=hmax_stormid.drop('band')
encoding={variable_name:{'dtype': 'int16', 'scale_factor': 0.001, 'complevel': 3, 'zlib': True}}

#print('hmax_stormid:', hmax_stormid)
hmax_stormid.to_netcdf(outfile, encoding=encoding, mode='a')

#--------------------------------------------------------------------------------------------------------------------------------------------------------------
## IMPACT MODEL
#--------------------------------------------------------------------------------------------------------------------------------------------------------------

# settings: minimum h at which population are affected
hmin = 0.15

# flood impact functions
def flood_damage(da_flddph, da_exposure, df_susceptibility, **kwargs):
    da0 = df_susceptibility.to_xarray()['factor']
    factor = np.minimum(1, da0.interp(depth=np.minimum(da0.max(),da_flddph), **kwargs))
    #print('FACTOR:', factor.max())
    #print('FACTOR:', factor.min())
    damage = (factor * da_exposure).astype(np.float32) 
    #print('DAMAGE:', damage.max())
    #print('DAMAGE:', damage.min())
    damage.name = da_exposure.name
    damage.attrs.update(**da_exposure.attrs)
    return damage

def flood_exposed(da_flddph, da_exposure, min_flddph=hmin):
    exposed = xr.where(da_flddph>min_flddph,da_exposure,0.0).astype(np.float32)
    exposed.attrs.update(**da_exposure.attrs)
    exposed.name = da_exposure.name
    return exposed

# EXPOSURE & VULNERABILITY
expovulnfile=r'/projects/0/einf2224/paper2/data/input/impact/input_impact/expovuln_submodel_' + sfincs_submodel + '.nc'
expovuln = xr.open_dataset(expovulnfile)
ds_buildall = expovuln.ds_buildall # build RES + NRES exposure raster for specific submodel
ds_buildnres = expovuln.ds_buildnres # build NRES exposure raster for specific submodel
ds_pop = expovuln.ds_pop # population exposure raster for specific submodel
max_damage_res = expovuln.max_damage_res # raster with maximum damages associated to each country
max_damage_ind = expovuln.max_damage_ind # raster with maximum damages associated to each country

# VULNERABILITY
# read vulnerability curves 
# residential
df_res = pd.read_csv(('/projects/0/einf2224/paper2/data/input/impact/susceptibility_studyarea/residential-damagecurve.csv'), index_col=0)
df_res.columns = ['factor']
df_res.index.name = 'depth'
df_res[df_res.index<=hmin] = 0 # correct min flood depth (hmin)
# non-residential  
df_nres = pd.read_csv(('/projects/0/einf2224/paper2/data/input/impact/susceptibility_studyarea/industrial-damagecurve.csv'), index_col=0)
df_nres.columns = ['factor']
df_nres.index.name = 'depth'
df_nres[df_nres.index<=hmin] = 0 # correct min flood depth (hmin)

# IMPACT MODELLING
# Built area
damage_buildres = flood_damage(hmax, ds_buildall, df_res)*max_damage_res 
#print('damage_buildres max:', damage_buildres.max())
#print('damage_buildres min:', damage_buildres.min())
damage_buildnres = flood_damage(hmax, ds_buildall, df_nres)*max_damage_ind # I use ds_buildres because I know the m2 that are build there. So we are considering all m2 are industrial, and after we mask the residential with the insustrial.
#damage_build = impact_buildres.where(impact_buildnres==0, impact_buildnres) # Merge impact rasters from residential and non-residential build area
damage_build = damage_buildres.where(ds_buildnres==0, damage_buildnres) # Merge impact rasters from residential and non-residential build area
#print('damage_build max:', damage_build.max())
#print('damage_build min:', damage_build.min())
# Population
damage_pop=flood_exposed(hmax, ds_pop, hmin)

# Create a new NetCDF file for build impact & for population impact
outfile_build = join('/projects/0/einf2224/paper2/scripts/model_runs/impact/', "submodel_" + sfincs_submodel + "_damage_build_" + yr + "_" + first_event + ".nc") 
outfile_pop = join('/projects/0/einf2224/paper2/scripts/model_runs/impact/', "submodel_" + sfincs_submodel + "_damage_pop_" + yr + "_" + first_event + ".nc") 
damage_build_stormid=damage_build.to_dataset(name=variable_name)
damage_build_stormid=damage_build_stormid.drop('band')
damage_pop_stormid=damage_pop.to_dataset(name=variable_name)
damage_pop_stormid=damage_pop_stormid.drop('band')

encoding2={variable_name: {'dtype': 'int32', 'scale_factor': 0.001, '_FillValue': -999, 'complevel': 3, 'zlib': True}}
damage_build_stormid[variable_name].attrs.pop("_FillValue", None)
damage_pop_stormid[variable_name].attrs.pop("_FillValue", None)
damage_build_stormid.to_netcdf(outfile_build, encoding=encoding2, mode='a')
damage_pop_stormid.to_netcdf(outfile_pop, encoding=encoding2, mode='a')
