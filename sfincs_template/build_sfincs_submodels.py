import sys
import geopandas as gpd
from hydromt_sfincs import SfincsModel
from hydromt.log import setuplog


sfincs_submodels='/projects/0/einf2224/paper2/data/input/flood/sfincs_submodels.shp'
sfincs_submodels_flooding=gpd.read_file(sfincs_submodels)

for submodel_id in sfincs_submodels_flooding.FID.unique().tolist():#[7:9]:
    sfincs_submodel=sfincs_submodels_flooding[sfincs_submodels_flooding.FID ==submodel_id]

    logger = setuplog('log_' + str(submodel_id))
    path_to_yml='data_catalog.yml'
    
    root = r'submodel_' + str(submodel_id)
    mod = SfincsModel(root, mode='w+', data_libs=path_to_yml, logger=logger)

    mod.setup_grid_from_region(region={'geom': sfincs_submodel}, res=200, rotated=True)
    
    mod.setup_config(
        tref = '20190105 000000',
        tstart = '20190105 000000',
        tstop = '20190112 000000',
        dtmaxout = 99999.0,
        dtout = 99999.0,
        dtwnd = 600.0,
        alpha = 0.5,
        zsini = 0.5,
        advection = 0, # before I had 0.0
        huthresh = 0.05,
    )

    datasets_dep = [{"elevtn":"fabdem", "zmin":0}, {"elevtn": "gebco"}]
    
    mod.setup_dep(datasets_dep = datasets_dep)
    
    mod.setup_mask_active(
        zmin = 0,                    # minimum elevation for valid cells
        exclude_mask = "osm_coastlines",
        drop_area = 1, # drops areas that are smaller than 1km2
    )
    
    mod.setup_mask_bounds(
        btype = "waterlevel",
        zmax = 10,
    )
    
    datasets_rgh = [{"lulc": "vito"}]
    
    mod.setup_subgrid(
        datasets_dep = datasets_dep,
        datasets_rgh=datasets_rgh,
        nr_subgrid_pixels=8, 
        write_dep_tif=True,
        write_man_tif=True,
    )

    mod._write_gis = False
    mod.write()
