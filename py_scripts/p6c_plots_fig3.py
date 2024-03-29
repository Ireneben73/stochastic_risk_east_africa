import pandas as pd
import numpy as np
import geopandas as gpd

import matplotlib.pyplot as plt
import matplotlib.colors as colors
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import matplotlib.gridspec as gridspec
from matplotlib.colors import ListedColormap, BoundaryNorm

print('Starting to plot')

# input data
rps_event_surge_allarea=pd.read_csv('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_RPs_surge_perEVENT_allAREA.csv')
rps_event_wl_allarea=pd.read_csv('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_RPs_waterlevel_perEVENT_allAREA.csv')
rps_event_floodext_allarea=pd.read_csv('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_RPs_floodext_perEVENT_allAREA.csv')
rps_event_loss_allarea=pd.read_csv('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_RPs_build_event_allAREA.csv')
rps_event_pop_allarea=pd.read_csv('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_RPs_pop_event_allAREA.csv')

tracks=pd.read_csv('/projects/0/einf2224/paper2/data/input/impact/worst_5events_tracks.csv', header=None)

# Plots
fig = plt.figure(figsize=(16,14))  # Adjust figsize as needed
gs = fig.add_gridspec(3, 2, wspace=0.2)

ax1 = fig.add_subplot(gs[0, 0], projection=ccrs.PlateCarree()) 
cmap = plt.get_cmap('rainbow', 6)  
#bounds = [0, 1, 2, 3, 4, 5, 6]
#norm = BoundaryNorm(bounds, cmap.N)
unique_values = tracks.iloc[:, -1].unique()
print(unique_values)

value_color_map = {
    385005: cmap(0.9),    # Replace 'red' with the color you want for value1
    637805: cmap(0.75),   # Replace 'blue' with the color you want for value2,
    231710: 'limegreen',
    913101: cmap(0.25), 
    544010: cmap(0),
    # Add more entries for other values and their associated colors
}

# Loop over each unique value
for value in unique_values:
    selected_rows = tracks[tracks.iloc[:, -1] == value]
    lonslice = selected_rows.iloc[:, 6]
    latslice = selected_rows.iloc[:, 5]
    values = selected_rows.iloc[:, 10].values
    
    num_points = 150  # Adjust the number of points for smoother lines
    lonslice_smooth = np.interp(np.linspace(0, 1, num_points), np.linspace(0, 1, len(lonslice)), lonslice)
    latslice_smooth = np.interp(np.linspace(0, 1, num_points), np.linspace(0, 1, len(latslice)), latslice)
    values_smooth = np.interp(np.linspace(0, 1, num_points), np.linspace(0, 1, len(values)), values)
    
    color = value_color_map.get(value, 'black')
    ax1.plot(lonslice_smooth, latslice_smooth, color=color, linestyle='-', linewidth=3.0, zorder=99)
    '''
    for i in range(len(lonslice_smooth) - 1):
        if i + 1 < len(lonslice_smooth):  # Check if the next index exists
            color = cmap(norm(values_smooth[i]))  # Get color based on value and colormap
            ax1.plot([lonslice_smooth[i], lonslice_smooth[i + 1]], [latslice_smooth[i], latslice_smooth[i + 1]], color=color, linestyle='-', linewidth=2.0, zorder=99)
    '''
coast = cfeature.GSHHSFeature(scale='full')
ax1.add_feature(coast, facecolor='lightgray', linewidth=0, zorder=1)
ax1.set_extent([23, 59, -33, -9], crs=ccrs.PlateCarree())
#ax1.set_extent([25, 75, -40, -5], crs=ccrs.PlateCarree())
gl1 = ax1.gridlines(alpha=0, draw_labels=True)
ax1.grid(False)
gl1.top_labels=False   # suppress top labels
gl1.right_labels=False # suppress right labels
gl1.xlabel_style = {'color': 'k', 'size':14, 'fontname':'Arial'}
gl1.ylabel_style = {'color': 'k', 'size':14, 'fontname':'Arial'}
ax1.scatter(34.88,-19.753,marker='o',s=10,c='k',edgecolor='k',linewidth=1.5,zorder=99)
ax1.text(31,-20,'Beira',fontsize=14, family='Arial', zorder=99)
#cbar = plt.colorbar(plt.cm.ScalarMappable(cmap=cmap, norm=norm), ax=ax1, pad=0.02, shrink=1)
#cbar.set_ticks([0.5, 1.5, 2.5, 3.5, 4.5, 5.5])
#cbar.set_ticklabels(['Cat.0', 'Cat.1', 'Cat.2', 'Cat.3', 'Cat.4', 'Cat.5'], fontsize=14)
ax1.text(0.02, 0.93, 'a)', transform=ax1.transAxes, fontsize=14, fontweight='bold')

# Exceedance probabilities - Storm surge
tolerance = 1e-6
print(type(rps_event_surge_allarea['Total stormsurge']))
#rps_event_floodext_event1=rps_event_floodext_allarea[rps_event_floodext_allarea['Total stormsurge']==1492547668.81481]
rps_event_surge_event1 = rps_event_surge_allarea[np.isclose(rps_event_surge_allarea['Total stormsurge'], 2.555027146, atol=tolerance)] 
rps_event_surge_event2 = rps_event_surge_allarea[np.isclose(rps_event_surge_allarea['Total stormsurge'], 3.853574337, atol=tolerance)]
rps_event_surge_event3 = rps_event_surge_allarea[np.isclose(rps_event_surge_allarea['Total stormsurge'], 3.329968837, atol=tolerance)]
rps_event_surge_event4 = rps_event_surge_allarea[np.isclose(rps_event_surge_allarea['Total stormsurge'], 3.040687173, atol=tolerance)]
rps_event_surge_event5 = rps_event_surge_allarea[np.isclose(rps_event_surge_allarea['Total stormsurge'], 2.389394300, atol=tolerance)]
'''
rps_event_surge_event1 = rps_event_surge_allarea[np.isclose(rps_event_surge_allarea['Total stormsurge'], 2.555027147, atol=tolerance)]
rps_event_surge_event2 = rps_event_surge_allarea[np.isclose(rps_event_surge_allarea['Total stormsurge'], 3.853574338, atol=tolerance)]
rps_event_surge_event3 = rps_event_surge_allarea[np.isclose(rps_event_surge_allarea['Total stormsurge'], 3.329968837, atol=tolerance)]
rps_event_surge_event4 = rps_event_surge_allarea[np.isclose(rps_event_surge_allarea['Total stormsurge'], 3.034898975, atol=tolerance)]
rps_event_surge_event5 = rps_event_surge_allarea[np.isclose(rps_event_surge_allarea['Total stormsurge'], 2.389394301, atol=tolerance)]
'''
print('rps_event_surge_event1:', rps_event_surge_event1)
ax5 = fig.add_subplot(gs[1, 0])
ax5.scatter(rps_event_surge_allarea['Return Period'], (rps_event_surge_allarea['Total stormsurge']), marker='x', c='lightgray', zorder=1, s=100)
ax5.scatter(rps_event_surge_event1['Return Period'], (rps_event_surge_event1['Total stormsurge']), marker='x', c=cmap(0.9), label='2.6 m', linewidths=3, zorder=99, s=100)
ax5.scatter(rps_event_surge_event2['Return Period'], (rps_event_surge_event2['Total stormsurge']), marker='x', c=cmap(0.75), label='3.9 m', linewidths=3,zorder=99, s=100)
ax5.scatter(rps_event_surge_event3['Return Period'], (rps_event_surge_event3['Total stormsurge']), marker='x', c='limegreen', label='3.3 m', linewidths=3,zorder=99, s=100)
ax5.scatter(rps_event_surge_event4['Return Period'], (rps_event_surge_event4['Total stormsurge']), marker='x', c=cmap(0.25), label='3.0 m', linewidths=3,zorder=99, s=100)
ax5.scatter(rps_event_surge_event5['Return Period'], (rps_event_surge_event5['Total stormsurge']), marker='x', c=cmap(0), label='2.4 m', linewidths=3,zorder=99, s=100)
ax5.set_xscale('log')
ax5.set_xlabel('Return period [-]', fontsize=14)
ax5.tick_params(axis='y', labelsize=14)
ax5.tick_params(axis='x', labelsize=14)
#ax2.set_xticklabels([])
ax5.set_ylim(0, (rps_event_surge_allarea['Total stormsurge'].max())*1.05)
ax5.set_ylabel('Max. Storm surge residuals [m+MSL]', fontsize=14)
ax5.legend(loc='lower right', handletextpad=0.5, fontsize=14, frameon=False)
ax5.text(0.02, 0.93, 'b)', transform=ax5.transAxes, fontsize=14, fontweight='bold')

# Exceedance probabilities - Water levels
tolerance = 1e-6
print(rps_event_wl_allarea)
#rps_event_floodext_event1=rps_event_wl_allarea[rps_event_wl_allarea['Total waterlevel']==1492547668.81481]
rps_event_wl_event1 = rps_event_wl_allarea[np.isclose(rps_event_wl_allarea['Total waterlevel'], 5.14800024032592, atol=tolerance)]
#rps_event_wl_event1=rps_event_wl_allarea[rps_event_wl_allarea['Total waterlevel']==5.14800024032592]

rps_event_wl_event2 = rps_event_wl_allarea[np.isclose(rps_event_wl_allarea['Total waterlevel'], 5.02400016784667, atol=tolerance)]
rps_event_wl_event3 = rps_event_wl_allarea[np.isclose(rps_event_wl_allarea['Total waterlevel'], 5.03000020980834, atol=tolerance)]
rps_event_wl_event4 = rps_event_wl_allarea[np.isclose(rps_event_wl_allarea['Total waterlevel'], 4.66700029373168, atol=tolerance)]
rps_event_wl_event5 = rps_event_wl_allarea[np.isclose(rps_event_wl_allarea['Total waterlevel'], 4.80400037765502, atol=tolerance)]
print('rps_event_wl_event1:', rps_event_wl_event1)
ax6 = fig.add_subplot(gs[2, 0])
ax6.scatter(rps_event_wl_allarea['Return Period'], (rps_event_wl_allarea['Total waterlevel']), marker='x', c='lightgray', zorder=1, s=100)
ax6.scatter(rps_event_wl_event1['Return Period'], (rps_event_wl_event1['Total waterlevel']), marker='x', c=cmap(0.9), label='5.1 m', linewidths=3, zorder=99, s=100)
ax6.scatter(rps_event_wl_event2['Return Period'], (rps_event_wl_event2['Total waterlevel']), marker='x', c=cmap(0.75), label='5.0 m', linewidths=3,zorder=99, s=100)
ax6.scatter(rps_event_wl_event3['Return Period'], (rps_event_wl_event3['Total waterlevel']), marker='x', c='limegreen', label='5.0 m', linewidths=3,zorder=99, s=100)
ax6.scatter(rps_event_wl_event4['Return Period'], (rps_event_wl_event4['Total waterlevel']), marker='x', c=cmap(0.25), label='4.7 m', linewidths=3,zorder=99, s=100)
ax6.scatter(rps_event_wl_event5['Return Period'], (rps_event_wl_event5['Total waterlevel']), marker='x', c=cmap(0), label='4.8 m', linewidths=3,zorder=99, s=100)
ax6.set_xscale('log')
ax6.set_xlabel('Return period [-]', fontsize=14)
ax6.tick_params(axis='y', labelsize=14)
ax6.tick_params(axis='x', labelsize=14)
#ax2.set_xticklabels([])
ax6.set_ylim(0, (rps_event_wl_allarea['Total waterlevel'].max())*1.05)
ax6.set_ylabel('Max. Storm tide [m+MSL]', fontsize=14)
ax6.legend(loc='lower right', handletextpad=0.5, fontsize=14, frameon=False)
ax6.text(0.02, 0.93, 'c)', transform=ax6.transAxes, fontsize=14, fontweight='bold')



# Exceedance probabilities - Flood extent
tolerance = 1e-6
#rps_event_floodext_event1=rps_event_floodext_allarea[rps_event_floodext_allarea['Total floodexts']==1492547668.81481]
rps_event_floodext_event1 = rps_event_floodext_allarea[np.isclose(rps_event_floodext_allarea['Total floodexts'], 1492547668.81481, atol=tolerance)]
rps_event_floodext_event2 = rps_event_floodext_allarea[np.isclose(rps_event_floodext_allarea['Total floodexts'], 1249924098.93133, atol=tolerance)]
rps_event_floodext_event3 = rps_event_floodext_allarea[np.isclose(rps_event_floodext_allarea['Total floodexts'], 1074252507.85546, atol=tolerance)]
rps_event_floodext_event4 = rps_event_floodext_allarea[np.isclose(rps_event_floodext_allarea['Total floodexts'], 3201023124.33691, atol=tolerance)]
rps_event_floodext_event5 = rps_event_floodext_allarea[np.isclose(rps_event_floodext_allarea['Total floodexts'], 1463366459.66265, atol=tolerance)]

ax2 = fig.add_subplot(gs[0, 1])
ax2.scatter(rps_event_floodext_allarea['Return Period'], (rps_event_floodext_allarea['Total floodexts']/1e6), marker='x', c='lightgray', zorder=1, s=100)
ax2.scatter(rps_event_floodext_event1['Return Period'], (rps_event_floodext_event1['Total floodexts']/1e6), marker='x', c=cmap(0.9), label='1,493 km$^2$', linewidths=3,zorder=99, s=100)
ax2.scatter(rps_event_floodext_event2['Return Period'], (rps_event_floodext_event2['Total floodexts']/1e6), marker='x', c=cmap(0.75), label='1,251 km$^2$', linewidths=3,zorder=99, s=100)
ax2.scatter(rps_event_floodext_event3['Return Period'], (rps_event_floodext_event3['Total floodexts']/1e6), marker='x', c='limegreen', label='1,075 km$^2$', linewidths=3,zorder=99, s=100)
ax2.scatter(rps_event_floodext_event4['Return Period'], (rps_event_floodext_event4['Total floodexts']/1e6), marker='x', c=cmap(0.25), label='3,203 km$^2$', linewidths=3,zorder=99, s=100)
ax2.scatter(rps_event_floodext_event5['Return Period'], (rps_event_floodext_event5['Total floodexts']/1e6), marker='x', c=cmap(0), label='1,464 km$^2$', linewidths=3,zorder=99, s=100)
ax2.set_xscale('log')
ax2.set_xlabel('Return period [-]', fontsize=14)
ax2.tick_params(axis='y', labelsize=14)
ax2.tick_params(axis='x', labelsize=14)
#ax2.set_xticklabels([])
ax2.set_ylim(0, (rps_event_floodext_allarea['Total floodexts'].max())/1e6*1.05)
ax2.set_ylabel('Max. Flood extent [km$^2$]', fontsize=14)
ax2.legend(loc='lower right', handletextpad=0.5, fontsize=14, frameon=False)
ax2.text(0.02, 0.93, 'd)', transform=ax2.transAxes, fontsize=14, fontweight='bold')

# Exceedance probabilities - Damages
rps_event_loss_event1=rps_event_loss_allarea[rps_event_loss_allarea['Total Damages']==559561535]
rps_event_loss_event2=rps_event_loss_allarea[rps_event_loss_allarea['Total Damages']==507374256]
rps_event_loss_event3=rps_event_loss_allarea[rps_event_loss_allarea['Total Damages']==507004107]
rps_event_loss_event4=rps_event_loss_allarea[rps_event_loss_allarea['Total Damages']==499684128]
rps_event_loss_event5=rps_event_loss_allarea[rps_event_loss_allarea['Total Damages']==485935401]

ax3 = fig.add_subplot(gs[1, 1])
ax3.scatter(rps_event_loss_allarea['Return Period'], (rps_event_loss_allarea['Total Damages']/1e6), marker='x', c='lightgray', s=100)
ax3.scatter(rps_event_loss_event1['Return Period'], (rps_event_loss_event1['Total Damages']/1e6), marker='x', c=cmap(0.9), label='560 m€', linewidths=3,zorder=99, s=100)
ax3.scatter(rps_event_loss_event2['Return Period'], (rps_event_loss_event2['Total Damages']/1e6), marker='x', c=cmap(0.75), label='507 m€', linewidths=3,zorder=99, s=100)
ax3.scatter(rps_event_loss_event3['Return Period'], (rps_event_loss_event3['Total Damages']/1e6), marker='x', c='limegreen', label='507 m€', linewidths=3,zorder=99, s=100)
ax3.scatter(rps_event_loss_event4['Return Period'], (rps_event_loss_event4['Total Damages']/1e6), marker='x', c=cmap(0.25), label='500 m€', linewidths=3,zorder=99, s=100)
ax3.scatter(rps_event_loss_event5['Return Period'], (rps_event_loss_event5['Total Damages']/1e6), marker='x', c=cmap(0), label='486 m€', linewidths=3,zorder=99, s=100)
ax3.set_xscale('log')
ax3.set_xlabel('Return period [-]', fontsize=14)
ax3.tick_params(axis='y', labelsize=14)
ax3.tick_params(axis='x', labelsize=14)
#ax3.set_xticklabels([])
ax3.set_ylim(0, (rps_event_loss_allarea['Total Damages'].max())/1e6*1.05)
ax3.set_ylabel('Damages [million €]', fontsize=14)
ax3.legend(loc='lower right', handletextpad=0.5, fontsize=14, frameon=False)
ax3.text(0.02, 0.93, 'e)', transform=ax3.transAxes, fontsize=14, fontweight='bold')

# Exceedance probabilities - Population
rps_event_pop_event1=rps_event_pop_allarea[rps_event_pop_allarea['Total Damages']==267809]
rps_event_pop_event2=rps_event_pop_allarea[rps_event_pop_allarea['Total Damages']==246687]
rps_event_pop_event3=rps_event_pop_allarea[rps_event_pop_allarea['Total Damages']==247453]
rps_event_pop_event4=rps_event_pop_allarea[rps_event_pop_allarea['Total Damages']==270193]
rps_event_pop_event5=rps_event_pop_allarea[rps_event_pop_allarea['Total Damages']==243935]

ax4 = fig.add_subplot(gs[2, 1])
ax4.scatter(rps_event_pop_allarea['Return Period'], (rps_event_pop_allarea['Total Damages']/1e3), marker='x', c='lightgray', s=100)
ax4.scatter(rps_event_pop_event1['Return Period'], (rps_event_pop_event1['Total Damages']/1e3), marker='x', c=cmap(0.9), label='268k', linewidths=3,zorder=99, s=100)
ax4.scatter(rps_event_pop_event2['Return Period'], (rps_event_pop_event2['Total Damages']/1e3), marker='x', c=cmap(0.75), label='247k', linewidths=3,zorder=99, s=100)
ax4.scatter(rps_event_pop_event3['Return Period'], (rps_event_pop_event3['Total Damages']/1e3), marker='x', c='limegreen', label='247k', linewidths=3,zorder=99, s=100)
ax4.scatter(rps_event_pop_event4['Return Period'], (rps_event_pop_event4['Total Damages']/1e3), marker='x', c=cmap(0.25), label='270k', linewidths=3,zorder=99, s=100)
ax4.scatter(rps_event_pop_event5['Return Period'], (rps_event_pop_event5['Total Damages']/1e3), marker='x', c=cmap(0), label='244k', linewidths=3,zorder=99, s=100)
ax4.set_xscale('log')
ax4.set_xlabel('Return period [-]', fontsize=14)
ax4.tick_params(axis='y', labelsize=14)
ax4.tick_params(axis='x', labelsize=14)
#ax4.set_xticklabels([])
ax4.set_ylim(0, (rps_event_pop_allarea['Total Damages'].max())/1e3*1.05)
ax4.set_ylabel('Affected population [x1000]', fontsize=14)
ax4.legend(loc='lower right', handletextpad=0.5, fontsize=14, frameon=False)
ax4.text(0.02, 0.93, 'f)', transform=ax4.transAxes, fontsize=14, fontweight='bold')

plt.subplots_adjust(top=0.95, bottom=0.05)
plt.tight_layout()
fig.savefig('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/figures/p6c_RPs_worst_events_fig7_v3.png', dpi=600)
