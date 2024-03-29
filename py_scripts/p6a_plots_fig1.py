import pandas as pd
import numpy as np
import geopandas as gpd

import matplotlib.pyplot as plt
import matplotlib.colors as colors
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from matplotlib.lines import Line2D
from matplotlib.patches import Patch


frequency_analysis= pd.read_csv(r'/projects/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6b_event_floodext_perCOUNTRY.csv')
countries_gdf = gpd.read_file(r'/gpfs/work2/0/einf2224/paper2/data/input/impact/admin_units/countries_studyarea.shp')
print('frequency_analysis:', frequency_analysis)
unique_country_ids = frequency_analysis['Index'].unique()


coast = cfeature.GSHHSFeature(scale='full')
fig = plt.figure(figsize=(16,6))
#gs = fig.add_gridspec(1, 3, width_ratios=[2, 2, 1.5], wspace=0.2)
gs = fig.add_gridspec(1, 2, width_ratios=[1, 1], wspace=0.2)

# add number of countries affected by nr. of events
ax3 = fig.add_subplot(gs[0, 1])
#data= pd.read_csv(r'/projects/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_event_damages_build_perCOUNTRY.csv')
data= pd.read_csv(r'/projects/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_event_damages_build_perCOUNTRY_goodzonal_new_unique.csv')
events=data.groupby('Variable')['index'].nunique()
count_of_sums = events.value_counts()
bars=ax3.bar(count_of_sums.index, count_of_sums, color='firebrick')
for bar in bars:
    ax3.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
             str(int(bar.get_height())), ha='center', va='bottom', fontsize=14)
ax3.set_xlabel('Nr. countries affected by a single event [-]', fontsize=14) 
ax3.set_ylabel('Nr. of events [-]', fontsize=14) 
ax3.set_ylim(0, 8500)
ax3.set_xticks(count_of_sums.index)
ax3.set_xticklabels(count_of_sums.index, rotation=0)
ax3.tick_params(axis='x', labelsize=14)
ax3.tick_params(axis='y', labelcolor='k', labelsize=14)
ax3.text(0.02, 0.93, 'b)', transform=ax3.transAxes, fontsize=14, fontweight='bold')

# flood frequency
ax1 = fig.add_subplot(gs[0, 0])
#plt.subplots_adjust(wspace=0.1) 
print('countries_gdf:', countries_gdf)
#countries = [countries_gdf.iloc[country_id]['name'] for country_id in unique_country_ids]
countries = [countries_gdf.iloc[country_id]['name'] if countries_gdf.iloc[country_id]['name'] != 'United Republic of Tanzania' else 'Tanzania' for country_id in unique_country_ids]
print('countries:', countries)
frequencies = [len(frequency_analysis[frequency_analysis['Index'] == country_id]['Sum']) / 1e4 for country_id in unique_country_ids]
print('frequencies:', frequencies)
ax1.bar(countries, frequencies, color='royalblue')  # You can choose a different color if you prefer
#ax1.set_xlabel('Country', fontsize=14)
ax1.set_ylabel('Flooding occurrence [events/year]', fontsize=14)
ax1.set_ylim(0, max(frequencies) + 0.1)
ax1.set_xticklabels(countries, rotation=45, ha='right')
ax1.tick_params(axis='x', labelsize=14)
ax1.tick_params(axis='y', labelcolor='k', labelsize=14)
ax1.text(0.02, 0.93, 'a)', transform=ax1.transAxes, fontsize=14, fontweight='bold')



'''
# damage frequency
damages= pd.read_csv(r'/projects/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6a_RPs_build_event_perCOUNTRY.csv')
unique_country_ids_damages = damages['Index'].unique()
ax2 = fig.add_subplot(gs[0, 1])
#countries = [countries_gdf.iloc[country_id]['name'] for country_id in unique_country_ids]
countries = [countries_gdf.iloc[country_id]['name'] if countries_gdf.iloc[country_id]['name'] != 'United Republic of Tanzania' else 'Tanzania' for country_id in unique_country_ids]
frequencies = []
for country_id in unique_country_ids:
    damages_country = damages[damages['Index'] == country_id]
    damages_1million = damages_country[damages_country['Total Damages'] > 1000000]
    frequency = len(damages_1million['Total Damages']) / 1e4
    frequencies.append(frequency)

ax2.bar(countries, frequencies, color='firebrick')  # You can choose a different color if you prefer
#ax2.set_xlabel('Country', fontsize=14)
ax2.set_ylabel('1 million euros exceedance [events/year]', fontsize=14)
ax2.set_ylim(0, max(frequencies) + 0.1)
ax2.set_xticklabels(countries, rotation=45, ha='right')
ax2.tick_params(axis='x', labelsize=14)
ax2.tick_params(axis='y', labelcolor='k', labelsize=14)
ax2.text(0.02, 0.93, 'b)', transform=ax2.transAxes, fontsize=14, fontweight='bold')
'''
#plt.subplots_adjust(left=0.05, right=0.95)
#plt.subplots_adjust(left=0.05, right=0.95, bottom=0.4)
plt.subplots_adjust(bottom=0.25)
fig.tight_layout()
fig.savefig('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/figures/p6b_flood_frequency_fig4_bars.png', dpi=600)
