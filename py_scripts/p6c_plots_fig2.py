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
from matplotlib.cm import ScalarMappable

def format_custom(value):
    # Determine the exponent of the value
    exponent = 0
    while abs(value) >= 10.0:
        value /= 10.0
        exponent += 1
    while abs(value) < 1.0:
        value *= 10.0
        exponent -= 1

    # Create a dictionary for larger superscript numbers
    
    superscript_large_digits = {
        '0': '⁰',
        '1': '¹',
        '2': '²',
        '3': '³',
        '4': '⁴',
        '5': '⁵',
        '6': '⁶',
        '7': '⁷',
        '8': '⁸',
        '9': '⁹',
        '-': '⁻',
    }
    
    #superscript_large_digits = tuple('⁰¹²³⁴⁵⁶⁷⁸⁹')

    # Format the value with two decimal places
    formatted_value = '{:.2f}'.format(value)
    print('formatted_value:', formatted_value)
    # Append the exponent as a larger superscript
    if exponent != 0:
        if exponent < 0:
            formatted_value += ' × 10'
            print('formatted_value:', formatted_value)
            formatted_value += '⁻'
            print('formatted_value:', formatted_value)
            formatted_value += f'${{^{exponent}}}$'
            print('formatted_value:', formatted_value)

        else:
            formatted_value += ' × 10'
            formatted_value += ''.join(superscript_large_digits[digit] for digit in str(abs(exponent)))
            formatted_value = formatted_value.replace(str(exponent), '${\\scalebox{1.5}{exponent}}$')
            
            
       

    return exponent

def as_si(x, ndp):
    s = '{x:0.{ndp:d}e}'.format(x=x, ndp=ndp)
    m, e = s.split('e')
    return r'{m:s}\times 10^{{{e:d}}}'.format(m=m, e=int(e))


# GDP & population per country data
gdp_pop_data=pd.read_csv('/projects/0/einf2224/paper2/data/input/impact/gdp_population.csv')

# Plots
country_names = {
    0: "Mozambique",
    1: "Glorioso Islands",
    2: "Comoros",
    3: "Reunion",
    4: "Swaziland",
    5: "South Africa",
    6: "Madagascar",
    7: "Mayotte",
    8: "Mauritius",
    9: "Tanzania",
    10: "Seychelles"
}

rps_event_loss=pd.read_csv('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_RPs_build_event_perCOUNTRY_new.csv')
rps_annual_loss=pd.read_csv('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_RPs_build_annual_perCOUNTRY_new_EVENTnrs.csv')
rps_event_pop=pd.read_csv('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_RPs_pop_event_perCOUNTRY_new.csv')
rps_annual_pop=pd.read_csv('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_RPs_pop_annual_perCOUNTRY_new_EVENTnrs.csv')

# Plots
num_rows=8 + 1
num_cols=2
#fig, axes = plt.subplots(num_rows, num_cols, figsize=(15, 22))  # Adjust figsize as needed
fig, axes = plt.subplots(num_rows, num_cols, figsize=(15, 23)) 

additional_row_axes = axes[0]  # This selects the first row of subplots

## PLOTS FOR ALL THE STUDY AREA
# Losses 
# Exceedance probabilities
rps_event_loss_allarea=pd.read_csv('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_RPs_build_event_allAREA.csv')
rps_annual_loss_allarea=pd.read_csv('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_RPs_build_annual_allAREA_EVENTnrs.csv')
rps_event_pop_allarea=pd.read_csv('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_RPs_pop_event_allAREA.csv')
rps_annual_pop_allarea=pd.read_csv('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_RPs_pop_annual_allAREA_EVENTnrs.csv')

event_counts_loss_allarea=rps_annual_loss_allarea['Event Count']

# make the discrete colorbar
#cmap = plt.cm.rainbow
#bounds = np.arange(int(event_counts_loss_allarea.min()), int(event_counts_loss_allarea.max()) + 2, 1) - 0.5
#norm = colors.BoundaryNorm(bounds, cmap.N)

cmap = plt.get_cmap('rainbow')
bounds = np.arange(int(event_counts_loss_allarea.min()), int(event_counts_loss_allarea.max()) + 2, 1) - 0.5
norm = colors.BoundaryNorm(bounds, cmap.N)
edgecolors_loss_allarea = [cmap(norm(value)) for value in event_counts_loss_allarea]

ax1 = additional_row_axes[0]
print('rps_annual_loss:', rps_annual_loss)
ax1.scatter(rps_event_loss_allarea['Return Period'], (rps_event_loss_allarea['Total Damages']/1e6), marker='x', c='dimgray', s=80)
annual=ax1.scatter(rps_annual_loss_allarea['Return Period'], (rps_annual_loss_allarea['Annual Damages']/1e6), marker='o', facecolor='none', edgecolor=edgecolors_loss_allarea, linewidth=2, cmap=cmap, norm=norm, s=100)
#annual=ax1.scatter(rps_annual_loss_allarea['Return Period'], (rps_annual_loss_allarea['Annual Damages']/1e6), marker='o', c=event_counts_loss_allarea, cmap='rainbow', norm=norm, edgecolor='dimgray', label='Per year')
#cbar_annual = plt.colorbar(annual, ax=ax1, orientation='vertical', ticks=np.unique(event_counts_loss_allarea), pad=0.01, aspect=20, label='Nr. Events [-]')

ax1.set_xscale('log')
ax1.tick_params(axis='y', labelsize=14)
ax1.tick_params(axis='x', labelsize=14)
ax1.set_ylim(0, (rps_annual_loss_allarea['Annual Damages'].max())/1e6*1.05)
ax1.text(0.025, 0.90, 'East coast of Africa (All study area)', transform=ax1.transAxes,
         fontsize=16, verticalalignment='top', color='black')
gdp = gdp_pop_data['GDP (million €)'].sum()
print('GDP', gdp)
aed_loss = ((((rps_annual_loss_allarea['Annual Damages'].sum())/10000)/1e6)/gdp)
print('aed_loss:', aed_loss)
ax1.text(0.025, 0.70, r"EAD/GDP: ${0:s}$".format(as_si(aed_loss,2)), transform=ax1.transAxes,
     fontsize=16, verticalalignment='top', color='black')
ax1.set_title('Damages [million €]', fontsize=16)

# Population for the whole study area
# Exceedance probabilities
event_counts_pop_allarea=rps_annual_pop_allarea['Event Count']
edgecolors_pop_allarea = [cmap(norm(value)) for value in event_counts_pop_allarea]
ax2 = additional_row_axes[1]
ax2.scatter(rps_event_pop_allarea['Return Period'], (rps_event_pop_allarea['Total Damages']/1e3), marker='x', c='dimgray', s=100) # this should be population
#ax2.scatter(rps_annual_pop_allarea['Return Period'], (rps_annual_pop_allarea['Annual Damages']/1e3), marker='o', facecolor='none', edgecolor='dimgray', label='Per year')
ax2.scatter(rps_annual_pop_allarea['Return Period'], (rps_annual_pop_allarea['Annual Damages']/1e3), marker='o', facecolor='none', edgecolor=edgecolors_pop_allarea, linewidth=2, cmap=cmap, norm=norm, s=100)
#ax2.scatter(rps_annual_loss['Return Period'], rps_annual_loss['Total Damages'], marker='x', c='navy', s=25, label='Annual losses')
ax2.set_xscale('log')
ax2.tick_params(axis='y', labelsize=14)
ax2.tick_params(axis='x', labelsize=14)
ax2.set_ylim(0, (rps_annual_pop_allarea['Annual Damages'].max())/1e3*1.05) # this should be population
ax2.text(1.22, 0.90, 'East coast of Africa (All study area)', transform=ax1.transAxes,
         fontsize=16, verticalalignment='top', color='black')
pop = gdp_pop_data['Population'].sum()
print('POP', pop)
aap = (((rps_annual_pop_allarea['Annual Damages'].sum())/10000)/pop)
formatted_aap = as_si(aap,2)
ax2.text(1.22, 0.70, r"EAAP/Pop.: ${0:s}$".format(as_si(aap,2)), transform=ax1.transAxes,
     fontsize=16, verticalalignment='top', color='black')
ax2.set_title('Affected population [x1000]', fontsize=16)

## PLOTS PER COUNTRY
unique_country_ids = rps_annual_loss['Index'].unique()
for i, country_id in enumerate(unique_country_ids):
    print('I:', i)
    ax1, ax2 = axes[i + 1]
    # Losses
    rps_event_loss_country=rps_event_loss[rps_event_loss['Index']==country_id] 
    rps_annual_loss_country=rps_annual_loss[rps_annual_loss['Index']==country_id] 
    event_counts_loss=rps_annual_loss_country['Event Count']
    edgecolors_loss_country = [cmap(norm(value)) for value in event_counts_loss]

    ax1.scatter(rps_event_loss_country['Return Period'], rps_event_loss_country['Total Damages']/1e6, marker='x', c='dimgray', s=100)#, label='Per event')    
    #cmap = plt.cm.rainbow
    #bounds = np.arange(int(event_counts_loss.min()), int(event_counts_loss.max()) + 2, 1) - 0.5
    #norm = colors.BoundaryNorm(bounds, cmap.N)
    
    '''
    cmap = plt.get_cmap('rainbow')
    #norm = Normalize(vmin=min(event_counts_loss), vmax=max(event_counts_loss))
    bounds = np.arange(int(event_counts_loss.min()), int(event_counts_loss.max()) + 2, 1) - 0.5
    norm = colors.BoundaryNorm(bounds, cmap.N)
    edgecolors = [cmap(norm(value)) for value in event_counts_loss]
    '''
    #annual=ax1.scatter(rps_annual_loss_country['Return Period'], (rps_annual_loss_country['Total Damages'])/1e6, marker='o', c=event_counts_loss, cmap='rainbow', norm=norm, edgecolor='dimgray')#, s=50)#, label='Per year')
    annual=ax1.scatter(rps_annual_loss_country['Return Period'], (rps_annual_loss_country['Total Damages'])/1e6, marker='o', facecolor='none', edgecolor=edgecolors_loss_country, linewidth=2, cmap=cmap, norm=norm, s=100)#, label='Per year')

    sm = ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])  # Pass an empty array or list here


    ax1.set_xscale('log')
    ax1.tick_params(axis='x', labelsize=14)
    ax1.tick_params(axis='y', labelsize=14)
    ax1.set_xlim(1, 15000) 
    ax1.set_ylim(0, (rps_annual_loss_country['Total Damages'].max())/1e6*1.05) 

    ax1.text(0.025, 0.90, country_names[country_id], transform=ax1.transAxes,
             fontsize=16, verticalalignment='top', color='black')
    gdp_country = gdp_pop_data['GDP (million €)'][gdp_pop_data['Index'] == country_id].values[0]
    aed_loss_country = ((((rps_annual_loss_country['Total Damages'].sum())/10000)/1e6)/gdp_country)
    ax1.text(0.025, 0.70, r"EAD/GDP: ${0:s}$".format(as_si(aed_loss_country,2)), transform=ax1.transAxes,
     fontsize=16, verticalalignment='top', color='black')
     
    print('MAX rps_annual_loss_country:', rps_annual_loss_country['Total Damages'].max())
    print('MAX rps_event_loss_country:', rps_event_loss_country['Total Damages'].max())
    
    # Population
    rps_event_pop_country=rps_event_pop[rps_event_pop['Index']==country_id] 
    rps_annual_pop_country=rps_annual_pop[rps_annual_pop['Index']==country_id] 
    event_counts_pop=rps_annual_pop_country['Event Count']
    edgecolors_pop_country = [cmap(norm(value)) for value in event_counts_pop]
    ax2.scatter(rps_event_pop_country['Return Period'], rps_event_pop_country['Total Damages']/1e3, marker='x', c='dimgray', s=100)#, label='Per event')
    #ax2.scatter(rps_annual_pop_country['Return Period'], (rps_annual_pop_country['Total Damages'])/1e3, marker='o', c=event_counts_pop, cmap='rainbow', edgecolor='dimgray')#, s=50)#, 
    ax2.scatter(rps_annual_pop_country['Return Period'], (rps_annual_pop_country['Total Damages'])/1e3, marker='o', facecolor='none', edgecolor=edgecolors_pop_country,  linewidth=2, cmap=cmap, norm=norm, s=100)#, 
    #cbar_annual = plt.colorbar(annual, ax=ax2, orientation='vertical', ticks=np.unique(event_counts_loss), pad=0.01, aspect=20, label='Nr. Events [-]')
    ax2.tick_params(axis='y', labelcolor='k', labelsize=14)
    ax2.tick_params(axis='x', labelsize=14)
    ax2.set_xlim(1, 15000) 
    ax2.set_ylim(0, (rps_annual_pop_country['Total Damages'].max())/1e3*1.05) 
    ax2.set_xscale('log')
    ax2.text(1.22, 0.90, country_names[country_id], transform=ax1.transAxes,
             fontsize=16, verticalalignment='top', color='black')
    pop_country = gdp_pop_data['Population'][gdp_pop_data['Index'] == country_id].values[0]
    aap = (((rps_annual_pop_country['Total Damages'].sum())/10000)/pop_country)
    ax2.text(1.22, 0.70, r"EAAP/Pop.: ${0:s}$".format(as_si(aap,2)), transform=ax1.transAxes,
         fontsize=16, verticalalignment='top', color='black')

    # Settings for certain rows
    if i == num_rows - 1 - 1:  # Only show y label and ticks for the last row
        ax1.set_xlabel('Return period [-]', fontsize=14)
        ax2.set_xlabel('Return period [-]', fontsize=14)
        ax1.tick_params(axis='x', labelsize=14)
        ax2.tick_params(axis='x', labelsize=14)

    else:
        #ax1.set_xticklabels([])  # Remove x-axis tick labels for other rows
        #ax2.set_xticklabels([])  # Remove y-axis tick labels for other rows
        ax1.tick_params(axis='x', labelsize=14)
        ax2.tick_params(axis='x', labelsize=14)
        

        
cbar = plt.colorbar(sm, cax=fig.add_axes([0.37, 0.028, 0.25, 0.01]), orientation='horizontal', ticks=np.arange(1, 10))
cbar.set_label('Number of events [-]', fontsize=16)
cbar.ax.tick_params(labelsize=16)

legend_labels = ['Single event', 'Annual aggregate']  # Labels from that specific subplot
legend_elements = [plt.Line2D([0], [0], marker='x', color='none', markeredgecolor='dimgray', markersize=10, markeredgewidth=2, label='Single event'),
                   plt.Line2D([0], [0], marker='o', color='none', markeredgecolor='dimgray', markersize=10, markeredgewidth=2, label='Annual aggregate')]

fig.legend(handles=legend_elements, labels=legend_labels, loc='lower center', bbox_to_anchor=(0.5, 0.035), fontsize=16, frameon=False, ncol=2)
#plt.subplots_adjust(left=0.1, right=0.9, top=0.95, bottom=0.05, wspace=0.2, hspace=0.2)
plt.subplots_adjust(left=0.1, right=0.9, top=0.98, bottom=0.08, wspace=0.2, hspace=0.2)

fig.savefig('/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/figures/p6c_RPs_optiond_all.png', dpi=600)
