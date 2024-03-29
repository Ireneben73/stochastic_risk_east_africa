import hydromt
import xarray as xr
import numpy as np
import pandas as pd
import geopandas as gpd
import re
import os
import sys
import csv
import math
from scipy import stats

import dask
import dask.array as da
import dask.dataframe as dd
from dask import delayed, compute
from dask.distributed import Client

asset=sys.argv[1]

events=pd.read_csv(r'/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_event_damages_' + asset + '_perCOUNTRY_goodzonal_new_unique.csv')
print('events:', events) 

print('START CALCULATING RETURN PERIODS', flush=True)  
for country_id in events['index'].unique(): 
    print('country_id:', country_id)
    events_country=events[events['index']==country_id]
    events_country['Year'] = events_country['Variable'].str.extract(r'(stormid_\d{4})')
    event_grouped = events_country.groupby(['Variable', 'Year'], as_index=False)['Sum'].sum()
    print('event_submodel:', event_grouped)
    event_counts = event_grouped['Year'].value_counts().sort_index().tolist()
    print('events_country:', events_country)
    print('event_counts:', event_counts)
    # Group and sum values by the modified variable names
    annual = events_country.groupby('Year', as_index=False)['Sum'].sum()  
    annual_damages=annual['Sum']
    annual_damages=annual_damages[annual_damages != 0]
    print('annual_damages:', annual_damages)
    if annual_damages is not None and annual_damages.size > 0:  ## TO DO - need to change this!
        # Rank the variables based on the total damages per year
        def get_return_periods(x, extremes_rate, a=0.0):
            b=1.0 - 2.0*a
            ranks = (len(x) + 1) - stats.rankdata(x, method="average")
            freq = ((ranks - a) / (10000 + b)) * extremes_rate
            rps = 1 / freq
            return ranks, rps
            

        #print('LIST OF EVENTS:', events_damages.tolist())
        #print('LIST OF EVENTS:', list(events_damages.values()))
        ranks, return_periods = get_return_periods(annual_damages.tolist(), extremes_rate=1.0)
        #extremes_rate=len(list(events_damages.values()))/total_years
        #print('extremes_rate:', extremes_rate)
    
        #data = {'Return_Period': return_periods, 'Annual_Damage': list(annual_damages)}
        data = {'Return_Period': return_periods, 'Annual_Damage': list(annual_damages), 'Event_Counts': event_counts}
        print('data:', data)
        df = pd.DataFrame(data)
        print('RETURN PERIODS HAVE BEEN CALCULATED', flush=True)    
        def calculate_expected_Annual_Damages(Annual_Damages, return_periods, extremes_rate):
            expected_damages=np.array(Annual_Damages)/np.array(return_periods)
            total_expected_Annual_Damages = np.sum(expected_damages)
            return total_expected_Annual_Damages
        print('START CALCULATING ANNUAL EXPECTED DAMAGES', flush=True)     
        annual_expected_damages_values = calculate_expected_Annual_Damages(df['Annual_Damage'],df['Return_Period'], 1.0)
        rps_values_list = df['Return_Period'].tolist()
        annual_damages_list = df['Annual_Damage'].tolist()
        event_counts_list = df['Event_Counts'].tolist()
    
    
    print('SAVING DATA TO THE CSV', flush=True)
    for i in range(len(ranks)):
        rank=ranks[int(i)-1]
        annual_damage=annual_damages_list[int(i)-1]
        rps=rps_values_list[int(i)-1]
        event_count=event_counts_list[int(i)-1]
        if not os.path.exists('output_postprocess/p6a_RPs_' + asset + '_annual_perCOUNTRY_new_EVENTnrs.csv'):
          with open('output_postprocess/p6a_RPs_' + asset + '_annual_perCOUNTRY_new_EVENTnrs.csv', 'w', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(['Index', 'Rank', 'Total Damages', 'Return Period', 'Event Count'])
        
        with open('output_postprocess/p6a_RPs_' + asset + '_annual_perCOUNTRY_new_EVENTnrs.csv', 'a', encoding='utf-8') as csvfile:
          writer = csv.writer(csvfile, delimiter=',')
          writer.writerow([country_id, rank, annual_damage, rps, event_count])
          

    
    
