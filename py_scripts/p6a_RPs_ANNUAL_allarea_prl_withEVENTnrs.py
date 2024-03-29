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
admin_unit=sys.argv[2]

events=pd.read_csv(r'/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_event_damages_'+ asset + '.csv')#.head(40)

#events_sorted = events.sort_values(by='Sum', ascending=False)
#events = events_sorted.head(30)
#print(events)
events['Year'] = events['Variable'].str.extract(r'(stormid_\d{4})')
print(events)
#event_counts = events['Year'].value_counts().sort_index().tolist()
#event_grouped = events.groupby(['Variable', 'Year'], as_index=False)['Sum'].sum()
#print('event_submodel:', event_grouped)
events_sorted = events.sort_values(by='Year', ascending=True)
print(events_sorted)
event_counts = events_sorted['Year'].value_counts().sort_index().tolist()
print('event_counts:', event_counts)
print('length event_counts:', len(event_counts))
# Group and sum values by the modified variable names

annual = events_sorted.groupby('Year', as_index=False)['Sum'].sum()
annual['Event_counts']=event_counts
print(annual)
'''
annual_damages=annual['Sum']
print('length annual damages:', len(annual_damages))
print('annual_damages:', annual_damages)
annual_damages=annual_damages[annual_damages != 0]
print('length annual damages:', len(annual_damages))
event_counts = [event_count for event_count, damage in zip(event_counts, annual_damages) if damage != 0]
print('length event_counts:', len(event_counts))
print('annual_damages:', annual_damages)
annual_damages_sorted=annual.sort_values(by='Sum', ascending=False)
print('annual_damages_sorted:', annual_damages_sorted)
'''

annual_sorted=annual.sort_values(by='Sum', ascending=False)
print('annual_sorted:', annual_sorted)
annual_damages=annual_sorted['Sum']
print('length annual damages:', len(annual_damages))
print('annual_damages:', annual_damages)
annual_damages=annual_damages[annual_damages != 0]
print('length annual damages:', len(annual_damages))
print('event_counts:', event_counts)
event_counts_sorted=annual_sorted['Event_counts']
event_counts_sorted = [event_count for event_count, damage in zip(event_counts_sorted, annual_damages) if damage != 0]
print('event_counts_0:', event_counts_sorted)

print('START CALCULATING RETURN PERIODS', flush=True)    
if annual_damages is not None and annual_damages.size > 0:  ## TO DO - need to change this!
    # Rank the variables based on the total damages per year
    '''
    def get_return_periods(x, extremes_rate, a=0.0):
        #b = 1.0 - 2.0*a
        b=0
        ranks = (len(x) + 1) - stats.rankdata(x, method="average")
        freq = ((ranks - a) / (10000 + b)) * extremes_rate
        rps = 1 / freq
        return ranks, rps
    '''    
    def get_return_periods(x, extremes_rate, a=0.0):
        b=1.0 - 2.0*a
        ranks = (len(x) + 1) - stats.rankdata(x, method="average")
        freq = ((ranks - a) / (10000 + b)) * extremes_rate
        rps = 1 / freq
        return ranks, rps
        
    ranks, return_periods = get_return_periods(annual_damages.tolist(), extremes_rate=1.0)
    print('return_periods:', (return_periods))
    print('annual_damages:', (list(annual_damages)))
    print('event_counts:', (event_counts))
    data = {'Return_Period': return_periods, 'Annual_Damage': list(annual_damages), 'Event_Counts': event_counts_sorted}
    df = pd.DataFrame(data)
    print('RETURN PERIODS HAVE BEEN CALCULATED', flush=True)    
    def calculate_expected_Annual_Damages(Annual_Damages, return_periods, extremes_rate):
        expected_damages=np.array(Annual_Damages)/np.array(return_periods)
        total_expected_Annual_Damages = np.sum(expected_damages)
        return total_expected_Annual_Damages
    #print('START CALCULATING ANNUAL EXPECTED DAMAGES', flush=True)     
    #annual_expected_damages_values = calculate_expected_Annual_Damages(df['Annual_Damage'],df['Return_Period'], 1.0)
    rps_values_list = df['Return_Period'].tolist()
    annual_damages_list = df['Annual_Damage'].tolist()
    event_counts_list = df['Event_Counts'].tolist()


print('SAVING DATA TO THE CSV', flush=True)
for i in range(len(ranks)):
    rank=ranks[int(i)-1]
    annual_damage=annual_damages_list[int(i)-1]
    rps=rps_values_list[int(i)-1]
    event_count=event_counts_list[int(i)-1]
    if not os.path.exists('output_postprocess/p6a_RPs_' + asset + '_' + admin_unit +'_EVENTnrs.csv'):
      with open('output_postprocess/p6a_RPs_' + asset + '_' + admin_unit + '_EVENTnrs.csv', 'w', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['Rank', 'Annual Damages', 'Return Period', 'Event Count'])
    
    with open('output_postprocess/p6a_RPs_' + asset + '_' + admin_unit + '_EVENTnrs.csv', 'a', encoding='utf-8') as csvfile:
      writer = csv.writer(csvfile, delimiter=',')
      writer.writerow([rank, annual_damage, rps, event_count])

'''      
#for country_id in index:
if not os.path.exists('output_postprocess/p6a_event_expected_damage_' + asset + '_' + admin_unit + '.csv'):
  with open('output_postprocess/p6a_event_expected_damage_' + asset + '_' + admin_unit + '.csv', 'w', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    writer.writerow(['AED'])

with open('output_postprocess/p6a_event_expected_damage_' + asset + '_' + admin_unit + '.csv', 'a', encoding='utf-8') as csvfile:
  writer = csv.writer(csvfile, delimiter=',')
  writer.writerow([annual_expected_damages_values])
'''
