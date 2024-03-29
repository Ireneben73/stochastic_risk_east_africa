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

# NOTE THAT ALL THE NAME VARIABLES ARE STILL FOR FLOODEXT AND NOT FOR SURGE

events=pd.read_csv(r'/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6d_surge_perEVENT_v2.csv')
events_floodexts=events['max_surge']

#events=pd.read_csv(r'/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6d_water_level_perEVENT.csv')
#events_floodexts=events['max_wl']

print('events_floodexts:', events_floodexts)

print('START CALCULATING RETURN PERIODS', flush=True)    
if events_floodexts is not None and events_floodexts.size > 0:  ## TO DO - need to change this!
    # Rank the variables based on the total floodexts per year
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
        b = 1.0 - 2.0*a
        ranks = (len(x) + 1) - stats.rankdata(x, method="average")
        freq = ((ranks - a) / (len(x) + b)) * extremes_rate
        rps = 1 / freq
        return ranks, rps
    total_years=10000
    #print('LIST OF EVENTS:', events_floodexts.tolist())
    #print('LIST OF EVENTS:', list(events_floodexts.values()))
    extremes_rate=len(events_floodexts.tolist())/total_years
    #extremes_rate=len(list(events_floodexts.values()))/total_years
    #print('extremes_rate:', extremes_rate)

    ranks, return_periods = get_return_periods(events_floodexts.tolist(), extremes_rate=extremes_rate)
    data = {'Return_Period': return_periods, 'Event_floodext': list(events_floodexts)}
    df = pd.DataFrame(data)
    print('RETURN PERIODS HAVE BEEN CALCULATED', flush=True)    
    def calculate_expected_event_floodexts(event_floodexts, return_periods, extremes_rate):
        expected_floodexts=np.array(event_floodexts)/np.array(return_periods)
        total_expected_event_floodexts = np.sum(expected_floodexts)
        return total_expected_event_floodexts
    print('START CALCULATING ANNUAL EXPECTED floodextS', flush=True)     
    #annual_expected_floodexts_values = calculate_expected_event_floodexts(df['Event_floodext'],df['Return_Period'], 1.0)
    rps_values = df['Return_Period'].tolist()
    event_floodexts = df['Event_floodext'].tolist()


print('SAVING DATA TO THE CSV', flush=True)
for i in range(len(ranks)):
    rank=ranks[int(i)-1]
    event_floodext=event_floodexts[int(i)-1]
    rps=rps_values[int(i)-1]
    #if not os.path.exists('output_postprocess/p6a_RPs_waterlevel_perEVENT_allAREA.csv'):
    if not os.path.exists('output_postprocess/p6a_RPs_surge_perEVENT_allAREA.csv'):
      #with open('output_postprocess/p6a_RPs_waterlevel_perEVENT_allAREA.csv', 'w', encoding='utf-8') as csvfile:
      with open('output_postprocess/p6a_RPs_surge_perEVENT_allAREA.csv', 'w', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['Rank', 'Total floodexts', 'Return Period'])
    
    #with open('output_postprocess/p6a_RPs_waterlevel_perEVENT_allAREA.csv', 'a', encoding='utf-8') as csvfile:
    with open('output_postprocess/p6a_RPs_surge_perEVENT_allAREA.csv', 'a', encoding='utf-8') as csvfile:
      writer = csv.writer(csvfile, delimiter=',')
      writer.writerow([rank, event_floodext, rps])
'''      
#for country_id in index:
if not os.path.exists('output_postprocess/p6a_event_expected_floodext_' + asset + '_' + admin_unit + '.csv'):
  with open('output_postprocess/p6a_event_expected_floodext_' + asset + '_' + admin_unit + '.csv', 'w', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    writer.writerow(['AED'])

with open('output_postprocess/p6a_event_expected_floodext_' + asset + '_' + admin_unit + '.csv', 'a', encoding='utf-8') as csvfile:
  writer = csv.writer(csvfile, delimiter=',')
  writer.writerow([annual_expected_floodexts_values])
'''
