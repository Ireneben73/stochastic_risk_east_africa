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

events=pd.read_csv(r'/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6d_surge_perEVENT_v2.csv')
events_stormsurge=events['max_surge']

print('START CALCULATING RETURN PERIODS', flush=True)    
if events_stormsurge is not None and events_stormsurge.size > 0: 
    # Rank the variables based on the total stormsurge per year
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
    #print('LIST OF EVENTS:', events_stormsurge.tolist())
    #print('LIST OF EVENTS:', list(events_stormsurge.values()))
    extremes_rate=len(events_stormsurge.tolist())/total_years
    #extremes_rate=len(list(events_stormsurge.values()))/total_years
    #print('extremes_rate:', extremes_rate)

    ranks, return_periods = get_return_periods(events_stormsurge.tolist(), extremes_rate=extremes_rate)
    data = {'Return_Period': return_periods, 'Event_stormsurge': list(events_stormsurge)}
    df = pd.DataFrame(data)
    print('RETURN PERIODS HAVE BEEN CALCULATED', flush=True)    
    def calculate_expected_event_stormsurge(event_stormsurge, return_periods, extremes_rate):
        expected_stormsurge=np.array(event_stormsurge)/np.array(return_periods)
        total_expected_event_stormsurge = np.sum(expected_stormsurge)
        return total_expected_event_stormsurge
    print('START CALCULATING ANNUAL EXPECTED stormsurge', flush=True)     
    #annual_expected_stormsurge_values = calculate_expected_event_stormsurge(df['Event_stormsurge'],df['Return_Period'], 1.0)
    rps_values = df['Return_Period'].tolist()
    event_stormsurge = df['Event_stormsurge'].tolist()


print('SAVING DATA TO THE CSV', flush=True)
for i in range(len(ranks)):
    rank=ranks[int(i)-1]
    Event_stormsurge=event_stormsurge[int(i)-1]
    rps=rps_values[int(i)-1]
    if not os.path.exists('output_postprocess/p6a_RPs_surge_perEVENT_allAREA.csv'):
      #with open('output_postprocess/p6a_RPs_waterlevel_perEVENT_allAREA.csv', 'w', encoding='utf-8') as csvfile:
      with open('output_postprocess/p6a_RPs_surge_perEVENT_allAREA.csv', 'w', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['Rank', 'Total stormsurge', 'Return Period'])
    
    with open('output_postprocess/p6a_RPs_surge_perEVENT_allAREA.csv', 'a', encoding='utf-8') as csvfile:
      writer = csv.writer(csvfile, delimiter=',')
      writer.writerow([rank, Event_stormsurge, rps])
