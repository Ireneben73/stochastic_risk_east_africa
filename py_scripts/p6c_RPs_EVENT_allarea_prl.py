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

events=pd.read_csv(r'/gpfs/work2/0/einf2224/paper2/scripts/py_scripts/output_postprocess/p6c_event_damages_'+ asset + '.csv')
events_damages=events['Sum']
events_damages=events_damages[events_damages != 0]
print('events_damages:', events_damages)

print('START CALCULATING RETURN PERIODS', flush=True)    
if events_damages is not None and events_damages.size > 0:  ## TO DO - need to change this!
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
        b = 1.0 - 2.0*a
        ranks = (len(x) + 1) - stats.rankdata(x, method="average")
        freq = ((ranks - a) / (len(x) + b)) * extremes_rate
        rps = 1 / freq
        return ranks, rps
    total_years=10000
    #print('LIST OF EVENTS:', events_damages.tolist())
    #print('LIST OF EVENTS:', list(events_damages.values()))
    extremes_rate=len(events_damages.tolist())/total_years
    #extremes_rate=len(list(events_damages.values()))/total_years
    #print('extremes_rate:', extremes_rate)

    ranks, return_periods = get_return_periods(events_damages.tolist(), extremes_rate=extremes_rate)
    data = {'Return_Period': return_periods, 'Event_Damage': list(events_damages)}
    df = pd.DataFrame(data)
    print('RETURN PERIODS HAVE BEEN CALCULATED', flush=True)    
    def calculate_expected_event_damages(event_damages, return_periods, extremes_rate):
        expected_damages=np.array(event_damages)/np.array(return_periods)
        total_expected_event_damages = np.sum(expected_damages)
        return total_expected_event_damages
    print('START CALCULATING ANNUAL EXPECTED DAMAGES', flush=True)     
    annual_expected_damages_values = calculate_expected_event_damages(df['Event_Damage'],df['Return_Period'], 1.0)
    rps_values = df['Return_Period'].tolist()
    event_damages = df['Event_Damage'].tolist()


print('SAVING DATA TO THE CSV', flush=True)
for i in range(len(ranks)):
    rank=ranks[int(i)-1]
    event_damage=event_damages[int(i)-1]
    rps=rps_values[int(i)-1]
    if not os.path.exists('output_postprocess/p6a_RPs_' + asset + '_' + admin_unit +'.csv'):
      with open('output_postprocess/p6a_RPs_' + asset + '_' + admin_unit + '.csv', 'w', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['Rank', 'Total Damages', 'Return Period'])
    
    with open('output_postprocess/p6a_RPs_' + asset + '_' + admin_unit + '.csv', 'a', encoding='utf-8') as csvfile:
      writer = csv.writer(csvfile, delimiter=',')
      writer.writerow([rank, event_damage, rps])
