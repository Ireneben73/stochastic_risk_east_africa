# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 16:03:39 2023

@author: ibe202
"""

# -*- coding: utf-8 -*-
"""
Created on Sat Jan  7 10:33:43 2023

@author: ibe202
"""

import os
import numpy as np
import pandas as pd




#%%
yrindex=0
filename = r'/projects/0/einf2224/paper2/data/input/meteo/STORMV4/STORM_DATA_IBTRACS_SI_1000_YEARS_' + str(yrindex) + '.txt'
#filename = r'C:\Users\ibe202\Desktop\STORM_test_2events.txt'

data = np.loadtxt(filename,delimiter=',')
yearall,monthall,tcnumberall,timeall,latall,lonall,presall,windall,rmaxall,distall=data[:,0],data[:,1],data[:,2],data[:,3],data[:,5],data[:,6],data[:,7],data[:,8],data[:,9],data[:,12]
df = pd.DataFrame(data=data)
df_file = df.rename(columns={0:'year',1:'month',2:'tc_index',3:'time',4:'basin',5:'lat',6:'lon',7:'pressure',8:'wind',9:'rmax',10:'category',11:'landfall',12:'distance',})
# Generate a unique identifier based on the year and the tc index
#df_file['tc_id']=df_file["year"].astype(str) + '_' + df_file["tc_index"].astype(str)

# XYYYDD: X=1000 year index, starts at 0; YYY=year, starts at 0; DD=TCnumber
df_file['tc_id']='{0:0=1d}'.format(yrindex) + (df_file["year"].astype(int)).map('{0:0=3d}'.format).astype(str) + (df_file["tc_index"].astype(int)).map('{0:0=2d}'.format).astype(str)
#print('TOTAL EVENT SET LENGTH', len(df_file.tc_id.unique()))

# Generate an empty dataframe where the filtered events will be saved at:
tc_eventset=[] # generate event dataset
tc_timesteps=[] # generate list of max timesteps per event filtered


# Loop over each event to filter only the events where the distance to land is less than 750km:
for storm_id in df_file.tc_id.unique().tolist():
    print(' STORM ID', storm_id)
    distslice=distall[df_file.tc_id==storm_id]
    
    # Filter events within the study area x=25,75; y=-5,-40, but keepig the event parts that go out of my study area and come back
    tc_event=df_file[df_file.tc_id == storm_id]
    df_mask_lon = (df_file.lon >= 25) & (df_file.lon <= 75)
    df_mask_lat = (df_file.lat >= -40) & (df_file.lat <= -5)
    tc_event_area=tc_event.where(df_mask_lon & df_mask_lat).dropna()
    min_timestep_area=tc_event_area.time.min()
    max_timestep_area=tc_event_area.time.max()
    tc_areafilter=tc_event[((tc_event.time).astype(int)>=min_timestep_area) & ((tc_event.time).astype(int)<=max_timestep_area)]
    #print(tc_areafilter)
    
    if any(distslice<750):
        #print('IDENTIFIER', storm_id)
        
        # Filter events that have a distance of less than 750km from land
        tc_distfilter750=tc_areafilter[tc_areafilter.tc_id == storm_id]
        # Filter events that have a distance of less than 1000km from land
        tc_distfilter1000=tc_distfilter750[tc_distfilter750['distance']<1000]
        
        # min & max timestep with distance less than 1000km from land. To find the 1st and last timestep I should filter the 750km filter
        # to avoid filtering data in between that is further than 1000km
        min_timestep_dist=tc_distfilter1000.time.min()
        max_timestep_dist=tc_distfilter1000.time.max()
        
        tc_distfilter=tc_distfilter750[((tc_distfilter750.time).astype(int)>=min_timestep_dist) & ((tc_distfilter750.time).astype(int)<=max_timestep_dist)]
        # Append all the events that are at less than 750km from land
        tc_eventset.append(tc_distfilter)
        #print(tc_eventset)

        # Calculate the maximum timesteps of each event
        #print(tc_distfilter.time.max())
        #tc_timesteps.append(tc_distfilter.time.max())
        
# Concatenate all the events that are at less than 750km from land
tc_eventset=pd.concat(tc_eventset)
#print('1st event set', tc_eventset)
#print('2nd event set',tc_eventset)


# Number of events remaining:
#print('FILTERED EVENT SET LENGTH', len(tc_eventset.tc_id.unique()))

# print average timesteps of all the events filtered
#print('AVERAGE TIMESTEP OF THE FILTERED EVENT SET', sum(tc_timesteps) / len(tc_timesteps)) # each timestep needs to be multiplied by 3h in STORM
    
# save the txt file
tc_eventset.to_csv(os.path.splitext(filename)[0]+'_filtered.txt', header=None, index=None, sep=',', mode='w')