# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 13:41:02 2022

@author: ibe202
"""


## IMPORT MODULES
import os
import numpy as np
import pandas as pd
import sys
sys.path.insert(1, r'/gpfs/home4/benitoli/papers/paper2/case_studies/storm2/models/holland')
import p1b_holland_model_ibl as hm
import math
from datetime import datetime
import p1c_write_spw_file_ibl as spw


#%%


filename = sys.argv[4]
#print(filename)
#startdate="201901" + sys.argv[1] + ' ' + sys.argv[2]
startdate=sys.argv[1] + ' ' + sys.argv[2]
startdate= datetime.strptime(startdate, "%Y%m%d %H")
tstop = 165 # Length of the track for example.. number of timesteps times 3h

storm_id=sys.argv[3]

# directory of the STORM event
outputdest=sys.argv[5] 


## READ STORM
#filename_txt=os.path.splitext(filename)[0]+'.txt'
dtype = [(float, )] * 13 + [(str, )]
data = np.loadtxt(filename,delimiter=',')
yearall,monthall,tcnumberall,timeall,latall,lonall,presall,windall,rmaxall,distall=data[:,0],data[:,1],data[:,2],data[:,3],data[:,5],data[:,6],data[:,7],data[:,8],data[:,9],data[:,12]
df = pd.DataFrame(data=data)
df_file = df.rename(columns={0:'year',1:'month',2:'tc_index',3:'time',4:'basin',5:'lat',6:'lon',7:'pressure',8:'wind',9:'rmax',10:'category',11:'landfall',12:'distance',13:'tc_id'})
#df_file['tc_id']='{0:0=1d}'.format(yrindex) + (df_file["year"].astype(int)).map('{0:0=3d}'.format).astype(str) + (df_file["tc_index"].astype(int)).map('{0:0=2d}'.format).astype(str)
#df_file.tc_id = (df_file["tc_index"].astype(int)).map('{0:0=6d}'.format).astype(str)
df_file['tc_id'] = (df_file.iloc[:,-1].astype(int)).map('{0:0=6d}'.format).astype(str)
#print(df_file)

#%%
#=============================================================================
# Constants from literature
#=============================================================================
alpha=0.55              # Deceleration of surface background wind - Lin & Chavas 2012
beta_bg=20.             # Angle of background wind flow - Lin & Chavas 2012
SWRF=0.85               # Empirical surface wind reduction factor (SWRF) - Powell et al 2005 
CF=0.915                # Wind conversion factor from 1 minute average to 10 minute average - Harper et al (2012)
Patm=101325.            # Atmospheric pressure

#==============================================================================
# Spyderweb specifications 
#==============================================================================
tc_radius=750           # Radius of the tropical cyclone, in km
n_cols=36               # Number of gridpoints in angular direction
n_rows=375              # Number of gridpoints in radial direction

#==============================================================================
# Create spiderweb file with the parametric wind model of Holland
#==============================================================================
storm = 0
#for storm_id in df_file.tc_id.unique().tolist():

if os.path.exists(outputdest + str(storm_id) + '.spw'):
  os.remove(outputdest + str(storm_id) + '.spw')
  print("Original file has been deleted")
else:
  print("The file does not exist")

## COUNT NUMBER OF STORMS
storm = storm + 1
#print(storm_id)
## SELECT SLICES WITH STORM DATA
latslice=latall[df_file.tc_id==storm_id]
lonslice=lonall[df_file.tc_id==storm_id]
windslice=windall[df_file.tc_id==storm_id]
presslice=presall[df_file.tc_id==storm_id]
timeslice=timeall[df_file.tc_id==storm_id]
rmaxslice=rmaxall[df_file.tc_id==storm_id]
distslice=distall[df_file.tc_id==storm_id]

# Reshuffling timesteps so that for each event it starts with 0
timeslice=timeslice-timeslice.min()


for j in range(1,len(rmaxslice)):
    ## DATA PER TIMESTEP
    lat0,lat1,lon0,lon1,t0,t1=latslice[j-1],latslice[j],lonslice[j-1],lonslice[j],timeslice[j-1],timeslice[j]
    U10,Rmax,P=windslice[j-1],rmaxslice[j-1],presslice[j-1]
    dt=(t1-t0)*3600*3.
    
    ## HOLLAND MODEL
    ## STEP 1: GENERATE THE SPIDERWEB MESH
    rlist,thetalist,xlist,ylist=hm.Generate_Spyderweb_mesh(n_cols,n_rows,tc_radius,lat0) # rlist = distance in km from eye, thetalist = wind angle, xlist = zonal distance in km, ylist = meridional distance in km
    latlist,lonlist=hm.Generate_Spyderweb_lonlat_coordinates(xlist,ylist,lat1,lon1)      # xlist and ylist converted to lon/lat coordinates
    
    ## STEP 2: CALCULATE THE BACKGROUND WIND
    [bg,ubg,vbg]=hm.Compute_background_flow(lon0,lat0,lon1,lat1,dt)                      # bg = translational speed (m/s), ubg/vbg are zonal/meridional components of bg
    
    ## STEP 3: SUBTRACT THE BACKGROUND FLOW FROM U10 (TROPICAL CYCLONE'S 10-METER WIND SPEED)
    Ugrad=(U10/SWRF)-(bg*alpha)                                                          # 10-minute maximum sustained surface wind (conversts surface wind to gradient level, and subtracts the background wind multiplied with a reduction factor alpha)
    
    P_mesh=np.zeros((xlist.shape))
    Pdrop_mesh=np.zeros((xlist.shape))
    up=np.zeros((xlist.shape))
    vp=np.zeros((xlist.shape))
    winddir=np.zeros((xlist.shape))
    
    ## STEP 4: CALCULATE WIND AND PRESSURE PROFILE USING THE HOLLAND MODEL
    for l in range(0,n_rows):
        r=rlist[0][l]                                                                     # Distance in km from the eye
        Vs,Ps=hm.Holland_model(lat0,P,Ugrad,Rmax,r)                                       # Calculate wind speed and pressure for this radius
        Vs=Vs*SWRF                                                                        # Convert back to 10-min surface wind speed
        
        P_mesh[:,l].fill(Ps)                                                              # Fill the circle with the computed pressure at the given radius
        Pdrop_mesh[:,l].fill((Patm-Ps))                                                   # Convert pressure to drop in pressure
        
        beta=hm.Inflowangle(r,Rmax,lat0)                                                  # Inflow angle, depends on the distance from the eye (r)
        for k in range(0,n_cols):                                                         # Compute wind components: wind speed at this radius + background windspeed + inflow angle
            ubp=alpha*(ubg*math.cos(math.radians(beta_bg))-np.sign(lat0)*vbg*math.sin(math.radians(beta_bg)))
            vbp=alpha*(vbg*math.cos(math.radians(beta_bg))+np.sign(lat0)*ubg*math.sin(math.radians(beta_bg)))
                               
            up[k,l]=-Vs*math.sin(thetalist[:,0][k]+beta)+ubp
            vp[k,l]=-Vs*math.cos(thetalist[:,0][k]+beta)+vbp
            winddir[k,l]=np.degrees(np.arctan2(up[k,l],vp[k,l]))+180                      # Wind direction in degrees, with 0/360 wind coming from the North
    
    u10=CF*up                                                                             # Convert zonal wind componenet from 1-minute average to 10-minute average
    v10=CF*vp                                                                             # Convert meridional wind componenet from 1-minute average to 10-minute average
    windfield=np.sqrt(u10**2.+v10**2.)                                                    # Calculate wind speed with Pythagoras
    time_out = t0*3                                                                       # STORM database timestep is three hourly
    
    ## ARRAY WITH ZEROS OF SIMILAR SIZE AS THE SPIDERWEB
    nodata_array_wind = np.zeros((windfield.shape))
    nodata_array_pressure = np.zeros((windfield.shape))
    
    ## WRITE TIMESTEP TO SPIDERWEB FILE
    spw.write_spw_file(outputdest + str(storm_id),n_cols,n_rows,tc_radius,startdate,nodata_array_wind,nodata_array_pressure,time_out,windfield,winddir,Pdrop_mesh,df_file,distslice,tstop,storm,j,lon0,lat0)

## MOVE FILES TO DEDICATED SUBFOLDER
#os.rename(filename,filename)
