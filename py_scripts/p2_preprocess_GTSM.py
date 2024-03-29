#! /usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 20 17:25:23 2023
@author: benitoli
"""

import sys
sys.path.insert(0, '/projects/0/einf2224/paper2/scripts/gtsm_template/')
import numpy as np
import os
import shutil
import fnmatch
import datetime
import templates
from distutils.dir_util import copy_tree
import glob 
import pandas as pd

templatedir="/projects/0/einf2224/paper2/scripts/gtsm_template/model_input_template"
modelfilesdir="/projects/0/einf2224/paper2/scripts/gtsm_template/model_files_common"
restart_path="/projects/0/einf2224/paper2/scripts/gtsm_template/restart_files"
storm_id=sys.argv[3]

# new directory for the specific STORM event model
#newdir="/projects/0/einf2224/paper2/scripts/model_runs/" + storm_id + "/gtsm" 
newdir=sys.argv[5]


#try:
#   os.stat(newdir)               
#   raise Exception("Directory already exists"+newdir) #this is to avoid overwritting runs that are there already by mistake
#except OSError:

# copy restart files (for each partition) to newdir
restart_files=glob.glob(restart_path + "/gtsm_fine_*_" + sys.argv[1] + '_' + sys.argv[2] + '0000_rst.nc')
for restart_file in restart_files:
    shutil.copy(restart_file, newdir)

# copy template files to newdir
print("copying ",templatedir," to ",newdir)
copy_tree(templatedir,newdir)#,symlinks=False,ignore=None)
    
# copy static model files
copy_tree(modelfilesdir, newdir)

# calculate tstop time for GTSM
storm_filename = sys.argv[4]
data = np.loadtxt(storm_filename,delimiter=',')
yearall,monthall,tcnumberall,timeall,latall,lonall,presall,windall,rmaxall,distall=data[:,0],data[:,1],data[:,2],data[:,3],data[:,5],data[:,6],data[:,7],data[:,8],data[:,9],data[:,12]
df = pd.DataFrame(data=data)
df_file = df.rename(columns={0:'year',1:'month',2:'tc_index',3:'time',4:'basin',5:'lat',6:'lon',7:'pressure',8:'wind',9:'rmax',10:'category',11:'landfall',12:'distance',13:'tc_id'})
df_file['tc_id'] = (df_file.iloc[:,-1].astype(int)).map('{0:0=6d}'.format).astype(str)
timeslice=timeall[df_file.tc_id==storm_id]
#print('TIMESLICE', timeslice)
timeslice=timeslice-timeslice.min()
rmaxslice=rmaxall[df_file.tc_id==storm_id]
#print('RMAX:', rmaxslice)
for j in range(1,len(rmaxslice)):
    tstop=(timeslice[j-1])*3 + int(sys.argv[2]) + 1 + 48 # we add 48 hours!

# spiderweb file from the holland model     
holland_file="" + storm_id + ".spw" 

## change templates
#-------------------
keywords_MDU={'REFDATE':sys.argv[1],'TSTART':sys.argv[2],'TSTOP':str(tstop), 'RESTART':"gtsm_fine_" + sys.argv[1] + '_' + sys.argv[2] + '0000_rst.nc'} #tstop still needs to be calculated!
templates.replace_all(os.path.join(newdir,"gtsm_fine.mdu.template"), os.path.join(newdir,"gtsm_fine.mdu"),keywords_MDU,'%')
#templates.replace_all(os.path.join(newdir,"gtsm_fine_*.mdu.template"), os.path.join(newdir,"gtsm_fine_*.mdu"),keywords_MDU,'%')
mdu_template_files=glob.glob(os.path.join(newdir,"gtsm_fine_*.mdu.template"))
for mdu_template_file in mdu_template_files:
    # Construct the output file name with the .mdu extension
    mdu_file = os.path.splitext(mdu_template_file)[0] #+ ".mdu"
    # Rewrite the restart file for each partition
    keywords_MDU_partition={'REFDATE':sys.argv[1],'TSTART':sys.argv[2],'TSTOP':str(tstop), 'RESTART':os.path.basename(os.path.splitext(mdu_file)[0]) + '_' + sys.argv[1] + '_' + sys.argv[2] + '0000_rst.nc'} #tstop still needs to be calculated!
    # Replace the keywords in the input file and write to the output file
    templates.replace_all(mdu_template_file, mdu_file, keywords_MDU_partition, '%')


keywords_EXT={'METEOFILE_GLOB_W':holland_file,'METEOFILE_GLOB_P':holland_file}
templates.replace_all(os.path.join(newdir,'gtsm_forcing.ext.template'),os.path.join(newdir,'gtsm_forcing.ext'),keywords_EXT,'%') 

#keywords_QSUB={'NDOM':str(ndom),'NNODE':str(nnode),'JOBNAME':workfolder} 
#templates.replace_all(os.path.join(templates_path,'%s.template'%(shfile)),os.path.join(rundir,'%s'%(shfile)),keywords_QSUB,'%')