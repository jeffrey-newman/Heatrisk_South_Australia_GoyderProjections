# -*- coding: utf-8 -*-
"""
Created on Tue Aug 16 16:35:01 2016

@author: a1091793
"""

# -*- coding: utf-8 -*-
import pickle
from functools import reduce
import os
import errno

import imp
from imp import reload
imp.reload(EHFTimeseries); from EHFTimeseries import calcEHF
imp.reload(ReadStationList); from ReadStationList import readStationList
imp.reload(ListRawDataFiles); from ListRawDataFiles import listRawDataFiles

station_list = r"/Volumes/home/QNAP RTRR Folder/Data/South Australia Goyder-CSIRO downscaled future climates/Adelaide_Mt_Lofty_Ranges/StationList.txt"
stations = readStationList(station_list)
rootdir = r"/Volumes/home/QNAP RTRR Folder/Data/South Australia Goyder-CSIRO downscaled future climates/Adelaide_Mt_Lofty_Ranges/Adelaide_Mt_Lofty_Ranges"
data_files = listRawDataFiles(rootdir, stations)
work_dir = r"/Volumes/home/QNAP RTRR Folder/Data/South Australia Goyder-CSIRO downscaled future climates/Adelaide_Mt_Lofty_Ranges/Processed"




cwd = os.getcwd()

def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

num_files = len(data_files)
t95_vals = {}
i = 0
for time_series in data_files:
    i = i + 1
    os.chdir(work_dir)
    # gcm
    make_sure_path_exists(time_series[2])
    os.chdir(time_series[2])
    if time_series[2] not in t95_vals:
        t95_vals[time_series[2]] = {}
    #scenario
    make_sure_path_exists(time_series[3])
    os.chdir(time_series[3])
    if time_series[3] not in t95_vals[time_series[2]]:
        t95_vals[time_series[2]][time_series[3]] = {}
    #station
    make_sure_path_exists(time_series[4])
    os.chdir(time_series[4])
    if time_series[4] not in t95_vals[time_series[2]][time_series[3]]:
        t95_vals[time_series[2]][time_series[3]][time_series[4]] = []
    filename = "replicate_" + time_series[5] + ".txt"
    filename_t95 = "replicate_" + time_series[5] + "_t95.txt"
    msg = "Calculating EHF for " + time_series[2] + " " + time_series[3] + " " + time_series[4] + " replicate " + time_series[5] + "; file " + str(i) + " of " + str(num_files)
    print(msg)
    if (time_series[2] == "csiro.mk36"):
        t95 = calcEHF(time_series[0], filename, filename_t95)    
        t95_vals[time_series[2]][time_series[3]][time_series[4]].append([time_series[5], t95])

os.chdir(cwd)

with open('t95vals.pickle', 'wb') as f:
    # Pickle the 'data' dictionary using the highest protocol available.
    pickle.dump(t95_vals, f, pickle.HIGHEST_PROTOCOL)

column_hdrs = "GCM\tScenario\tStationNum\tReplicate\tT95\n"

t95_avg_vals = {}
for gcm, gcm_dicts in t95_vals.items():
    os.chdir(gcm)
    if gcm not in t95_avg_vals:
        t95_avg_vals[gcm] = {}
    if (gcm == "csiro.mk36"):
        for sc, sc_dicts in gcm_dicts.items(): 
            os.chdir(sc)
            if sc not in t95_avg_vals[gcm]:
                t95_avg_vals[gcm][sc] = {}
            for statn, t95_lists in sc_dicts.items():
                os.chdir(statn)
                if statn not in t95_avg_vals[gcm][sc]:
                    t95_avg_vals[gcm][sc][statn] = 0
                avg_t95 = reduce(lambda x, y: x + y[1], t95_lists, 0) / len(t95_lists)
    #            avg_t95 = sum(statn_lists)/len(statn_lists)
                t95vals_file = "t95_vals.txt"
                with open(t95vals_file, 'w') as f_t95:
                    f_t95.write(column_hdrs)
                    for t95_vals in t95_lists:
                        t95_str = gcm + "\t" + sc + "\t" + statn + "\t" + t95_vals[0] + "\t" + t95_vals[1] + "\n"
                        f_t95.write(t95_str)
                t95avg_file = "t95_avg.txt"
                with open(t95avg_file, 'w') as f_avg:
                    f_avg.write(avg_t95)
                t95_avg_vals[gcm][sc][statn] = avg_t95

with open('t95avg.pickle', 'wb') as f2:
    # Pickle the 'data' dictionary using the highest protocol available.
    pickle.dump(t95_avg_vals, f2, pickle.HIGHEST_PROTOCOL)              
            

i = 0
for time_series in data_files:
    i = i + 1
    os.chdir(work_dir)
    # gcm
    make_sure_path_exists(time_series[2])
    os.chdir(time_series[2])
    if time_series[2] not in t95_vals:
        t95_vals[time_series[2]] = {}
    #scenario
    make_sure_path_exists(time_series[3])
    os.chdir(time_series[3])
    if time_series[3] not in t95_vals[time_series[2]]:
        t95_vals[time_series[2]][time_series[3]] = {}
    #station
    make_sure_path_exists(time_series[4])
    os.chdir(time_series[4])
    if time_series[4] not in t95_vals[time_series[2]][time_series[3]]:
        t95_vals[time_series[2]][time_series[3]][time_series[4]] = []
    filename = "replicate_" + time_series[5] + "recalc.txt"
    msg = "Calculating EHF for " + time_series[2] + " " + time_series[3] + " " + time_series[4] + " replicate " + time_series[5] + "; file " + str(i) + " of " + str(num_files)
    print(msg)
    if (time_series[2] == "csiro.mk36"):
        calcEHF_recalc(time_series[0], filename)    
       

os.chdir(cwd)

with open('t95vals.pickle', 'wb') as f:
    # Pickle the 'data' dictionary using the highest protocol available.
    pickle.dump(t95_vals, f, pickle.HIGHEST_PROTOCOL)
# raw = np.dtype([('id', np.str, 6), ('name', np.str, 48), ('x', np.float), ('y', np.float)])
# raw_data = np.loadtxt(r"/Volumes/home/QNAP RTRR Folder/Data/South Australia Goyder-CSIRO downscaled future climates/StationList.txt", dtype=raw)
    

# 
# 
# stat_id_dict_fil = r"a_file"
# 
# path = "/Volumes/home/QNAP RTRR Folder/Projects/Heatwave risk mapping/RAW DATA/Adelaide Airport 23034/CanESM2#/CanESM2 #Historic data/amlr27.canesm2.his.23034.002.txt"
