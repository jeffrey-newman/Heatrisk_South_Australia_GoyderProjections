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

from collections import Counter

import EHFTimeseries
from EHFTimeseries import calcEHF
import EHFRecalc
from EHFRecalc import calcEHF_recalc
import EHFRecalc
from EHFRecalc import calcEHF_recalc
import ReadStationList
from ReadStationList import readStationList
import ListRawDataFiles
from ListRawDataFiles import listRawDataFiles

import imp
from imp import reload
imp.reload(EHFTimeseries); from EHFTimeseries import calcEHF
imp.reload(EHFRecalc); from EHFRecalc import calcEHF_recalc
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

## For each timeseries, calculate EHF
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

os.chdir(work_dir)

#with open('t95vals.pickle', 'wb') as f:
#    # Pickle the 'data' dictionary using the highest protocol available.
#    pickle.dump(t95_vals, f, pickle.HIGHEST_PROTOCOL)

column_hdrs = "GCM\tScenario\tStationNum\tReplicate\tT95\n"

t95_avg_vals = {}
for gcm, gcm_dicts in t95_vals.items():
    
    if gcm not in t95_avg_vals:
        t95_avg_vals[gcm] = {}
    if (gcm == "csiro.mk36"):
        for sc, sc_dicts in gcm_dicts.items(): 
            
            if sc not in t95_avg_vals[gcm]:
                t95_avg_vals[gcm][sc] = {}
            for statn, t95_lists in sc_dicts.items():
                os.chdir(work_dir)
                os.chdir(gcm)
                os.chdir(sc)
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

os.chdir(work_dir)
#with open('t95avg.pickle', 'wb') as f2:
#    # Pickle the 'data' dictionary using the highest protocol available.
#    pickle.dump(t95_avg_vals, f2, pickle.HIGHEST_PROTOCOL)              
            

i = 0
stat_vals = {}
for time_series in data_files:
    i = i + 1
    os.chdir(work_dir)
    # gcm
    make_sure_path_exists(time_series[2])
    os.chdir(time_series[2])
    if time_series[2] not in stat_vals:
        stat_vals[time_series[2]] = {}
    #scenario
    make_sure_path_exists(time_series[3])
    os.chdir(time_series[3])
    if time_series[3] not in stat_vals[time_series[2]]:
        stat_vals[time_series[2]][time_series[3]] = {}
    #station
    make_sure_path_exists(time_series[4])
    os.chdir(time_series[4])
    if time_series[4] not in stat_vals[time_series[2]][time_series[3]]:
        stat_vals[time_series[2]][time_series[3]][time_series[4]] = []
    filename = "replicate_" + time_series[5] + "recalc.txt"
    msg = "Calculating EHF for " + time_series[2] + " " + time_series[3] + " " + time_series[4] + " replicate " + time_series[5] + "; file " + str(i) + " of " + str(num_files)
    print(msg)
    if (time_series[2] == "csiro.mk36"):
        this_stats = Counter(calcEHF_recalc(time_series[0], filename, time_series[5], t95_avg_vals[time_series[2]][time_series[3]][time_series[4]]))
        if not stat_vals[time_series[2]][time_series[3]][time_series[4]]:
            stat_vals[time_series[2]][time_series[3]][time_series[4]] = this_stats;
        else:
            stat_vals[time_series[2]][time_series[3]][time_series[4]] = stat_vals[time_series[2]][time_series[3]][time_series[4]] + this_stats
       
os.chdir(work_dir)
files_by_year= {}
for gcm, gcm_dicts in stat_vals.items():
    if gcm not in files_by_year:
        files_by_year[gcm] = {}
    if (gcm == "csiro.mk36"):
        for sc, sc_dicts in gcm_dicts.items(): 
            if sc not in files_by_year[gcm]:
                files_by_year[gcm][sc] = {}
            for statn, stats in sc_dicts.items():
                
                os.chdir(work_dir)
                os.chdir(gcm)
                os.chdir(sc)
                os.chdir(statn)
                stats[:] = [x / 100 for x in stats]
                with open('yearly_avg_stats.txt', 'w') as f_ystats:
                    f_ystats.write("year\tsum_days_summer\tsum_days_autumn\tsum_days_winter\tsum_days_spring\tsum_ehf_summer\tsum_ehf_autumn\tsum_ehf_winter\tsum_ehf_spring\tmax_ehf_summer\tmax_ehf_autumn\tmax_ehf_winter\tmax_ehf_spring\ttot_days_in_summer\ttot_days_in_autumn\ttot_days_in_winter\ttot_days_in_spring\tprop_days_in_summer\tprop_days_in_autumn\tprop_days_in_winter\tprop_days_in_spring\tavg_ehf_summer\tavg_ehf_autumn\tavg_ehf_winter\tavg_ehf_spring\tsum_days\tsum_ehf\tmax_ehf\ttot_days\tprop_days\tavg_ehf\t")
                    for year, stat in stats.items:
                        if year not in files_by_year[gcm][sc]:
                            file_name = str(gcm) + "_" + str(sc) + "_" + str(year) + ".txt"
                            files_by_year[gcm][sc][year] = open(file_name, 'w')
                            files_by_year[gcm][sc][year].write("station_id\tlat\tlong\tsum_days_summer\tsum_days_autumn\tsum_days_winter\tsum_days_spring\tsum_ehf_summer\tsum_ehf_autumn\tsum_ehf_winter\tsum_ehf_spring\tmax_ehf_summer\tmax_ehf_autumn\tmax_ehf_winter\tmax_ehf_spring\ttot_days_in_summer\ttot_days_in_autumn\ttot_days_in_winter\ttot_days_in_spring\tprop_days_in_summer\tprop_days_in_autumn\tprop_days_in_winter\tprop_days_in_spring\tavg_ehf_summer\tavg_ehf_autumn\tavg_ehf_winter\tavg_ehf_spring\tsum_days\tsum_ehf\tmax_ehf\ttot_days\tprop_days\tavg_ehf\t")
                        f_ystats.write(str(year)+"\t")
                        files_by_year[gcm][sc][year].write(str(statn) +"\t"+str(stations[statn][1]) +"\t"+str(stations[statn][2]) )
                        for i in range(0, 30): 
                            f_ystats.write(str(stat[i]) + "\t")
                            files_by_year[gcm][sc][year].write(str(stat[i]) + "\t")
                        f_ystats.write("\n")
                                
                
                
                

# print data


        
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
