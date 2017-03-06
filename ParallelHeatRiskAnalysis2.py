from mpi4py import MPI
import os
import errno
import pickle
import numpy
# os.environ['R_HOME'] = '/Users/a1091793/anaconda/lib/R'
os.environ['R_HOME'] = '/Library/Frameworks/R.framework/Versions/3.3/Resources'
import rpy2.robjects as robjects
import rpy2.robjects.packages as rpackages

# from collections import Counter

# Initializations and preliminaries of MPI
comm = MPI.COMM_WORLD   # get MPI communicator object
num_processes = comm.size        # total number of processes
workers = num_processes - 1
rank = comm.rank        # rank of this process
status = MPI.Status()   # get MPI status object

# import imp
# import ReadStationList
from ReadStationList import readStationList
# imp.reload(ReadStationList); from ReadStationList import readStationList
# import ListRawDataFiles
from ListRawDataFiles import listRawDataFiles
# imp.reload(ListRawDataFiles); from ListRawDataFiles import listRawDataFiles
# import EHFTimeseries
from CalcDailyMeanTempFromTimeseries import calcDailyMeanTemp
# imp.reload(CalcDailyMeanTempFromTimeseries); from CalcDailyMeanTempFromTimeseries import calcDailyMeanTemp
# import CalcEHF
from CalcEHF import calcEHF
# import Calct95ForStation
from Calct95ForStation import calct95ForStation
# imp.reload(Calct95ForStation); from Calct95ForStation import calct95ForStation
from calcGPDAndQ85InR import CalcQ85FromGPDOfEHF
from AggregateStats import aggregateStats
from CreateShapeFile import print2shape
from CalcStatistics import calcStatistics
from InterpolateMap import interpolateAndMap

t95_vals = {}
t95_avg_vals = {}
q85_vals = {}
stat_vals = {}



def make_sure_path_exists(path):
    """
    Checks whether a file path exists on the file system
    :param path: Path to ensure whether it exists on the file system
    """
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
        else:
            pass


# Check that the dict storing the 95 percentile EHF has the necessary keys
def checkt95Dict(time_series, t95_vals):
    if time_series[1] not in t95_vals:
        t95_vals[time_series[1]] = {}
    if time_series[2] not in t95_vals[time_series[1]]:
        t95_vals[time_series[1]][time_series[2]] = {}
    if time_series[3] not in t95_vals[time_series[1]][time_series[2]]:
        t95_vals[time_series[1]][time_series[2]][time_series[3]] = {}
    if time_series[7] not in t95_vals[time_series[1]][time_series[2]][time_series[3]]:
        t95_vals[time_series[1]][time_series[2]][time_series[3]][time_series[7]] = []


# Check that the dict storing the statistics for each climate timeseries has the necessary keys
def checkStatVals(time_series, stat_vals):
    if time_series[2] not in stat_vals:
        stat_vals[time_series[2]] = {}
    # scenario
    if time_series[3] not in stat_vals[time_series[2]]:
        stat_vals[time_series[2]][time_series[3]] = {}
    # station
    if time_series[7] not in stat_vals[time_series[2]][time_series[3]]:
        stat_vals[time_series[2]][time_series[3]][time_series[7]] = []


# generic send function for master
def send(num_sends, data, to_id):
    if num_sends < len(data):
        datum_send = data[num_sends]
        # print('sending: ', datum_send)
        comm.send(datum_send, dest=to_id, tag=num_sends)
        num_sends = num_sends + 1
    else:
        comm.send(data[0], dest=to_id, tag=999999)
    return num_sends


#generic recv function for master
def recv(num_recvs, data, funct):
    source_id = -1
    if num_recvs < len(data):
        datum_recv = comm.recv(status=status)
        num_recvs = num_recvs + 1
        source_id = status.Get_source()
        # print('receiving: ', datum_recv)
        funct(datum_recv)
    return num_recvs, source_id


# distritute tasks to workers
# data is a collection - each item is sent to a worker
# workers is the size of the worker pool
# funct is applied to the received results
def distributeTask(workers, data, funct):
    num_sends = 0
    num_datums = len(data)

    for idx in range(workers):
        to_id = idx + 1
        num_sends = send(num_sends, data, to_id)

    num_recvs = 0
    for idx in range(workers):
        num_recvs, from_id = recv(num_recvs, data, funct)
        num_sends = send(num_sends, data, from_id)

    while num_recvs < num_datums:
        num_recvs, from_id = recv(num_recvs, data, funct)
        num_sends = send(num_sends, data, from_id)


# postprocess the intial EHF calculations. Store the t95 values for each timeseries in a dict
def  processCalcDailyMeanTemp(time_series):
    checkt95Dict(time_series, t95_vals)
    t95_vals[time_series[1]][time_series[2]][time_series[3]][time_series[7]].append([time_series[5], time_series[-1]])


# postprocess the recalculated EHF calculations (this is the calcs that require knowing the t95 value.
# Store the statsitics of each climate timeseries in a dict
def processCalcEHF(time_series):
    return 1


def processCalcStatistics(time_series):
    return 1

def processMapData(timeslice_info):
    return 1

# Generic receive fuinction to receive a task at a worker. Takes a function that will be called on the received data
def receiveTasks(job_funct):
    do_abort = False
    source_id = -1
    while not do_abort:
        datum_recv = comm.recv(status=status)
        # print('receiving: ', datum_recv)
        tag = status.Get_tag()
        if tag >= 999999:
            do_abort = True
        else:
            os.chdir(work_dir)
            datum_send = job_funct(datum_recv)
            source_id = status.Get_source()
            comm.send(datum_send, dest=source_id, tag=tag)
    return datum_send


# Check working directory for job exists, and change to this directory.
def checkAndMove2Directory(time_series):
    os.chdir(work_dir)

    # zone
    make_sure_path_exists(time_series[1])
    os.chdir(time_series[1])

    # gcm
    make_sure_path_exists(time_series[2])
    os.chdir(time_series[2])

    # scenario
    make_sure_path_exists(time_series[3])
    os.chdir(time_series[3])

    # station
    make_sure_path_exists(time_series[7])
    os.chdir(time_series[7])


# Calculate the daily mean temperature for a climate timeseries
def calcDailyMeanTempjob(time_series):
    # print(time_series)
    msg = "Calculating DMT for " + time_series[1] + " " + time_series[2] + " " + time_series[3] + " " + time_series[
        7] + " replicate " + time_series[5]
    checkAndMove2Directory(time_series)
    filename = "replicate_" + time_series[5] + ".dmt"
    filename_t95 = "replicate_" + time_series[5] + ".t95"

    print(msg)
    t95 = calcDailyMeanTemp(time_series[0], filename, filename_t95)
    # print("t95 is ", t95)
    time_series.append(t95)
    return time_series


# Recalculate variables requireing the t95 value for a climate timeseries
def calcEHFjob(time_series):
    checkAndMove2Directory(time_series)
    filename_calcs = "replicate_" + time_series[5] + "ehf_calcs.pickle"
    filename_ehfs = "replicate_" + time_series[5] + "ehf_values.ehf"
    msg = "Calculating EHF for " + time_series[1] + " " + time_series[2] + " " + time_series[3] + " " + time_series[7] + " replicate " + time_series[5]
    print(msg)
    t95_val = time_series[-1]  #10?
    calcEHF(time_series[0], filename_calcs, t95_val, filename_ehfs)
    return time_series

def calcStatisticsJob(time_series):
    checkAndMove2Directory(time_series)
    ehf_pickle = "replicate_" + time_series[5] + "ehf_calcs.pickle"
    filename_yearly_stats = "replicate_" + time_series[5] + "yearly_statistics.txt"
    filename_accumulative_stats = "replicate_" + time_series[5] + "accumulative_statistics.txt"
    filename_pickle = "replicate_" + time_series[5] + ".stats"
    msg = "Calculating Statistics for " + time_series[1] + " " + time_series[2] + " " + time_series[3] + " " + time_series[
        7] + " replicate " + time_series[5]
    print(msg)
    q85_val = time_series[-1]  #
    calcStatistics(ehf_pickle, q85_val, filename_yearly_stats, filename_accumulative_stats, filename_pickle)
    return time_series

def getStationDict(t95_vals):
    station_dict = []
    for zone, zone_dicts in t95_vals.items():
        if zone not in t95_avg_vals:
            t95_avg_vals[zone] = {}
            q85_vals[zone] = {}
            stat_vals[zone] = {}

        for gcm, gcm_dicts in zone_dicts.items():

            if gcm not in t95_avg_vals[zone]:
                t95_avg_vals[zone][gcm] = {}
                q85_vals[zone][gcm] = {}
                stat_vals[zone][gcm] = {}

            for sc, sc_dicts in gcm_dicts.items():

                if sc not in t95_avg_vals[zone][gcm]:
                    t95_avg_vals[zone][gcm][sc] = {}
                    q85_vals[zone][gcm][sc] = {}
                    stat_vals[zone][gcm][sc] = {}

                for statn, t95_lists in sc_dicts.items():
                    if statn not in t95_avg_vals[zone][gcm][sc]:
                        t95_avg_vals[zone][gcm][sc][statn] = 0
                        q85_vals[zone][gcm][sc][statn] = 0
                        stat_vals[zone][gcm][sc][statn] = []

                    statn_info = [work_dir, zone, gcm, sc, statn]
                    station_dict.append(statn_info)
    return station_dict


def  processCalcT95(statn_info):
    t95_avg_vals[statn_info[1]][statn_info[2]][statn_info[3]][statn_info[4]] = statn_info[5]

def processCalcQ85(statn_info):
    q85_vals[statn_info[1]][statn_info[2]][statn_info[3]][statn_info[4]] = statn_info[-1]

def processAccumulateStats(statn_info):
    stat_vals[statn_info[1]][statn_info[2]][statn_info[3]][statn_info[4]].append(statn_info[-1])

# Now for the script which integrates the calculation.
station_list = r"/Volumes/Samsung_T3/heatwave/StationList.txt"
rootdir = r"/Volumes/Samsung_T3/heatwave/Subset"
work_dir = r"/Volumes/Samsung_T3/heatwave/Processed"


# Calculate daily mean temperature
if rank == 0:
    os.chdir(work_dir)
    stations = readStationList(station_list)
    data_files = listRawDataFiles(rootdir, stations)
    ## For each timeseries, calculate EHF
    distributeTask(workers, data_files, processCalcDailyMeanTemp)
    with open('dmt.pickle', 'wb') as f:
        # Pickle the 'data' dictionary using the highest protocol available.
        pickle.dump(t95_vals, f, pickle.HIGHEST_PROTOCOL)
else:
    receiveTasks(calcDailyMeanTempjob)

# Calculate the t95 value for each station (zone, gcm, scenario treated separately)
if rank == 0:
    os.chdir(work_dir)
    station_dict = getStationDict(t95_vals)
    distributeTask(workers, station_dict, processCalcT95)
    for time_series in data_files:
        time_series.append(t95_avg_vals[time_series[1]][time_series[2]][time_series[3]][time_series[7]])
    with open('t95.pickle', 'wb') as f:
        # Pickle the 'data' dictionary using the highest protocol available.
        pickle.dump(t95_avg_vals, f, pickle.HIGHEST_PROTOCOL)
else:
    receiveTasks(calct95ForStation)

# Calcualate the EHF.
if rank == 0:
    os.chdir(work_dir)
    distributeTask(workers, data_files, processCalcEHF)
else:
    receiveTasks(calcEHFjob)

# Now we need to find the q85 point of the excess EHF data to calculate severity.
if rank == 0:
    os.chdir(work_dir)
    distributeTask(workers, station_dict, processCalcQ85)
    for time_series in data_files:
        time_series.append(q85_vals[time_series[1]][time_series[2]][time_series[3]][time_series[7]])
    with open('q85.pickle', 'wb') as f:
        # Pickle the 'data' dictionary using the highest protocol available.
        pickle.dump(q85_vals, f, pickle.HIGHEST_PROTOCOL)
else:
    receiveTasks(CalcQ85FromGPDOfEHF)

# Now we calculate statistics for each time_series.
if rank == 0:
    os.chdir(work_dir)
    distributeTask(workers, data_files, processCalcStatistics)
else:
    receiveTasks(calcStatisticsJob)

#Now we calculated what the average statistics are across the replicates
if rank == 0:
    os.chdir(work_dir)
    distributeTask(workers, station_dict, processAccumulateStats)
    for time_series in data_files:
        time_series.append(stat_vals[time_series[1]][time_series[2]][time_series[3]][time_series[7]][0])
        if not (stat_vals[time_series[1]][time_series[2]][time_series[3]][time_series[7]][-1] == True):
            stat_vals[time_series[1]][time_series[2]][time_series[3]][time_series[7]].append(
                ([time_series[4]], [time_series[8]], [time_series[9]]))
            stat_vals[time_series[1]][time_series[2]][time_series[3]][time_series[7]].append(True)

    with open('stats.pickle', 'wb') as f:
        # Pickle the 'data' dictionary using the highest protocol available.
        pickle.dump(stat_vals, f, pickle.HIGHEST_PROTOCOL)
else:
    receiveTasks(aggregateStats)

# Now we map these statistics (conversion of time oriented dataset to spatial oriented)

space_oriented_root_dir = work_dir + r"/spaceDomain"
make_sure_path_exists(space_oriented_root_dir)

if rank == 0:
    os.chdir(work_dir)
    files_by_year = {}
    space_domain_directories = []
    for zone, zone_dicts in stat_vals.items():
    #     if zone not in files_by_year:
    #         files_by_year[zone] = {}
        for gcm, gcm_dicts in zone_dicts.items():
            if gcm not in files_by_year:
                files_by_year[gcm] = {}
            # if gcm not in space_domain_directories:
                # space_domain_directories[gcm] = {}
            for sc, sc_dicts in gcm_dicts.items():
                if sc not in files_by_year[gcm]:
                    files_by_year[gcm][sc] = {}
                # if sc not in space_domain_directories:
                    # space_domain_directories[gcm][sc] = {}
                for statn, stats in sc_dicts.items():

                    dir = space_oriented_root_dir
                    dir = dir + r"/" + gcm
                    make_sure_path_exists(dir)
                    dir = dir + r"/" + sc
                    make_sure_path_exists(dir)


                    for year in stats[0]:
                        if year not in files_by_year[gcm][sc]:
                            file_name = dir + r"/" + str(gcm) + "_" + str(sc) + "_" + str(year) + ".txt"
                            files_by_year[gcm][sc][year] = open(file_name, 'w')
                            files_by_year[gcm][sc][year].write(
                                "station_id\tzone\tlat\tlong\tsum_days_summer\tsum_days_autumn\tsum_days_winter\tsum_days_spring\tsum_ehf_summer\tsum_ehf_autumn\tsum_ehf_winter\tsum_ehf_spring\tmax_ehf_summer\tmax_ehf_autumn\tmax_ehf_winter\tmax_ehf_spring\ttot_days_in_summer\ttot_days_in_autumn\ttot_days_in_winter\ttot_days_in_spring\tprop_days_in_summer\tprop_days_in_autumn\tprop_days_in_winter\tprop_days_in_spring\tavg_ehf_summer\tavg_ehf_autumn\tavg_ehf_winter\tavg_ehf_spring\tsum_days\tsum_ehf\tmax_ehf\ttot_days\tprop_days\tavg_ehf\tnum_low_days\tnum_mod_days\tnum_high_days\n")
                        files_by_year[gcm][sc][year].write(
                            str(stats[-2][0]) + "\t" + str(zone) + "\t" + str(stats[-2][1]) + "\t" + str(stats[-2][2]))
                        stat = stats[0][year]
                        for i in range(0, 34):
                            files_by_year[gcm][sc][year].write(str(stat[i]) + "\t")
                        files_by_year[gcm][sc][year].write("\n")

                        shp_file_name = dir + r"/" + str(gcm) + "_" + str(sc) + "_" + str(year) + ".shp"
                        print2shape(stat_vals, year, gcm, sc, shp_file_name)
                        # if year not in space_domain_directories[gcm][sc]:
                        shape_name = str(gcm) + "_" + str(sc) + "_" + str(year)
                        space_domain_directories.append((gcm, sc, year, dir, shape_name))

if rank == 0:
    os.chdir(work_dir)
    with open('space_domain_directories.pickle', 'wb') as f:
        pickle.dump(space_domain_directories, f, pickle.HIGHEST_PROTOCOL)

    distributeTask(workers, space_domain_directories, processMapData)
else:
    receiveTasks(interpolateAndMap)


