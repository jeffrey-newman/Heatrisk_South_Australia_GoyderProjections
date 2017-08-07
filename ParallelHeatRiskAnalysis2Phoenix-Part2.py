from mpi4py import MPI
import os
import errno
import pickle
import numpy
# os.environ['R_HOME'] = '/Users/a1091793/anaconda/lib/R'
# os.environ['R_HOME'] = '/Library/Frameworks/R.framework/Versions/3.3/Resources'
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


t95_vals = {} # A multilayered dict with t95 values for a replicate timeseries. Heierarchy [zone][gcm][scenario][station id]
t95_avg_vals = {} # A multilayered dict with t95 values averaged across each replicate timeseries. Heierarchy [zone][gcm][scenario][station id]
q85_vals = {} # A multilayered dict with q85 values averaged across each replicate timeseries. Heierarchy [zone][gcm][scenario][station id]
stat_vals = {} # A multilayered dict with summary statistics for each station. Heierarchy [zone][gcm][scenario][station id]

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

def getStationDefineDict(data_files):
    station_list = []

    for file_info in data_files:
        zone = file_info[1]
        if zone not in t95_avg_vals:
            t95_avg_vals[zone] = {}
            q85_vals[zone] = {}
            stat_vals[zone] = {}
            # calced_status[zone] = {}

        gcm = file_info[2]
        if gcm not in t95_avg_vals[zone]:
            t95_avg_vals[zone][gcm] = {}
            q85_vals[zone][gcm] = {}
            stat_vals[zone][gcm] = {}
            # calced_status[zone][gcm] = {}

        sc = file_info[3]
        if sc not in t95_avg_vals[zone][gcm]:
            t95_avg_vals[zone][gcm][sc] = {}
            q85_vals[zone][gcm][sc] = {}
            stat_vals[zone][gcm][sc] = {}
            # calced_status[zone][gcm][sc] = {}

        statn = file_info[7]
        if statn not in t95_avg_vals[zone][gcm][sc]:
            t95_avg_vals[zone][gcm][sc][statn] = 0
            q85_vals[zone][gcm][sc][statn] = 0
            stat_vals[zone][gcm][sc][statn] = []
            # calced_status[zone][gcm][sc][statn] = [{}]

        # rep = file_info[5]
        # if rep not in calced_status[zone][gcm][sc][statn][0]:
        #     calced_status[zone][gcm][sc][statn][0][rep] = []

        statn_info = [work_dir, zone, gcm, sc, statn]
        station_list.append(statn_info)
    return station_list



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
def distributeTask(workers, data, funct, task_name):
    num_sends = 0
    num_datums = len(data)
    sent_jobs_count = 0

    for idx in range(workers):
        to_id = idx + 1
        num_sends = send(num_sends, data, to_id)
        print("Sent " + task_name + " job " + num_sends + " of " + num_datums)

    num_recvs = 0
    for idx in range(workers):
        num_recvs, from_id = recv(num_recvs, data, funct)
        num_sends = send(num_sends, data, from_id)
        print("Sending " + task_name + " job " + num_sends + " of " + num_datums)

    while num_recvs < num_datums:
        num_recvs, from_id = recv(num_recvs, data, funct)
        num_sends = send(num_sends, data, from_id)
        print("Sending " + task_name + " job " + num_sends + " of " + num_datums)


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
    t95_pickle_file_name = "replicate_" + time_series[5] + "_t95.pickle"

    print(msg)
    t95 = calcDailyMeanTemp(time_series[0], filename, filename_t95, t95_pickle_file_name)
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
    t95_val = time_series[-1]  #13 in list.
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


# postprocess the intial EHF calculations. Store the t95 values for each timeseries in a dict
def  processCalcDailyMeanTemp(time_series):
    # time_series is a list with the following elements:
    #   0) path to file, (1) the climatic zone, (2) The GCM used to generate the data,
    #         (3) The future climate scenario. (4) The station id, (5) the replicate downscale number, (6) The file format,
    #         (7) Station name, (8) longitude, (9) lattitude, and
    #         (10) the t95 value
    checkt95Dict(time_series, t95_vals)
    t95_vals[time_series[1]][time_series[2]][time_series[3]][time_series[7]].append([time_series[5], time_series[-1]])
    with open('calc_dmt_done.pickle', 'a+b') as f:
        pickle.dump(time_series, f, pickle.HIGHEST_PROTOCOL)


# postprocess the recalculated EHF calculations (this is the calcs that require knowing the t95 value.
# Store the statsitics of each climate timeseries in a dict
def processCalcEHF(time_series):
    # time_series contains (0) path to file, (1) the climatic zone, (2) The GCM used to generate the data,
    #         (3) The future climate scenario. (4) The station id, (5) the replicate downscale number, (6) The file format,
    #         (7) Station name, (8) longitude, (9) lattitude,
    #         (10) the avg_t95 value for the respective statsion
    with open('calc_ehf_done.pickle', 'a+b') as f:
        pickle.dump(time_series, f, pickle.HIGHEST_PROTOCOL)
    return 1


def processCalcStatistics(time_series):
    with open('calc_stats_done.pickle', 'a+b') as f:
        pickle.dump(time_series, f, pickle.HIGHEST_PROTOCOL)
    return 1

def processMapData(timeslice_info):
    with open('map_data_done.pickle', 'a+b') as f:
        pickle.dump(timeslice_info, f, pickle.HIGHEST_PROTOCOL)
    return 1


def  processCalcT95(statn_info):
    # statn_info is a list with the following info: (0) work_dir, (1) zone, (2) gcm, (3) sc, (4) statn, (5) avg_t95 across replicates.
    t95_avg_vals[statn_info[1]][statn_info[2]][statn_info[3]][statn_info[4]] = statn_info[5]
    with open('calc_avg_t95_done.pickle', 'a+b') as f:
        pickle.dump(statn_info, f, pickle.HIGHEST_PROTOCOL)

def processCalcQ85(statn_info):
    # statn_info is a list with the following info: (0) work_dir, (1) zone, (2) gcm, (3) sc, (4) statn, (5) beta, (6) xi, (7) qgpd85.
    # xi and beta are parameters of the distribution
    # qgpd85 is the 85 percentile of the distribution.
    q85_vals[statn_info[1]][statn_info[2]][statn_info[3]][statn_info[4]] = statn_info[-1]
    with open('calc_q85_done.pickle', 'a+b') as f:
        pickle.dump(statn_info, f, pickle.HIGHEST_PROTOCOL)

def processAccumulateStats(statn_info):
    stat_vals[statn_info[1]][statn_info[2]][statn_info[3]][statn_info[4]].append(statn_info[-1])
    with open('calc_accum_stats_done.pickle', 'a+b') as f:
        pickle.dump(statn_info, f, pickle.HIGHEST_PROTOCOL)

# Now for the script which integrates the calculation.

#Phoenix
station_list = r"/fast/users/a1091793/Heatwave/StationList.txt"
rootdir = r"/fast/users/a1091793/Heatwave/Goyder_Climate_Futures_Sorted_Timeseries_SA/Sorted"
work_dir = r"/fast/users/a1091793/Heatwave/Processed"
#Testing
# station_list = r"/Volumes/Samsung_T3/heatwaveTestDir/StationList.txt"
# rootdir = r"/Volumes/Samsung_T3/heatwaveTestDir/StationList.txt"
# work_dir = r"/Volumes/Samsung_T3/heatwaveTestDir/Subset"


##################################################################################
#####     Get list of stations and find all climate data series files.
##################################################################################
if rank == 0:
    os.chdir(work_dir)
    # Read the list of stations and their coordinates from file
    # stations is a dict with station id as the key, and with a list of values in this order: (0) station name, (1) longitude, and (2) latitude
    stations = readStationList(station_list)
    # Recurse through directories making a list of data files, and link these to their name and location
    # data_files is a list with (0) path to file, (1) the climatic zone, (2) The GCM used to generate the data,
    #         (3) The future climate scenario. (4) The station id, (5) the replicate downscale number, (6) The file format,
    #         (7) Station name, (8) longitude, (9) lattitude
    data_files = listRawDataFiles(rootdir, stations)
    with open('stations.pickle', 'wb') as f1:
        # Pickle the 'data' dictionary using the highest protocol available.
        pickle.dump(stations, f1, pickle.HIGHEST_PROTOCOL)
    with open('data_files.pickle', 'wb') as f2:
        # Pickle the 'data' dictionary using the highest protocol available.
        pickle.dump(data_files, f2, pickle.HIGHEST_PROTOCOL)

# if rank == 0:
#     os.chdir(work_dir)
#     stations = pickle.load('stations.pickle')
#     data_files = pickle.load('data_files.pickle')

##################################################################################
#####     Make a list of all stations and the zones, gcm, scenarios.
##################################################################################
if rank == 0:
    os.chdir(work_dir)
    # Create a list of the stations with only some information as needed by a few of the tasks.
    # station_list is a list. Each element is the list with the following information : work_dir, zone, gcm, sc, statn
    station_list = getStationDefineDict(data_files)
    with open('station_list.pickle', 'wb') as f:
        pickle.dump(station_list, f, pickle.HIGHEST_PROTOCOL)

# if rank == 0:
#     os.chdir(work_dir)
#     station_dict = pickle.load('station_dict.pickle')

##################################################################################
#####     CALCULATING DMT
##################################################################################
# # Calculate daily mean temperature
if rank == 0:
    os.chdir(work_dir)
    distributeTask(workers, data_files, processCalcDailyMeanTemp, "Calculating daily mean temperature")
    with open('dmt.pickle', 'wb') as f:
        # Pickle the 'data' dictionary using the highest protocol available.
        pickle.dump(t95_vals, f, pickle.HIGHEST_PROTOCOL)
else:
    receiveTasks(calcDailyMeanTempjob)

# if rank == 0:
#     os.chdir(work_dir)
#     t95_vals = pickle.load('dmt.pickle')


##################################################################################
#####     CALCULATING t95 for each station
##################################################################################
# Calculate the t95 value for each station (zone, gcm, scenario treated separately)
if rank == 0:
    os.chdir(work_dir)
    distributeTask(workers, station_list, processCalcT95, "Calculating t95")
    # processCalcT95 places the t95 value across each replicate into the t95_avg_val dict.
    for time_series in data_files:
        time_series.append(t95_avg_vals[time_series[1]][time_series[2]][time_series[3]][time_series[7]])
        #data_files now also includes the t95 value for the station in the 13th element of each list.
    with open('t95_avg.pickle', 'wb') as f:
        # Pickle the 'data' dictionary using the highest protocol available.
        pickle.dump(t95_avg_vals, f, pickle.HIGHEST_PROTOCOL)
else:
    receiveTasks(calct95ForStation)

# if rank == 0:
#     os.chdir(work_dir)
#     t95_avg_vals = pickle.load('t95_avg_vals.pickle')

##################################################################################
#####     CALCULATING EHF for each data file
##################################################################################
# Calcualate the EHF.
if rank == 0:
    os.chdir(work_dir)
    distributeTask(workers, data_files, processCalcEHF, "Calculating EHF")
else:
    receiveTasks(calcEHFjob)

##################################################################################
#####     CALCULATING q85
##################################################################################
# Now we need to find the q85 point of the excess EHF data to calculate severity.
if rank == 0:
    os.chdir(work_dir)
    distributeTask(workers, station_list, processCalcQ85, "Calculating Q85")
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
    distributeTask(workers, data_files, processCalcStatistics, "Calculating Statistics")
else:
    receiveTasks(calcStatisticsJob)

#Now we calculated what the average statistics are across the replicates
if rank == 0:
    os.chdir(work_dir)
    distributeTask(workers, station_list, processAccumulateStats, "Calculating Avg Statistics across replicates")
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
    os.chdir(work_dir)
    with open('space_domain_directories.pickle', 'wb') as f:
        pickle.dump(space_domain_directories, f, pickle.HIGHEST_PROTOCOL)

if rank == 0:
    os.chdir(work_dir)
    distributeTask(workers, space_domain_directories, processMapData, "Interpolating and mapping")
else:
    receiveTasks(interpolateAndMap)


