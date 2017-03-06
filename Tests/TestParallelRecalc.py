from mpi4py import MPI
import os
import errno
import pickle
from collections import Counter

# Initializations and preliminaries of MPI
comm = MPI.COMM_WORLD   # get MPI communicator object
num_processes = comm.size        # total number of processes
workers = num_processes - 1
rank = comm.rank        # rank of this process
status = MPI.Status()   # get MPI status object

import imp
import ReadStationList
from ReadStationList import readStationList
imp.reload(ReadStationList); from ReadStationList import readStationList
import ListRawDataFiles
from ListRawDataFiles import listRawDataFiles
imp.reload(ListRawDataFiles); from ListRawDataFiles import listRawDataFiles
import EHFTimeseries
from CalcDailyMeanTempFromTimeseries import calcDailyMeanTemp
imp.reload(CalcDailyMeanTempFromTimeseries); from CalcDailyMeanTempFromTimeseries import calcDailyMeanTemp
import EHFRecalc2
from EHFRecalc2 import calcEHF_recalc2


t95_vals = {}
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
    if time_series[2] not in t95_vals:
        t95_vals[time_series[2]] = {}
    if time_series[3] not in t95_vals[time_series[2]]:
        t95_vals[time_series[2]][time_series[3]] = {}
    if time_series[4] not in t95_vals[time_series[2]][time_series[3]]:
        t95_vals[time_series[2]][time_series[3]][time_series[4]] = []


# Check that the dict storing the statistics for each climate timeseries has the necessary keys
def checkStatVals(time_series, stat_vals):
    if time_series[2] not in stat_vals:
        stat_vals[time_series[2]] = {}
    # scenario
    if time_series[3] not in stat_vals[time_series[2]]:
        stat_vals[time_series[2]][time_series[3]] = {}
    # station
    if time_series[4] not in stat_vals[time_series[2]][time_series[3]]:
        stat_vals[time_series[2]][time_series[3]][time_series[4]] = []


# generic send function
def send(num_sends, data, to_id):
    if num_sends < len(data):
        datum_send = data[num_sends]
        # print('sending: ', datum_send)
        comm.send(datum_send, dest=to_id, tag=num_sends)
        num_sends = num_sends + 1
    return num_sends


#generic recv function
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
def  processCalcEHF(time_series):
    checkt95Dict(time_series, t95_vals)
    t95_vals[time_series[2]][time_series[3]][time_series[4]].append([time_series[5], time_series[9]])


# As there are replicates, aggregate statistics across replicates.
def combine_stat_dicts(a, b):
    combined = {}
    for year, a_stats in a.items():
        new_stats = a_stats.copy()
        if year in b:
            b_stats = b[year]
            for index, stat in enumerate(new_stats):
                new_stats[index] += b_stats[index]
        combined[year] = new_stats
    return combined


# postprocess the recalculated EHF calculations (this is the calcs that require knowing the t95 value.
# Store the statsitics of each climate timeseries in a dict
def processReCalcEHF(time_series):
    checkStatVals(time_series, stat_vals)
    print(time_series)
    this_stats = time_series[-1]
    if not stat_vals[time_series[2]][time_series[3]][time_series[4]]:
        stat_vals[time_series[2]][time_series[3]][time_series[4]] = this_stats;
    else:
        stat_vals[time_series[2]][time_series[3]][time_series[4]] = combine_stat_dicts(stat_vals[time_series[2]][time_series[3]][time_series[4]], this_stats)


# Generic receive fuinction to receive a task. Takes a function that will be called on the received data
def receiveTasks(job_funct):
    do_abort = False
    source_id = -1
    while not do_abort:
        datum_recv = comm.recv(status=status)
        # print('receiving: ', datum_recv)
        tag = status.Get_tag()
        if tag < 0:
            do_abort = True

        os.chdir(work_dir)
        datum_send = job_funct(datum_recv)
        source_id = status.Get_source()
        comm.send(datum_send, dest=source_id, tag=tag)
    return datum_send


# Check working directory for job exists, and change to this directory.
def checkAndMove2Directory(time_series):
    os.chdir(work_dir)

    # gcm
    make_sure_path_exists(time_series[2])
    os.chdir(time_series[2])

    # scenario
    make_sure_path_exists(time_series[3])
    os.chdir(time_series[3])

    # station
    make_sure_path_exists(time_series[4])
    os.chdir(time_series[4])


# Calculate the EHF for a climate timeseries
def calcEHFjob(time_series):
    checkAndMove2Directory(time_series)
    filename = "replicate_" + time_series[5] + ".dmt"
    filename_t95 = "replicate_" + time_series[5] + ".t95"
    msg = "Calculating EHF for " + time_series[2] + " " + time_series[3] + " " + time_series[4] + " replicate " + time_series[5]
    print(msg)
    t95 = calcEHF(time_series[0], filename, filename_t95)
    # print("t95 is ", t95)
    time_series.append(t95)
    return time_series


# Recalculate variables requireing the t95 value for a climate timeseries
def reCalcEHFjob(time_series):
    checkAndMove2Directory(time_series)
    filename = "replicate_" + time_series[5] + "recalc.txt"
    filename_xl = "replicate_" + time_series[5] + ".xlsx"
    msg = "Calculating EHF recalc for " + time_series[2] + " " + time_series[3] + " " + time_series[4] + " replicate " + time_series[5]
    print(msg)
    t95_val = time_series[-1]  #10?
    print("filename is: ", filename_xl, "t95 is ", str(t95_val))
    this_stats = calcEHF_recalc2(time_series[0], filename, time_series[5], t95_val, filename_xl)
    time_series.append(this_stats)
    return time_series



# Now for the script which integrates the calculation.
station_list = r"/Volumes/Samsung_T3/heatwave/StationList.txt"
rootdir = r"/Volumes/Samsung_T3/heatwave/Subset"
work_dir = r"/Volumes/Samsung_T3/heatwave/Processed"

os.chdir(work_dir)

# The intial EHF calcualtions
if rank == 0:
    stations = readStationList(station_list)
    data_files = listRawDataFiles(rootdir, stations)
    ## For each timeseries, calculate EHF
    # distributeTask(workers, data_files, processCalcEHF)
# else:
#     receiveTasks(calcEHFjob)

# Aggregare and save the t95 values to file and pickle. Aggregate as we have repeats timeseries for each gcm, scenario and station.
if rank == 0:
    os.chdir(work_dir)

    with open('t95vals_BP1.pickle', 'rb') as f:
       # Pickle the 'data' dictionary using the highest protocol available.
       t95_vals = pickle.load(f)

    column_hdrs = "GCM\tScenario\tStationNum\tReplicate\tT95\n"

    t95_avg_vals = {}
    for gcm, gcm_dicts in t95_vals.items():

        if gcm not in t95_avg_vals:
            t95_avg_vals[gcm] = {}

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
                avg_t95 = 0
                for t95list in t95_lists:
                    avg_t95 += t95list[1]
                avg_t95 = avg_t95 / len(t95_lists)
                # avg_t95 = reduce(lambda x, y: x + y[1], t95_lists, 0) / len(t95_lists)
                #            avg_t95 = sum(statn_lists)/len(statn_lists)
                t95vals_file = "t95_vals.txt"
                with open(t95vals_file, 'w') as f_t95:
                    f_t95.write(column_hdrs)
                    for t95_vals in t95_lists:
                        t95_str = str(gcm) + "\t" + str(sc) + "\t" + str(statn) + "\t" + str(t95_vals[0]) + "\t" + str(t95_vals[1]) + "\n"
                        f_t95.write(t95_str)
                t95avg_file = "t95_avg.txt"
                with open(t95avg_file, 'w') as f_avg:
                    f_avg.write(str(avg_t95))
                t95_avg_vals[gcm][sc][statn] = avg_t95

# Now do the EHF calculations based on the t95 value.
if rank == 0:
    for time_series in data_files:
        t95_val = t95_avg_vals[time_series[2]][time_series[3]][time_series[4]]
        time_series.append(t95_val)
    distributeTask(workers, data_files, processReCalcEHF)
else:
    receiveTasks(reCalcEHFjob)

if rank == 0:
    os.chdir(work_dir)
    files_by_year = {}
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
                        f_ystats.write(
                            "year\tsum_days_summer\tsum_days_autumn\tsum_days_winter\tsum_days_spring\tsum_ehf_summer\tsum_ehf_autumn\tsum_ehf_winter\tsum_ehf_spring\tmax_ehf_summer\tmax_ehf_autumn\tmax_ehf_winter\tmax_ehf_spring\ttot_days_in_summer\ttot_days_in_autumn\ttot_days_in_winter\ttot_days_in_spring\tprop_days_in_summer\tprop_days_in_autumn\tprop_days_in_winter\tprop_days_in_spring\tavg_ehf_summer\tavg_ehf_autumn\tavg_ehf_winter\tavg_ehf_spring\tsum_days\tsum_ehf\tmax_ehf\ttot_days\tprop_days\tavg_ehf\t")
                        for year, stat in stats.items:
                            if year not in files_by_year[gcm][sc]:
                                file_name = str(gcm) + "_" + str(sc) + "_" + str(year) + ".txt"
                                files_by_year[gcm][sc][year] = open(file_name, 'w')
                                files_by_year[gcm][sc][year].write(
                                    "station_id\tlat\tlong\tsum_days_summer\tsum_days_autumn\tsum_days_winter\tsum_days_spring\tsum_ehf_summer\tsum_ehf_autumn\tsum_ehf_winter\tsum_ehf_spring\tmax_ehf_summer\tmax_ehf_autumn\tmax_ehf_winter\tmax_ehf_spring\ttot_days_in_summer\ttot_days_in_autumn\ttot_days_in_winter\ttot_days_in_spring\tprop_days_in_summer\tprop_days_in_autumn\tprop_days_in_winter\tprop_days_in_spring\tavg_ehf_summer\tavg_ehf_autumn\tavg_ehf_winter\tavg_ehf_spring\tsum_days\tsum_ehf\tmax_ehf\ttot_days\tprop_days\tavg_ehf\t")
                            f_ystats.write(str(year) + "\t")
                            files_by_year[gcm][sc][year].write(
                                str(statn) + "\t" + str(stations[statn][1]) + "\t" + str(stations[statn][2]))
                            for i in range(0, 30):
                                f_ystats.write(str(stat[i]) + "\t")
                                files_by_year[gcm][sc][year].write(str(stat[i]) + "\t")
                            f_ystats.write("\n")
    with open('t95vals_BP2.pickle', 'wb') as f1:
       # Pickle the 'data' dictionary using the highest protocol available.
       pickle.dump(t95_vals, f1, pickle.HIGHEST_PROTOCOL)
    if __name__ == '__main__':
        with open('stat_vals.pickle', 'wb') as f2:
            # pickle the statistics using the highest protocol available.
            pickle.dump(stat_vals, f2, pickle.HIGHEST_PROTOCOL)

# Now we fit the data to the generalised Pareto distribution using R. We can also parallelise this as each python can run it's own R, I think.




    # Now we do the spatial interpolation in R



# Now we make the shape files.
