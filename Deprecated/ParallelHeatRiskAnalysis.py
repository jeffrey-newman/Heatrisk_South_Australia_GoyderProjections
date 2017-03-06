from mpi4py import MPI
import os
import errno
import pickle
from collections import Counter

# Initializations and preliminaries of MPI
comm = MPI.COMM_WORLD   # get MPI communicator object
size = comm.size        # total number of processes
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
from EHFTimeseries import calcEHF
imp.reload(EHFTimeseries); from EHFTimeseries import calcEHF
import EHFRecalc
from EHFRecalc import calcEHF_recalc




def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
        else:
            pass

def checkt95Dict(time_series, t95_vals):
    if time_series[2] not in t95_vals:
        t95_vals[time_series[2]] = {}
    if time_series[3] not in t95_vals[time_series[2]]:
        t95_vals[time_series[2]][time_series[3]] = {}
    if time_series[4] not in t95_vals[time_series[2]][time_series[3]]:
        t95_vals[time_series[2]][time_series[3]][time_series[4]] = []

def checkStatVals(time_series, stat_vals):
    if time_series[2] not in stat_vals:
        stat_vals[time_series[2]] = {}
    # scenario
    if time_series[3] not in stat_vals[time_series[2]]:
        stat_vals[time_series[2]][time_series[3]] = {}
    # station
    if time_series[4] not in stat_vals[time_series[2]][time_series[3]]:
        stat_vals[time_series[2]][time_series[3]][time_series[4]] = []

def send(num_sends, data):
    if num_sends < data.size():
        datum_send = data[num_sends]
        comm.send(datum_send, dest=source_id, tag=num_sends)
        num_sends = num_sends + 1
    return num_sends

def recv(num_recvs, data, funct):
    source_id = -1
    if num_recvs < data.size():
        datum_recv = comm.recv(status=status)
        num_recvs = num_recvs + 1
        source_id = status.Get_source()
        print(datum_recv)
        funct(datum_recv)
    return num_recvs

def distributeTask(workers, data, funct):
    num_sends = 0
    num_datums = data.size()
    for idx in range(workers):
        num_sends = send(num_sends, data)

    num_recvs = 0
    for idx in range(workers):
        num_recvs = recv(num_recvs, data, funct)
        num_sends = send(num_sends, data)

    while num_recvs < num_datums:
        num_recvs = recv(num_recvs, data, funct)
        num_sends = send(num_sends, data)

def processCalcEHF(time_series):
    checkt95Dict(time_series, t95_vals)
    t95_vals[time_series[2]][time_series[3]][time_series[4]].append([time_series[5], time_series[9]])

def processReCalcEHF():


station_list = r"/Volumes/home/Jeffs Resilio Sync Folder/Data/South Australia Goyder-CSIRO downscaled future climates/Adelaide_Mt_Lofty_Ranges/StationList.txt"
rootdir = r"/Volumes/home/Jeffs Resilio Sync Folder/Data/South Australia Goyder-CSIRO downscaled future climates/Adelaide_Mt_Lofty_Ranges/Adelaide_Mt_Lofty_Ranges"
work_dir = r"/Volumes/home/Jeffs Resilio Sync Folder/Data/South Australia Goyder-CSIRO downscaled future climates/Adelaide_Mt_Lofty_Ranges/Processed"

os.chdir(work_dir)

if rank == 0:
    stations = readStationList(station_list)
    data_files = listRawDataFiles(rootdir, stations)
    ## For each timeseries, calculate EHF
    num_files = len(data_files)
    t95_vals = {}
    send_clim_sc_id = 0
    distributeTask(size, data_files, processCalcEHF)

    for idx in range(size):
        if send_clim_sc_id < num_files:
            time_series = data_files[send_clim_sc_id]
            comm.send(time_series, dest = idx, tag = send_clim_sc_id)
            send_clim_sc_id = send_clim_sc_id + 1

    received_clim_sc_id = 0
    for idx in range(size):
        source_id = -1
        if received_clim_sc_id < num_files:
            time_series = comm.recv(status=status)
            received_clim_sc_id = received_clim_sc_id + 1
            source_id = status.Get_source()
            print(time_series)
            checkt95Dict(time_series, t95_vals)

            t95_vals[time_series[2]][time_series[3]][time_series[4]].append([time_series[5], time_series[9]])

            if send_clim_sc_id < num_files:
                time_series = data_files[send_clim_sc_id]
                comm.send(time_series, dest=source_id, tag=send_clim_sc_id)
                send_clim_sc_id = send_clim_sc_id + 1

    while received_clim_sc_id < num_files:
        source_id = -1
        if received_clim_sc_id < num_files:
            time_series = comm.recv(status=status)
            received_clim_sc_id = received_clim_sc_id + 1
            source_id = status.Get_source()
            checkt95Dict(time_series, t95_vals)
            # if time_series[2] not in t95_vals:
            #     t95_vals[time_series[2]] = {}
            # if time_series[3] not in t95_vals[time_series[2]]:
            #     t95_vals[time_series[2]][time_series[3]] = {}
            # if time_series[4] not in t95_vals[time_series[2]][time_series[3]]:
            #     t95_vals[time_series[2]][time_series[3]][time_series[4]] = []
            t95_vals[time_series[2]][time_series[3]][time_series[4]].append([time_series[5], time_series[9]])


            if send_clim_sc_id < num_files:
                time_series = data_files[send_clim_sc_id]
                comm.send(time_series, dest=source_id, tag=send_clim_sc_id)
                send_clim_sc_id = send_clim_sc_id + 1


else:
    do_abort = False
    while not do_abort:
        time_series = comm.recv()
        tag = status.Get_tag()
        if tag < 0:
            do_abort = True

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

        filename = "replicate_" + time_series[5] + ".txt"
        filename_t95 = "replicate_" + time_series[5] + "_t95.txt"
        msg = "Calculating EHF for " + time_series[2] + " " + time_series[3] + " " + time_series[4] + " replicate " + \
              time_series[5]
        print(msg)
        t95 = calcEHF(time_series[0], filename, filename_t95)
        time_series.append(t95)
        comm.send(time_series, dest=0)



if rank == 0:
    os.chdir(work_dir)

    with open('t95vals.pickle', 'wb') as f:
       # Pickle the 'data' dictionary using the highest protocol available.
       pickle.dump(t95_vals, f, pickle.HIGHEST_PROTOCOL)

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
                            t95_str = gcm + "\t" + sc + "\t" + statn + "\t" + t95_vals[0] + "\t" + t95_vals[1] + "\n"
                            f_t95.write(t95_str)
                    t95avg_file = "t95_avg.txt"
                    with open(t95avg_file, 'w') as f_avg:
                        f_avg.write(avg_t95)
                    t95_avg_vals[gcm][sc][statn] = avg_t95

if rank == 0:
    i = 0
    stat_vals = {}
    for time_series in data_files:
        send_clim_sc_id = 0
        for idx in range(size):
            if send_clim_sc_id < num_files:
                time_series = data_files[send_clim_sc_id]
                t95_val = t95_avg_vals[time_series[2]][time_series[3]][time_series[4]]
                send_tuple = (time_series, t95_val)
                comm.send(send_tuple, dest=idx, tag=send_clim_sc_id)
                send_clim_sc_id = send_clim_sc_id + 1

        received_clim_sc_id = 0
        for idx in range(size):
            source_id = -1
            if received_clim_sc_id < num_files:
                time_series = comm.recv(status=status)
                received_clim_sc_id = received_clim_sc_id + 1
                source_id = status.Get_source()
                print(time_series)
                checkStatVals(time_series, t95_vals)
                if not stat_vals[time_series[2]][time_series[3]][time_series[4]]:
                    stat_vals[time_series[2]][time_series[3]][time_series[4]] = this_stats;
                else:
                    stat_vals[time_series[2]][time_series[3]][time_series[4]] = \
                    stat_vals[time_series[2]][time_series[3]][time_series[4]] + this_stats

                if send_clim_sc_id < num_files:
                    time_series = data_files[send_clim_sc_id]
                    t95_val = t95_avg_vals[time_series[2]][time_series[3]][time_series[4]]
                    send_tuple = (time_series, t95_val)
                    comm.send(send_tuple, dest=idx, tag=send_clim_sc_id)
                    send_clim_sc_id = send_clim_sc_id + 1

        while received_clim_sc_id < num_files:
            source_id = -1
            if received_clim_sc_id < num_files:
                time_series = comm.recv(status=status)
                received_clim_sc_id = received_clim_sc_id + 1
                source_id = status.Get_source()
                checkStatVals(time_series, t95_vals)
                if not stat_vals[time_series[2]][time_series[3]][time_series[4]]:
                    stat_vals[time_series[2]][time_series[3]][time_series[4]] = this_stats;
                else:
                    stat_vals[time_series[2]][time_series[3]][time_series[4]] = \
                    stat_vals[time_series[2]][time_series[3]][time_series[4]] + this_stats

                if send_clim_sc_id < num_files:
                    time_series = data_files[send_clim_sc_id]
                    t95_val = t95_avg_vals[time_series[2]][time_series[3]][time_series[4]]
                    send_tuple = (time_series, t95_val)
                    comm.send(send_tuple, dest=idx, tag=send_clim_sc_id)
                    send_clim_sc_id = send_clim_sc_id + 1


else:
    do_abort = False
    while not do_abort:
        send_tuple = comm.recv()
        time_series = send_tuple[0]
        t95_val = send_tuple[1]
        tag = status.Get_tag()
        if tag < 0:
            do_abort = True

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

        filename = "replicate_" + time_series[5] + "recalc.txt"
        msg = "Calculating EHF recalc for " + time_series[2] + " " + time_series[3] + " " + time_series[4] + " replicate " + \
              time_series[5]
        print(msg)

        this_stats = Counter(calcEHF_recalc(time_series[0], filename, time_series[5],
                                            t95_val))
        time_series.append(this_stats)
        comm.send(time_series, dest=0)



