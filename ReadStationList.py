import numpy as np
import fileinput
import re


def readStationList(stationList_file):
    """
    Reads a 'blank' seperated text file with a list of stations with their corresponding coordinates.
    :param stationList_file: The file containing a list of stations with their coordinates
    :return: An Python dict with stationa names mapped to their coordinates.revious
    """
    statn_info = dict()
    for line in fileinput.input(stationList_file):
        statn_id = line[:6]
        statn_id_int = int(statn_id)
        statn_name = line[7:-19]
        statn_long = line[-9:]
        statn_long_flt = float(statn_long)
        statn_lat = line[-19:-11]
        statn_lat_flt = float(statn_lat)
        #s = "station id: " + statn_id + " with name: " + statn_name + " located at: " + statn_lat + " lat, " + statn_long + " long"
        #print(s)
        statn_name2 = statn_name.strip()
        re.sub(r'[^\w]', ' ', statn_name2)
        statn_info[statn_id_int] = [statn_name2, statn_long_flt, statn_lat_flt]

    return (statn_info)


