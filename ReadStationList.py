import numpy as np
import fileinput

def readStationList(stationList_file):
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
        statn_info[statn_id_int] = [statn_name, statn_long_flt, statn_lat_flt]
        
    return (statn_info)
    
