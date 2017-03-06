import shutil
import os
import imp
import ReadStationList
from ReadStationList import readStationList
imp.reload(ReadStationList); from ReadStationList import readStationList
import ListRawDataFiles
from ListRawDataFiles import listRawDataFiles
imp.reload(ListRawDataFiles); from ListRawDataFiles import listRawDataFiles
import pickle
import re


def copyRawDataFiles(rootdir, station_list, newdir):
    # time_series_list = listRawDataFiles(rootdir, station_list)

    # # Pickle the time_series_list so we do not need to create this again in case of failure later on. It takes some time to prcoess through the tb of data files to form this list.
    # with open('climate_data_list.pickle', 'wb') as f:
    # # Pickle the 'data' dictionary using the highest protocol available.
    #     pickle.dump(time_series_list, f, pickle.HIGHEST_PROTOCOL)

    time_series_list = pickle.load(open('climate_data_list.pickle', 'rb') )

    for info in time_series_list:
        src = info[0]
        stat_name = (info[7]).strip()
        re.sub(r'[^\w]', ' ', stat_name)
        dst = os.path.join(os.path.join(os.path.join(os.path.join(os.path.join(newdir,info[1]),info[2]), info[3]),  stat_name), os.path.basename(info[0]))
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        s = "copying " + src + " to " + dst
        print(s)
        shutil.copy2(src,dst)



station_list = r"/Volumes/home/Jeffs Resilio Sync Folder/Data/South Australia Goyder-CSIRO downscaled future climates/StationList.txt"
rootdir = r"/Volumes/home/Jeffs Resilio Sync Folder/Data/South Australia Goyder-CSIRO downscaled future climates/All"
newdir = r"/Volumes/home/Jeffs Resilio Sync Folder/Data/South Australia Goyder-CSIRO downscaled future climates/Sorted"


stations = readStationList(station_list)
copyRawDataFiles(rootdir,stations,newdir)