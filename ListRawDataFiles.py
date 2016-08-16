import os

def listRawDataFiles(rootdir, station_list):
    time_series_list = []
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            #print os.path.join(subdir, file)
            filepath = subdir + os.sep + file
    
            if filepath.endswith(".txt"):
                #print (filepath)
                format = file[-3:]
                replicate = file[-7:-4]
                station = file[-13:-8]
                climate = file[-17:-14]
                gcm = file[7:-18]
                zone = file[:4]
                info = [filepath, zone, gcm, climate, station, replicate, format, station_list[int(station)][0], station_list[int(station)][1], station_list[int(station)][2]]
                time_series_list.append(info)
    return time_series_list
