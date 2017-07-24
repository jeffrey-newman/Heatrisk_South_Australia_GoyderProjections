import os


def listRawDataFiles(rootdir, station_list):
    """
    Walks through recursively through a directory to find climate timeseries data files. Matches these to the stations in a station list.
    :param rootdir: is the directory wherein the station data is found.
    :param station_list: is a dict with station id as the key, and with a list of values in this order: (0) station name, (1) longitude, and (2) latitude
    :return: A list of datafiles. List contains: (0) path to file, (1) the climatic zone, (2) The GCM used to generate the data,
            (3) The future climate scenario. (4) The station id, (5) the replicate downscale number, (6) The file format,
            (7) Station name, (8) longitude, (9) lattitude
    """
    gcms = ["miroc.esm", "miroc5", "access10", "access13", "cnrm.cm5", "csiro.mk36", "bcc.csm11m", "gfdl.esm2g", "gfdl.esm2m", "ipsl.cm5blr", "canesm2", "noresm1m", "inmcm4", "ipsl.cm5alr", "mri.cgcm3"]
    time_series_list = []
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            # print(os.path.join(subdir, file))
            filepath = subdir + os.sep + file
    
            if filepath.endswith(".txt"):

                for gcm_name in gcms:
                    p1 = file.find(gcm_name)
                    if file.find(gcm_name) != -1:
                        break

                # print (filepath)
                p6 = file.rfind('.')
                # p6 = str.rfind('.')
                p5 = file.rfind('.', 0, p6)
                p4 = file.rfind('.', 0, p5)
                p3 = file.rfind('.', 0, p4)
                format = file[p6 + 1:]
                replicate = file[p5 + 1:p6]
                station = file[p4 + 1:p5]
                climate = file[p3 + 1:p4]
                gcm = file[p1:p3]
                zone = file[:p1 - 1]
                info = [filepath, zone, gcm, climate, station, replicate, format, station_list[int(station)][0], station_list[int(station)][1], station_list[int(station)][2]]
                time_series_list.append(info)

    return time_series_list




