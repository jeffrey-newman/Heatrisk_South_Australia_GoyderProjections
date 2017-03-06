import os


def listRawDataFiles(rootdir, station_list):
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




