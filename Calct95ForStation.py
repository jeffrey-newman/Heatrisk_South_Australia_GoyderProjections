import os
import numpy as np
import pickle

def calct95ForStationv2(statn_info):
    dmt_vals = np.zeros(0)
    # print(statn_info)

    for subdir, dirs, files in os.walk(os.getcwd()):
        for file in files:
            # print(os.path.join(subdir, file))
            # filepath = subdir + os.sep + file
            if file.endswith(".dmt"):
                with open(file, 'rb') as f:
                    daily_means = pickle.load(f)
                    expansion = (daily_means.shape)[0]
                    # print("expand by: ", expansion)
                    current_size = dmt_vals.size
                    # print("current size is: ", current_size)
                    new_size = current_size + expansion
                    dmt_vals.resize(new_size, refcheck =False)
                    dmt_vals[current_size:] = daily_means

    avg_t95 = np.percentile(dmt_vals, 95.0, overwrite_input=True)

    t95avg_file = "t95_avg.txt"
    with open(t95avg_file, 'w') as f_avg:
        f_avg.write(str(avg_t95))
    statn_info.append(avg_t95)
    with open('t95_avg.pickle', 'wb') as f:
        pickle.dump(statn_info, f, pickle.HIGHEST_PROTOCOL)
    return statn_info

def calct95ForStation(statn_info):
    os.chdir(statn_info[0])
    os.chdir(statn_info[1])
    os.chdir(statn_info[2])
    os.chdir(statn_info[3])
    os.chdir(statn_info[4])

    if os.path.isfile('t95_avg.pickle'):
        try:
            with open ('t95_avg.pickle', 'rb') as f:
                previous_calced_val = pickle.load(f)
                return previous_calced_val
        except:
            return calct95ForStationv2(statn_info)
    else:
        return calct95ForStationv2(statn_info)


