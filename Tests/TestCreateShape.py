import os
import errno
import pickle

from CreateShapeFile import print2shape

os.environ['GDAL_DATA'] = r"/Users/a1091793/anaconda/pkgs/libgdal-2.1.0-0/share/gdal"


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


stats_file = r"/Volumes/Samsung_T3/heatwave/Processed/stats.pickle"
work_dir = r"/Volumes/Samsung_T3/heatwave/Processed"

with open(stats_file, 'rb') as f:
    # Pickle the 'data' dictionary using the highest protocol available.
    stat_vals = pickle.load(f)

space_oriented_root_dir = work_dir + r"/spaceDomain"
make_sure_path_exists(space_oriented_root_dir)

rank = 0
if rank == 0:
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