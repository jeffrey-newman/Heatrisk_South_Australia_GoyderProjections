
import os
import numpy as np
import pickle

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

def aggregateStatsv2(statn_info):
    agg_stats = None
    num_replicates = 0

    for subdir, dirs, files in os.walk(os.getcwd()):
        for file in files:
            # print(os.path.join(subdir, file))
            # filepath = subdir + os.sep + file
            if file.endswith(".stats"):
                with open(file, 'rb') as f:
                    this_stats = pickle.load(f)
                    if agg_stats is None:
                        agg_stats = this_stats;
                        num_replicates += 1
                    else:
                        agg_stats = combine_stat_dicts(agg_stats, this_stats)
                        num_replicates += 1

    for year in agg_stats:
        agg_stats[year] = [stat_val / num_replicates for stat_val in agg_stats[year] ]

    agg_stats_file = "aggregated_stats.pickle"
    with open(agg_stats_file, 'wb') as f_stats:
        pickle.dump(agg_stats, f_stats, pickle.HIGHEST_PROTOCOL)

    statn_info.append(agg_stats)
    return statn_info


def aggregateStatsTest(dir):
    agg_stats = None
    num_replicates = 0

    for subdir, dirs, files in os.walk(dir):
        for file in files:
            print(os.path.join(subdir, file))
            filepath = subdir + os.sep + file
            if file.endswith(".stats"):
                with open(filepath, 'rb') as f:
                    this_stats = pickle.load(f)
                    if agg_stats is None:
                        agg_stats = this_stats;
                        num_replicates += 1
                    else:
                        agg_stats = combine_stat_dicts(agg_stats, this_stats)
                        num_replicates += 1

    # print(agg_stats[1961])
    for year in agg_stats:
        agg_stats[year] = [stat_val / num_replicates for stat_val in agg_stats[year] ]
    # print(agg_stats[1961])

    agg_stats_file = "aggregated_stats.pickle"
    with open(agg_stats_file, 'wb') as f_stats:
        pickle.dump(agg_stats, f_stats, pickle.HIGHEST_PROTOCOL)

    return agg_stats


def aggregateStats(statn_info):
    os.chdir(statn_info[0])
    os.chdir(statn_info[1])
    os.chdir(statn_info[2])
    os.chdir(statn_info[3])
    os.chdir(statn_info[4])
    if os.path.isfile('aggregated_stats.pickle'):
        try:
            with open ('aggregated_stats.pickle', 'rb') as f:
                previous_calced_val = pickle.load(f)
                return previous_calced_val
        except:
            return aggregateStatsv2(statn_info)
    else:
        return aggregateStatsv2(statn_info)