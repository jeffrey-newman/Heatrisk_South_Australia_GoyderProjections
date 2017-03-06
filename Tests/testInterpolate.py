from InterpolateMap import interpolateAndMap
import os
import pickle

work_dir = r"/Volumes/Samsung_T3/heatwave/Processed"
os.chdir(work_dir)

with open('space_domain_directories.pickle', 'rb') as f:
    space_domain_directories = pickle.load(f)

interpolateAndMap(space_domain_directories[0])

