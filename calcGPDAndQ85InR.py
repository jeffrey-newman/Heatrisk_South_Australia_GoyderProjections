import os

# os.environ['R_HOME'] = '/Library/Frameworks/R.framework/Versions/3.3/Resources'

import rpy2
import rpy2.robjects as robjects
# import rpy2's package module
import rpy2.robjects.packages as rpackages
# import numpy as np
import pickle

robjects.r('''Sys.setenv(LANG = "en")''')

base = rpackages.importr('base')
# import R's utility package
utils = rpackages.importr('utils')

# select a mirror for R packages
utils.chooseCRANmirror(ind=1)  # select the first mirror in the list

# ENSURE WE HAAVE THE REQUIRED PACKAGES INSTALLED AND LOADED

# select a mirror for R packages
utils.chooseCRANmirror(ind=1)  # select the first mirror in the list
# R package names
packnames = ('fExtremes', 'ggplot2')
# R vector of strings
from rpy2.robjects.vectors import StrVector

# Selectively install what needs to be install.
# We are fancy, just because we can.
names_to_install = [x for x in packnames if not rpackages.isinstalled(x)]
if len(names_to_install) > 0:
    utils.install_packages(StrVector(names_to_install))

fExtremes = rpackages.importr('fExtremes')

# Function that fits a generalsieed Pareto Distribution to the positive EHF values., then calculates the 85 percentile value from this distribution
r_gpd_ehf2 = robjects.r('''
                        gpd_ehf <- function(data){
                          fit <- gpdFit(data, type = 'mle', u = 0)
                          pars <- fit@fit$par.ests
                          q85 <- qgpd(0.85, xi = pars['xi'], mu = 0, beta = pars['beta'])
                          val <- c(q85, pars[1], pars[2])
                          val <- setNames(val, c("qgpd85", "xi", "beta"))
                          return(val)
                        }
                        ''')

# res = r_gpd_ehf(r"/Volumes/home/QNAP RTRR Folder/Data/South Australia Goyder-CSIRO downscaled future climates/Adelaide_Mt_Lofty_Ranges/Processed/csiro.mk36/his/23013/heat_wave_ehfs.txt")

def CalcQ85FromGPDOfEHFv2(statn_info):
    # dmt_vals = np.zeros(0)
    # print(statn_info)
    all_ehfs = []

    for subdir, dirs, files in os.walk(os.getcwd()):
        for file in files:
            # print(os.path.join(subdir, file))
            # filepath = subdir + os.sep + file
            if file.endswith(".ehf"):
                with open(file, 'rb') as f:
                    ehfs_for_station = pickle.load(f)
                    all_ehfs.extend(ehfs_for_station)

    all_ehfs_Rvec = robjects.FloatVector(all_ehfs)
    res = r_gpd_ehf2(all_ehfs_Rvec)
    statn_info.append(res[2]) # value of beta
    statn_info.append(res[1]) # value of xi
    statn_info.append(res[0]) # value of qppd85

    with open('qgpd85.pickle', 'wb') as f:
        pickle.dump(statn_info, f, pickle.HIGHEST_PROTOCOL)
    return statn_info


def CalcQ85FromGPDOfEHF(statn_info):
    os.chdir(statn_info[0])
    os.chdir(statn_info[1])
    os.chdir(statn_info[2])
    os.chdir(statn_info[3])
    os.chdir(statn_info[4])

    if os.path.isfile('qgpd85.pickle'):
        try:
            with open ('qgpd85.pickle', 'rb') as f:
                previous_calced_val = pickle.load(f)
                return previous_calced_val
        except:
            return CalcQ85FromGPDOfEHFv2(statn_info)
    else:
        return CalcQ85FromGPDOfEHFv2(statn_info)

