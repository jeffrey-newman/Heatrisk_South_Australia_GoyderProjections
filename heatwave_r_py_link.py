# -*- coding: utf-8 -*-
"""
Created on Mon Aug 29 11:38:06 2016

@author: a1091793
"""
import numpy as np
import scipy as sp
import rpy2
import rpy2.robjects as robjects
from rpy2.robjects.packages import importr
# import rpy2's package module
import rpy2.robjects.packages as rpackages
# R vector of strings
from rpy2.robjects.vectors import StrVector

# import R's utility package
utils = rpackages.importr('utils')
# select a mirror for R packages
utils.chooseCRANmirror(ind=1) # select the first mirror in the list

# R package names
packnames = ('fExtremes', 'ggplot2')

# R vector of strings
from rpy2.robjects.vectors import StrVector

# Selectively install what needs to be install.
# We are fancy, just because we can.
names_to_install = [x for x in packnames if not rpackages.isinstalled(x)]
if len(names_to_install) > 0:
    utils.install_packages(StrVector(names_to_install))
    
fExtremes = importr('fExtremes')


r_gpd_ehf = robjects.r('''
                        gpd_ehf <- function(path_data){
                          ehf <- read.table(path_data)
                          fit <- gpdFit(ehf$V1, type = 'mle', u = 0)
                          pars <- fit@fit$par.ests
                          q85 <- qgpd(0.85, xi = pars['xi'], mu = 0, beta = pars['beta'])
                          val <- c(q85, pars[1], pars[2])
                          val <- setNames(val, c("qgpd85", "xi", "beta"))
                          return(val)
                        }
                        ''')
    
res = r_gpd_ehf(r"/Volumes/home/QNAP RTRR Folder/Data/South Australia Goyder-CSIRO downscaled future climates/Adelaide_Mt_Lofty_Ranges/Processed/csiro.mk36/his/23013/heat_wave_ehfs.txt")     

qgpd85 = res[0]
xi = res[1]
beta = res[2]

