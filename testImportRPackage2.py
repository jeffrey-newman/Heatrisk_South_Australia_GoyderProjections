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
packnames = ('fExtremes', 'gplot2')

# R vector of strings
from rpy2.robjects.vectors import StrVector

# Selectively install what needs to be install.
# We are fancy, just because we can.
names_to_install = [x for x in packnames if not rpackages.isinstalled(x)]
if len(names_to_install) > 0:
    utils.install_packages(StrVector(names_to_install))