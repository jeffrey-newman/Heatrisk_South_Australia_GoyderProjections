# -*- coding: utf-8 -*-
"""
Created on Tue Aug 16 23:44:33 2016

@author: a1091793
"""

import EHFRecalc
from EHFRecalc import calcEHF_recalc
import imp
from imp import reload
imp.reload(EHFRecalc); from EHFRecalc import calcEHF_recalc
import os

os.chdir(r"/Volumes/home/QNAP RTRR Folder/Data/South Australia Goyder-CSIRO downscaled future climates/Adelaide_Mt_Lofty_Ranges/Processed/csiro.mk36/r45/23013")
filename = "replicate_001_recalc.txt"
data = r"/Volumes/home/QNAP RTRR Folder/Data/South Australia Goyder-CSIRO downscaled future climates/Adelaide_Mt_Lofty_Ranges/Adelaide_Mt_Lofty_Ranges/amlr27.csiro.mk36.r45.23013.001.txt"
calcEHF_recalc(data, filename, 25.7)