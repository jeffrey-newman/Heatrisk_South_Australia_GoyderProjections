# -*- coding: utf-8 -*-
"""
Created on Fri Aug 26 00:41:34 2016

@author: a1091793
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Aug 16 16:36:40 2016

@author: a1091793
"""

# -*- coding: utf-8 -*-
import numpy as np
import pickle
# import scipy.stats as ss
# import scipy as sp

# from rpy2.robjects.packages import importr
# import rpy2.robjects as ro
# # import pandas.rpy.common as com
import xlsxwriter


# gPdtest = importr('gPdtest')


# WHAT MAKES THIS FILE DIFFERENCE FROM EHFRecalc?

def calcEHF(file_path, filename_calcs, t95, filename_ehfs):
    # Year, Month, Day, Weather State (you probably won’t use this), Rainfall (mm), Tmax (oC), Tmin (oC), Short wave solar radiation (MJ/m2), Vapour Pressure Deficit (hPa), Morton’s APET (mm).
    raw = np.dtype([('year', np.uint), ('month', np.uint), ('day', np.uint), ('wState', np.uint), ('rain', np.float_),
                    ('maxT', np.float_), ('minT', np.float_), ('srad', np.float_), ('pres', np.float_),
                    ('apet', np.float_)])
    calced = np.dtype(
        [('dmt', np.float_), ('3day', np.float_), ('30day', np.float_), ('EHI_sig', np.float_), ('EHI_accl', np.float_),
         ('EHF', np.float_), ('EHF_respec', np.float_), ('Heatwave_day', np.uint), ('EHF_12day_sum', np.float_),
         ('EHF_12day_max', np.float_), ('RF_cat0', np.uint), ('RF_cat1', np.uint), ('RF_cat2', np.uint), ('RF_cat3', np.uint),
         ('RF_cat4', np.uint), ('RF_fatalityrate', np.float_), ('Severity', np.float_), ('low', np.uint), ('medium', np.uint), ('high', np.uint)])
    dt = np.dtype([('raw', raw), ('calced', calced)])


    raw_data = np.loadtxt(file_path, dtype=raw)
    data = np.empty(raw_data.size, dt)

    # load raw data into amalgamated data array
    it = np.nditer(data, flags=['f_index'])
    while not it.finished:
        index = it.index
        data[index]['raw'] = raw_data[index]
        # Calculate daily mean temperature
        # See note in Nairne and Fawcett (2015) The Excess Heat Factor: A Metric for Heatwave Intensity and Its Use in Classifying Heatwave Severity. Int. J. Environ. Res. Public Health 2015, 12, 227-253; doi:10.3390/ijerph120100227
        # For heat, better to use max and min records for the same day, even though 'day' for BOM in Australia is based on a 9am to 9am cycle, meaning max and min will usually be on seperate days.
        # See comments made by Nairne and Fawcett in their introduction.
        rd = data[index]['raw']
        cd = data[index]['calced']
        cd['dmt'] = (rd['maxT'] + rd['minT']) / 2

        # Calculate three day averages
        cd['3day'] = 0
        if (index > 1):
            for i in range(index - 2, index + 1):
                cd['3day'] += data[i]['calced']['dmt']
            cd['3day'] /= 3

        # Calculate thirty day averages
        cd['30day'] = 0
        if (index == 32):
            for i in range(index - 32, index - 2):
                cd['30day'] += data[i]['calced']['dmt']
            cd['30day'] /= 30
        if (index > 32):
            cd['30day'] = data[index - 1]['calced']['30day'] * 30 - data[index - 33]['calced']['dmt'] + data[index - 3]['calced']['dmt']
            cd['30day'] /= 30

        # print index
        it.iternext()

    # #Calculate the 95th percentile
    #    t95 = np.percentile(daily_means, 95.0, overwrite_input=True)
    #    f_t95 = open(path_out_t95, 'w')
    #    f_t95.write(str(t95))

    heat_wave_ehfs = []

    in_heatwave = False;
    day_of_peak_heatwave = -1
    max_ehf = 0
    max_sum_ehf = 0
    day_of_start_heatwave = -1
    # Calculate EHI_sig
    # load raw data into amalgamated data array
    it = np.nditer(data, flags=['f_index'])
    while not it.finished:
        index = it.index
        data[index]['raw'] = raw_data[index]
        # Calculate daily mean temperature
        # See note in Nairne and Fawcett (2015) The Excess Heat Factor: A Metric for Heatwave Intensity and Its Use in Classifying Heatwave Severity. Int. J. Environ. Res. Public Health 2015, 12, 227-253; doi:10.3390/ijerph120100227
        # For heat, better to use max and min records for the same day, even though 'day' for BOM in Australia is based on a 9am to 9am cycle, meaning max and min will usually be on seperate days.
        # See comments made by Nairne and Fawcett in their introduction.
        rd = data[index]['raw']
        cd = data[index]['calced']
        if (index > 1):
            cd['EHI_sig'] = cd['3day'] - t95
        else:
            cd['EHI_sig'] = 0

        cd['EHI_accl'] = 0
        cd['EHF'] = 0
        cd['EHF_respec'] = 0
        cd['EHF_12day_sum'] = 0;
        cd['EHF_12day_max'] = 0;
        if (index > 31):
            cd['EHI_accl'] = cd['3day'] - cd['30day']
            cd['EHF'] = cd['EHI_sig'] * max(1, cd['EHI_accl'])
            cd['EHF_respec'] = max(0, cd['EHI_sig']) * max(1, cd['EHI_accl'])
            if (cd['EHF_respec'] > 0):
                heat_wave_ehfs.append(cd['EHF_respec'])
            # Calculate the 12 day avg and max for death statistics
            if (index == 43):
                for i in range(index - 11, index + 1):
                    cd['EHF_12day_sum'] += data[i]['calced']['EHF_respec']
            if (index > 43):
                cd['EHF_12day_sum'] = data[index - 1]['calced']['EHF_12day_sum'] + cd['EHF_respec'] - data[index - 12]['calced']['EHF_respec']


        cd['RF_cat0'] = 0
        cd['RF_cat1'] = 0
        cd['RF_cat2'] = 0
        cd['RF_cat3'] = 0
        cd['RF_cat4'] = 0
        cd['RF_fatalityrate'] = 0

        if (cd['EHF_respec'] > 0 and in_heatwave == False):
            in_heatwave = True
            day_of_start_heatwave = index

        if (cd['EHF_respec'] <= 0 and in_heatwave == True):
            #we are at the end of a heatwave event.
            for i in range(day_of_start_heatwave, index + 1):
                if (data[i]['calced']['EHF'] > max_ehf):
                    max_ehf = data[i]['calced']['EHF']
                    day_of_peak_heatwave = i
            for i in range(day_of_peak_heatwave, day_of_peak_heatwave + 12):
                if (data[i]['calced']['EHF_12day_sum'] > max_sum_ehf):
                    max_sum_ehf = data[i]['calced']['EHF_12day_sum']
            if (max_sum_ehf > 300 and max_ehf > 70):
                data[day_of_peak_heatwave]['calced']['RF_cat4'] = 1
                data[day_of_peak_heatwave]['calced']['RF_fatalityrate'] = 1.7
            elif (max_sum_ehf > 150 and max_ehf > 50):
                data[day_of_peak_heatwave]['calced']['RF_cat3'] = 1
                data[day_of_peak_heatwave]['calced']['RF_fatalityrate'] = 0.2
            elif (max_sum_ehf > 80 and max_ehf > 30):
                data[day_of_peak_heatwave]['calced']['RF_cat2'] = 1
                data[day_of_peak_heatwave]['calced']['RF_fatalityrate'] = 0.1
            elif (max_sum_ehf > 30 and max_ehf > 15):
                data[day_of_peak_heatwave]['calced']['RF_cat1'] = 1
                data[day_of_peak_heatwave]['calced']['RF_fatalityrate'] = 0.05
            else:
                data[day_of_peak_heatwave]['calced']['RF_cat0'] = 1
                data[day_of_peak_heatwave]['calced']['RF_fatalityrate'] = 0
            in_heatwave = False
            max_ehf = 0
            day_of_start_heatwave = -1
            day_of_peak_heatwave = -1
            max_sum_ehf = 0

        # print index
        it.iternext()
        cd['Heatwave_day'] = 0
        cd['low'] = 0
        cd['medium'] = 0
        cd['high'] = 0


    with open(filename_calcs, "wb") as f:
        pickle.dump(data,f, pickle.HIGHEST_PROTOCOL)

    with open (filename_ehfs, "wb") as f:
        pickle.dump(heat_wave_ehfs, f, pickle.HIGHEST_PROTOCOL)
