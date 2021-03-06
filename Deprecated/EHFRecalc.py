# -*- coding: utf-8 -*-
"""
Created on Tue Aug 16 16:36:40 2016

@author: a1091793
"""

# -*- coding: utf-8 -*-
import numpy as np
import scipy.stats as ss 
import scipy as sp

from rpy2.robjects.packages import importr
import rpy2.robjects as ro
# import pandas.rpy.common as com

#gPdtest = importr('gPdtest')

def calcEHF(file_path, path_out, replicate, t95):
    # Year, Month, Day, Weather State (you probably won’t use this), Rainfall (mm), Tmax (oC), Tmin (oC), Short wave solar radiation (MJ/m2), Vapour Pressure Deficit (hPa), Morton’s APET (mm).
    raw = np.dtype([('year', np.uint), ('month', np.uint), ('day', np.uint), ('wState', np.uint),('rain', np.float_), ('maxT', np.float_), ('minT', np.float_), ('srad', np.float_), ('pres', np.float_), ('apet', np.float_)])
    calced = np.dtype([('dmt', np.float_), ('3day', np.float_), ('30day', np.float_), ('EHI_sig', np.float_), ('EHI_accl', np.float_), ('EHF', np.float_), ('EHF_respec', np.float_), ('Heatwave_day', np.uint)])
    dt = np.dtype([('raw', raw), ('calced',calced)])
    column_header = 'year\tmonth\tday\twState\train\tmaxT\tminT\tsrad\tpres\tapet\tdmt\t3day\t30day\tEHI_sig\tEHI_accl\tEHF\tEHF_respec\n'
    
    raw_data = np.loadtxt(file_path, dtype=raw)
    data = np.empty(raw_data.size, dt)
    daily_means = np.zeros(raw_data.size)
    
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
        cd['dmt'] = ( rd['maxT'] + rd['minT'] ) / 2
        daily_means[index] = cd['dmt'] 
        
        # Calculate three day averages
        cd['3day'] = 0
        if (index > 1):
            for i in range(index-2, index+1):
                cd['3day'] += data[i]['calced']['dmt']
            cd['3day'] /= 3
            
        # Calculate thirty day averages
        cd['30day'] = 0
        if (index > 28):
            for i in range(index-29, index+1):
                cd['30day'] += data[i]['calced']['dmt']
            cd['30day'] /= 30
        
        #print index
        it.iternext()
    
    
#    #Calculate the 95th percentile
#    t95 = np.percentile(daily_means, 95.0, overwrite_input=True)
#    f_t95 = open(path_out_t95, 'w')
#    f_t95.write(str(t95))
    
    heat_wave_ehfs = []
    
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
            
        if (index > 28):
            cd['EHI_accl'] = cd['3day'] - cd['30day']
            cd['EHF'] = cd['EHI_sig'] * max(1, cd['EHI_accl'])
            cd['EHF_respec'] = max(0, cd['EHI_sig']) * max(1, cd['EHI_accl'])
            if (cd['EHF_respec'] > 0):
                heat_wave_ehfs.append(cd['EHF_respec'])
        else:
            cd['EHI_accl'] = 0
            cd['EHF'] = 0
            cd['EHF_respec'] = 0
            
        cd['Heatwave_day'] = 0    
                
        #print index
        it.iternext()
    
    ehf_file = open('heat_wave_ehfs.txt', 'w')
    for item in heat_wave_ehfs:
        ehf_file.write(str(item) + "\n")
    
    # We can now determine the 85 percentile based on gpd. Use R for this.
    # ro.r('')


    sum_days_summer = 0         #0
    sum_days_autumn = 0         #1
    sum_days_winter = 0         #2
    sum_days_spring = 0         #3
    sum_ehf_summer = 0          #4
    sum_ehf_autumn = 0          #5
    sum_ehf_winter = 0          #6
    sum_ehf_spring = 0          #7
    max_ehf_summer = 0          #8
    max_ehf_autumn = 0          #9
    max_ehf_winter = 0          #10
    max_ehf_spring = 0          #11
    tot_days_in_summer = 0      #12
    tot_days_in_autumn = 0      #13
    tot_days_in_winter = 0      #14
    tot_days_in_spring = 0      #15
    prop_days_in_summer = 0     #16
    prop_days_in_autumn = 0     #17
    prop_days_in_winter = 0     #18
    prop_days_in_spring = 0     #19
    avg_ehf_summer = 0          #20
    avg_ehf_autumn = 0          #21
    avg_ehf_winter = 0          #22
    avg_ehf_spring = 0          #23
    sum_days = 0                #24
    sum_ehf = 0                 #25
    max_ehf = 0                 #26
    tot_days = 0                #27
    prop_days = 0               #28
    avg_ehf = 0                 #29
    yearly_stats = {}

    f = open(path_out, 'w')
    f.write(column_header)
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
        if (cd['EHF_respec'] > 0):
            cd['Heatwave_day'] = 1
            if ((index - 1 ) > 0 ):
                data[index - 1]['calced']['Heatwave_day'] = 1
            if ((index - 2 ) > 0 ):
                data[index - 2]['calced']['Heatwave_day'] = 1   
        dat = str(rd[0]) + "\t" + str(rd[1]) + "\t" + str(rd[2]) + "\t" + str(rd[3]) + "\t" + str(rd[4]) + "\t" + str(rd[5]) + "\t" + str(rd[6]) + "\t" + str(rd[7]) + "\t" + str(rd[8]) + "\t" + str(rd[9]) + "\t" + str(cd[0]) + "\t" + str(cd[0]) + "\t" + str(cd[1]) + "\t" + str(cd[2]) + "\t" + str(cd[3]) + "\t" + str(cd[4]) + "\t" + str(cd[5]) + "\t" + str(cd[6]) + "\t" + str(cd[7]) + "\n"
        f.write(dat)
        it.iternext()  
        
        year = rd['year']
        if year not in yearly_stats:
            yearly_stats[year] = [0.0] * 30
        month = rd['month']
        if (1 <= month <= 2) or (month == 12):
            # summer
            yearly_stats[year][12] += 1
            tot_days_in_summer += 1
            if (cd['EHF_respec'] > 0):
                yearly_stats[year][0] += 1
                sum_days_summer += 1
                yearly_stats[year][4] += cd['EHF_respec']
                sum_ehf_summer += cd['EHF_respec']
                if (cd['EHF_respec'] > max_ehf_summer): 
                    max_ehf_summer = cd['EHF_respec']
                if (cd['EHF_respec'] > yearly_stats[year][8]): 
                    yearly_stats[year][8] = cd['EHF_respec']
                    
        
        if (3 <= month <= 5):
            #autumn
            yearly_stats[year][13] += 1
            tot_days_in_autumn += 1
            if (cd['EHF_respec'] > 0):
                yearly_stats[year][1] += 1
                sum_days_autumn += 1
                sum_ehf_autumn += cd['EHF_respec']
                yearly_stats[year][5] += cd['EHF_respec']
                if (cd['EHF_respec'] > max_ehf_autumn): 
                    max_ehf_autumn = cd['EHF_respec']
                if (cd['EHF_respec'] > yearly_stats[year][9]): 
                    yearly_stats[year][9] = cd['EHF_respec']
        
        if (6 <= month <= 8):
            #winter
            yearly_stats[year][14] += 1
            tot_days_in_winter += 1
            if (cd['EHF_respec'] > 0):
                yearly_stats[year][2] += 1
                sum_days_winter += 1
                sum_ehf_winter += cd['EHF_respec']
                yearly_stats[year][6] += cd['EHF_respec']
                if (cd['EHF_respec'] > max_ehf_winter): 
                    max_ehf_winter = cd['EHF_respec']
                if (cd['EHF_respec'] > yearly_stats[year][10]): 
                    yearly_stats[year][10] = cd['EHF_respec']
        
        if (9 <= month <= 11):
            yearly_stats[year][15] += 1
            tot_days_in_spring+= 1
            if (cd['EHF_respec'] > 0):
                yearly_stats[year][3] += 1
                sum_days_spring += 1
                sum_ehf_spring += cd['EHF_respec']
                yearly_stats[year][7] += cd['EHF_respec']
                if (cd['EHF_respec'] > max_ehf_spring): 
                    max_ehf_spring = cd['EHF_respec']
                if (cd['EHF_respec'] > yearly_stats[year][11]): 
                    yearly_stats[year][11] = cd['EHF_respec']
            
    prop_days_in_summer = sum_days_summer / float(tot_days_in_summer)
    prop_days_in_autumn = sum_days_autumn / float(tot_days_in_autumn)
    prop_days_in_winter = sum_days_winter / float(tot_days_in_winter)
    prop_days_in_spring = sum_days_spring / float(tot_days_in_spring)
    if (sum_days_summer > 0):
        avg_ehf_summer = sum_ehf_summer / float(sum_days_summer)
    else:
        avg_ehf_summer = 0
    if (sum_days_autumn > 0):
        avg_ehf_autumn = sum_ehf_autumn / float(sum_days_autumn)
    else:
        avg_ehf_autumn = 0
    if (sum_days_winter > 0):
        avg_ehf_winter = sum_ehf_winter / float(sum_days_winter)
    else:
        avg_ehf_winter = 0
    if (sum_days_spring > 0):
        avg_ehf_spring = sum_ehf_spring / float(sum_days_spring)
    else:
        avg_ehf_spring = 0
    sum_days = sum_days_summer + sum_days_autumn + sum_days_winter + sum_days_spring
    sum_ehf = sum_ehf_summer + sum_ehf_autumn + sum_ehf_winter + sum_ehf_spring
    max_ehf = max(max_ehf_summer, max_ehf_autumn, max_ehf_winter, max_ehf_spring)
    tot_days = tot_days_in_summer + tot_days_in_autumn + tot_days_in_winter + tot_days_in_spring
    prop_days = sum_days / float(tot_days)
    if (sum_days > 0):
        avg_ehf = sum_ehf / sum_days
    else:
        avg_ehf = 0
    
    for year, stats in yearly_stats.items():
        stats[16] = stats[0] / float(stats[12])
        stats[17] = stats[1] / float(stats[13])
        stats[18] = stats[2] / float(stats[14])
        stats[19] = stats[3] / float(stats[15])
        if (stats[0]  > 0):
            stats[20] = stats[4] / float(stats[0])
        else:
            stats[20] = 0
        if (stats[1]  > 0):
            stats[21] = stats[5] / float(stats[1])
        else:
            stats[21] = 0
        if (stats[2]  > 0):
            stats[22] = stats[6] / float(stats[2])
        else:
            stats[22] = 0
        if (stats[3]  > 0):
            stats[23] = stats[7] / float(stats[3])
        else:
            stats[23] = 0
        stats[24] = stats[0] + stats[1] + stats[2] + stats[3]
        stats[25] = stats[4] + stats[5] + stats[6] + stats[7]
        stats[26] = max(stats[8], stats[9], stats[10], stats[11])
        stats[27] = stats[12] + stats[13] + stats[14] + stats[15]
        stats[28] = stats[24] / float(stats[27])
        if (stats[24] > 0):
            stats[29] = stats[25] / stats[24]
        else:
            stats[29] = 0
    
    with open(('stats_rep' + str(replicate) + '.txt'), 'w') as f_stats:
        f_stats.write("stat\tval")
        f_stats.write("sum_days_summer\t" + str(sum_days_summer) + "\n")
        f_stats.write("sum_days_autumn\t" + str(sum_days_autumn) + "\n")
        f_stats.write("sum_days_winter\t" + str(sum_days_winter) + "\n")
        f_stats.write("sum_days_spring\t" + str(sum_days_spring) + "\n")
        f_stats.write("sum_ehf_summer\t"  + str(sum_ehf_summer ) + "\n")
        f_stats.write("sum_ehf_autumn\t"  + str(sum_ehf_autumn ) + "\n")
        f_stats.write("sum_ehf_winter\t"  + str(sum_ehf_winter ) + "\n")
        f_stats.write("sum_ehf_spring\t"  + str(sum_ehf_spring ) + "\n")
        f_stats.write("max_ehf_summer\t"  + str(max_ehf_summer ) + "\n")
        f_stats.write("max_ehf_autumn\t"  + str(max_ehf_autumn ) + "\n")
        f_stats.write("max_ehf_winter\t"  + str(max_ehf_winter ) + "\n")
        f_stats.write("max_ehf_spring\t"  + str(max_ehf_spring ) + "\n")
        f_stats.write("tot_days_in_summer\t"  + str(tot_days_in_summer) + "\n")
        f_stats.write("tot_days_in_autumn\t"  + str(tot_days_in_autumn) + "\n")
        f_stats.write("tot_days_in_winter\t"  + str(tot_days_in_winter) + "\n")
        f_stats.write("tot_days_in_spring\t"  + str(tot_days_in_spring) + "\n")
        f_stats.write("prop_days_in_summer\t"  + str(prop_days_in_summer) + "\n")
        f_stats.write("prop_days_in_autumn\t"  + str(prop_days_in_autumn) + "\n")
        f_stats.write("prop_days_in_winter\t"  + str(prop_days_in_winter) + "\n")
        f_stats.write("prop_days_in_spring\t"  + str(prop_days_in_spring) + "\n")
        f_stats.write("avg_ehf_summer\t"  + str(avg_ehf_summer) + "\n")
        f_stats.write("avg_ehf_autumn\t"  + str(avg_ehf_autumn) + "\n")
        f_stats.write("avg_ehf_winter\t"  + str(avg_ehf_winter) + "\n")
        f_stats.write("avg_ehf_spring\t"  + str(avg_ehf_spring) + "\n")
        f_stats.write("sum_days\t"   + str(sum_days) + "\n")
        f_stats.write("sum_ehf\t"    + str(sum_ehf) + "\n")
        f_stats.write("max_ehf\t"    + str(max_ehf) + "\n")
        f_stats.write("tot_days\t"   + str(tot_days) + "\n")
        f_stats.write("prop_days\t"  + str(prop_days) + "\n")
        f_stats.write("avg_ehf\t"    + str(avg_ehf))

    
    stats = [sum_days_summer, sum_days_autumn, sum_days_winter, sum_days_spring, sum_ehf_summer, sum_ehf_autumn, sum_ehf_winter, sum_ehf_spring, max_ehf_summer, max_ehf_autumn, max_ehf_winter, max_ehf_spring, tot_days_in_summer, tot_days_in_autumn, tot_days_in_winter, tot_days_in_spring, prop_days_in_summer, prop_days_in_autumn, prop_days_in_winter, prop_days_in_spring, avg_ehf_summer, avg_ehf_autumn, avg_ehf_winter, avg_ehf_spring, sum_days, sum_ehf, max_ehf, tot_days, prop_days, avg_ehf]
        
    with open('yearly_stats_rep' + str(replicate) + '.txt', 'w') as f_ystats:
        f_ystats.write("year\tsum_days_summer\tsum_days_autumn\tsum_days_winter\tsum_days_spring\tsum_ehf_summer\tsum_ehf_autumn\tsum_ehf_winter\tsum_ehf_spring\tmax_ehf_summer\tmax_ehf_autumn\tmax_ehf_winter\tmax_ehf_spring\ttot_days_in_summer\ttot_days_in_autumn\ttot_days_in_winter\ttot_days_in_spring\tprop_days_in_summer\tprop_days_in_autumn\tprop_days_in_winter\tprop_days_in_spring\tavg_ehf_summer\tavg_ehf_autumn\tavg_ehf_winter\tavg_ehf_spring\tsum_days\tsum_ehf\tmax_ehf\ttot_days\tprop_days\tavg_ehf\t")
        for year, stats in yearly_stats.items():
            f_ystats.write(str(year)+"\t")
            for i in range(0, 30): 
                f_ystats.write(str(stats[i]) + "\t")
            f_ystats.write("\n")
    
    return yearly_stats
    #c=ss.genpareto.fit(heat_wave_ehfs)