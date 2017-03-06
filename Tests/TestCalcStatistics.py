from CalcStatistics import calcStatistics
import pickle


ehf_file = r"/Volumes/Samsung_T3/heatwave/Processed/amlr27/csiro.mk36/his/ADELAIDE (DRY CREEK SALTWORKS)/replicate_001ehf_calcs.pickle"
q85_val = 26.1

calcStatistics(ehf_file, q85_val, "yearly_stats.txt", "accum_stats.txt", "stats.pickle")
