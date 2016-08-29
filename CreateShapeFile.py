# -*- coding: utf-8 -*-
"""
Created on Wed Aug 17 21:18:53 2016

@author: a1091793
"""

import osgeo.ogr as ogr
import osgeo.osr as osr

def print2shape(statistics, year, gcm, sc, out_file, station_list):
    # set up the shapefile driver
    driver = ogr.GetDriverByName("ESRI Shapefile")
    # create the data source
    data_source = driver.CreateDataSource(out_file)
    # create the spatial reference, WGS84
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    
    # create the layer
    layer = data_source.CreateLayer("weather_stations", srs, ogr.wkbPoint)
    
     #Add the fields we're interested in
    field_id = ogr.FieldDefn("ID", ogr.OFTInteger)
    layer.CreateField(field_id)    
    
    field_name = ogr.FieldDefn("Name", ogr.OFTString)
    field_name.SetWidth(27)
    layer.CreateField(field_name)  
    
    layer.CreateField(ogr.FieldDefn("Latitude", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("Longitude", ogr.OFTReal))
    
    layer.CreateField(ogr.FieldDefn("sumDaysSmr", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sumDaysAut", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sumDaysWnt", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sumDaysSpr", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sumEhfSmr", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sumEhfAut", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sumEhfWnt", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sumEhfSpr", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("maxEhfSmr", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("maxEhfAut", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("maxEhfWnt", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("maxEhfSpr", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("totDaysSmr", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("totDaysAut", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("totDaysWin", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("totDaysSpr", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("PropDaySmr", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("PropDayAut", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("PropDayWin", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("PropDaySpr", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("avgEhfSmr", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("avgEhfAut", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("avgEhfWnt", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("avgEhfSpr", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sum_days", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sum_ehf", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("max_ehf", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("tot_days", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("prop_days", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("avg_ehf", ogr.OFTReal))
    

    stat_stats = statistics[gcm][sc]
    for statn, stats in stat_stats.items():
        
        stat_values = stats[year]
        point = ogr.Geometry(ogr.wkbPoint)
        point.AddPoint(station_list[int(statn)][1], station_list[int(statn)][2])
        # Create the feature and set values
        featureDefn = layer.GetLayerDefn()
        outFeature = ogr.Feature(featureDefn)
        outFeature.SetGeometry(point)
        outFeature.SetField("ID", int(statn))
        outFeature.SetField("Name", station_list[int(statn)][0])
        outFeature.SetField("Latitude", station_list[int(statn)][1])
        outFeature.SetField("Longitude", station_list[int(statn)][2])
        outFeature.SetField("sumDaysSmr", stat_values[0])
        outFeature.SetField("sumDaysAut", stat_values[1])
        outFeature.SetField("sumDaysWnt", stat_values[2])
        outFeature.SetField("sumDaysSpr", stat_values[3])
        outFeature.SetField("sumEhfSmr", stat_values[4])
        outFeature.SetField("sumEhfAut", stat_values[5])
        outFeature.SetField("sumEhfWnt", stat_values[6])
        outFeature.SetField("sumEhfSpr", stat_values[7])
        outFeature.SetField("maxEhfSmr", stat_values[8])
        outFeature.SetField("maxEhfAut", stat_values[9])
        outFeature.SetField("maxEhfWnt", stat_values[10])
        outFeature.SetField("maxEhfSpr", stat_values[11])
        outFeature.SetField("totDaysSmr", stat_values[12])
        outFeature.SetField("totDaysAut", stat_values[13])
        outFeature.SetField("totDaysWin", stat_values[14])
        outFeature.SetField("totDaysSpr", stat_values[15])
        outFeature.SetField("PropDaySmr", stat_values[16])
        outFeature.SetField("PropDayAut", stat_values[17])
        outFeature.SetField("PropDayWin", stat_values[18])
        outFeature.SetField("PropDaySpr", stat_values[19])
        outFeature.SetField("avgEhfSmr", stat_values[20])
        outFeature.SetField("avgEhfAut", stat_values[21])
        outFeature.SetField("avgEhfWnt", stat_values[22])
        outFeature.SetField("avgEhfSpr", stat_values[23])
        outFeature.SetField("sum_days", stat_values[24])
        outFeature.SetField("sum_ehf", stat_values[25])
        outFeature.SetField("max_ehf", stat_values[26])
        outFeature.SetField("tot_days", stat_values[27])
        outFeature.SetField("prop_days", stat_values[28])
        outFeature.SetField("avg_ehf", stat_values[29])
        layer.CreateFeature(outFeature)
        # Destroy the feature to free resources
        outFeature.Destroy()
        
    # Destroy the data source to free resources
    data_source.Destroy()
    