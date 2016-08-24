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
    
    layer.CreateField(ogr.FieldDefn("sum_heatwave_days_summer", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sum_heatwave_days_autumn", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sum_heatwave_days_winter", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sum_heatwave_days_spring", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sum_ehf_summer", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sum_ehf_autumn", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sum_ehf_winter", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("sum_ehf_spring", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("max_ehf_summer", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("max_ehf_autumn", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("max_ehf_winter", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("max_ehf_spring", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("tot_days_in_summer", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("tot_days_in_autumn", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("tot_days_in_winter", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("tot_days_in_spring", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("prop_days_in_summer", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("prop_days_in_autumn", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("prop_days_in_winter", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("prop_days_in_spring", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("avg_ehf_summer", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("avg_ehf_autumn", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("avg_ehf_winter", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("avg_ehf_spring", ogr.OFTReal))
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
        point.AddPoint(station_list[statn][2], station_list[statn][1])
        # Create the feature and set values
        featureDefn = layer.GetLayerDefn()
        outFeature = ogr.Feature(featureDefn)
        outFeature.SetGeometry(point)
        outFeature.SetField("sum_heatwave_days_summer", stat_values[0])
        outFeature.SetField("sum_heatwave_days_autumn", stat_values[1])
        outFeature.SetField("sum_heatwave_days_winter", stat_values[2])
        outFeature.SetField("sum_heatwave_days_spring", stat_values[3])
        outFeature.SetField("sum_ehf_summer", stat_values[4])
        outFeature.SetField("sum_ehf_autumn", stat_values[5])
        outFeature.SetField("sum_ehf_winter", stat_values[6])
        outFeature.SetField("sum_ehf_spring", stat_values[7])
        outFeature.SetField("max_ehf_summer", stat_values[8])
        outFeature.SetField("max_ehf_autumn", stat_values[9])
        outFeature.SetField("max_ehf_winter", stat_values[10])
        outFeature.SetField("max_ehf_spring", stat_values[11])
        outFeature.SetField("tot_days_in_summer", stat_values[12])
        outFeature.SetField("tot_days_in_autumn", stat_values[13])
        outFeature.SetField("tot_days_in_winter", stat_values[14])
        outFeature.SetField("tot_days_in_spring", stat_values[15])
        outFeature.SetField("prop_days_in_summer", stat_values[16])
        outFeature.SetField("prop_days_in_autumn", stat_values[17])
        outFeature.SetField("prop_days_in_winter", stat_values[18])
        outFeature.SetField("prop_days_in_spring", stat_values[19])
        outFeature.SetField("avg_ehf_summer", stat_values[20])
        outFeature.SetField("avg_ehf_autumn", stat_values[21])
        outFeature.SetField("avg_ehf_winter", stat_values[22])
        outFeature.SetField("avg_ehf_spring", stat_values[23])
        outFeature.SetField("sum_days", stat_values[24])
        outFeature.SetField("sum_ehf", stat_values[25])
        outFeature.SetField("max_ehf", stat_values[26])
        outFeature.SetField("tot_days", stat_values[27])
        outFeature.SetField("prop_days", stat_values[28])
        outFeature.SetField("avg_ehf", stat_values[29])
        layer.CreateFeature(outFeature)
        # Destroy the feature to free resources
        feature.Destroy()
        
    # Destroy the data source to free resources
    data_source.Destroy()
    