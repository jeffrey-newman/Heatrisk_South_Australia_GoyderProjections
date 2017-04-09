import os

# os.environ['R_HOME'] = '/Users/a1091793/anaconda/lib/R'
# os.environ['R_HOME'] = '/Library/Frameworks/R.framework/Versions/3.3/Resources'

import rpy2.robjects as robjects
import rpy2.robjects.packages as rpackages

robjects.r('''Sys.setenv(LANG = "en")''')
# robjects.r('''Sys.setenv(PATH=paste(Sys.getenv("PATH"),"/Users/a1091793/anaconda/envs/py35/bin",sep=":"))''')

base = rpackages.importr('base')
utils = rpackages.importr('utils')

utils.chooseCRANmirror(ind=1)

packnames = ('gstat', 'sp', 'maptools', 'rgdal', 'raster', 'automap')
# R vector of strings
from rpy2.robjects.vectors import StrVector



# Selectively install what needs to be install.
# We are fancy, just because we can.
names_to_install = [x for x in packnames if not rpackages.isinstalled(x)]
if len(names_to_install) > 0:
    utils.install_packages(StrVector(names_to_install))

gstats = rpackages.importr('gstat')
sp = rpackages.importr('sp')
maptools = rpackages.importr('maptools')
rgdal = rpackages.importr('rgdal')
raster = rpackages.importr('raster')
autotools = rpackages.importr('automap')

loadAndTransformData = robjects.r('''
                                  loadAndTransformData <- function(shape_dir, layer)
                                  {
                                    data = readOGR(shape_dir, layer)
                                    data2 = spTransform(data, CRS("+init=epsg:28353") )
                                    return(data2)
                                  }
                                  ''')



genGrid = robjects.r('''
                        genGrid <- function(data, sink_file)
                        {

                          coords = data@coords
                          x_coords = coords[,1]
                          y_coords = coords[,2]
                          x_range = range(x_coords)
                          y_range = range(y_coords)
                          grd = expand.grid(x=seq(from=x_range[1], to=x_range[2], by=100), y=seq(from=y_range[1], to=y_range[2], by=100))
                          coordinates(grd) <- ~ x + y
                          gridded(grd) <- TRUE
                          proj4string(grd) = CRS(proj4string(data))

                          return(grd)
                        }
                         ''')

interpAndSave = robjects.r('''
                            interpAndSave <- function(relatn_formula, data, grd, mask_lyr, file_name_idw_2, file_name_idw_3, file_name_krig, autokrig_pdf, sink_file)
                            {

                              idw2 <- idw(formula=relatn_formula, locations = data, newdata = grd, idp = 2)
                              proj4string(idw2) <- CRS("+init=epsg:28353")

                              idw2.output = as.data.frame(idw2)
                              names(idw2.output)[1:3] <- c("long", "lat", "var1.pred")

                              r2 <- raster(idw2)
                              r2.m <- mask(r2, mask_lyr)
                              writeRaster(r2.m, file_name_idw_2, "GTiff", overwrite=TRUE)

                              idw3 <- idw(formula=relatn_formula, locations = data, newdata = grd, idp = 3)
                              proj4string(idw3) <- CRS("+init=epsg:28353")

                              idw3.output = as.data.frame(idw3)
                              names(idw3.output)[1:3] <- c("long", "lat", "var1.pred")

                              r3 <- raster(idw3)
                              r3.m <- mask(r3, mask_lyr)
                              writeRaster(r3.m, file_name_idw_3, "GTiff", overwrite=TRUE)

                              krig <- autoKrige(formula=relatn_formula, data, grd)
                              png(autokrig_pdf)
                              plot(krig)
                              dev.off()
                              krig.pred <- krig$krige_output
                              r_krig <- raster(krig.pred)
                              r_krig.m <- mask(r_krig, mask_lyr)
                              writeRaster(r_krig.m, file_name_krig, "GTiff", overwrite=TRUE)
                            }
                              ''')

# doTheWork = robjects.r('''
#                         dothework <- function(shape_dir, layer, fmla)
#                         {
#
#                           data = loadAndTransformData(shape_dir, layer)
#                           grd = genGrid(data, sinkfile)
#                           mask = readOGR("/Volumes/Samsung_T3/heatwave", "SA")
#                           # fmla = avgEhfSmr ~ 1
#
#                           file_name_idw_2 <- "idw2.tiff"
#                           file_name_idw_3 <- "idw3.tiff"
#                           file_name_krig <- "krig.tiff"
#                           autokrig_pdf <- "krig.pdf"
#
#                           interpAndSave(fmla, data, grd, mask, file_name_idw_2, file_name_idw_3, file_name_krig, autokrig_pdf, sinkfile)
#                         }
#                         ''')

readogr = robjects.r['readOGR']
sink = robjects.r['sink']


mask_dir = r"/fast/users/a1091793/Heatwave"
mask_layer = "SA"
mask = readogr(mask_dir, mask_layer)

def interpolateAndMap(timeslice_info):
    # os.environ['R_HOME'] = '/Library/Frameworks/R.framework/Versions/3.3/Resources'
    msg = "Interpolating map"
    print (msg, timeslice_info)
    os.chdir(timeslice_info[3])

    base_file_name = str(timeslice_info[0]) + "_" + str(timeslice_info[1]) + "_" + str(timeslice_info[2])
    file_name_idw_2 = base_file_name + "_idw2.tif"
    file_name_idw_3 = base_file_name + "_idw3.tif"
    file_name_krig = base_file_name + "_autokrig.tif"
    autokrig_pdf = base_file_name + "_autokrig.png"
    sink_file = base_file_name + "_sink.txt"

    sink(sink_file)
    data = loadAndTransformData(timeslice_info[3], timeslice_info[4])
    grd = genGrid(data, sink_file)

    fmla = robjects.Formula('avgEhfSmr ~ 1')
    interpAndSave(fmla, data, grd, mask, file_name_idw_2, file_name_idw_3, file_name_krig, autokrig_pdf, sink_file)


    #
    # doTheWork(timeslice_info[3], timeslice_info[4], fmla)
    #
    #
    # data =
    # range = robjects.r['range']
    # coords = data.slots['coords']
    # x_coords = coords.rx(True,1)
    # y_coords = coords.rx(True,2)
    # x_range = range(x_coords)
    # y_range = range(y_coords)
    # expand_grid = robjects.r['expand.grid']
    # seq = robjects.r['seq']
    # grd = expand_grid(x=seq(x_range[0], x_range[1], 0.005), y=seq(y_range[0], y_range[1], 0.005))
    # grd2 = assigncoord(grd)
    # # idw_cmd = robjects.r['idw']
    #
    #
    #
    #
    # interpAndSave(fmla, data, grd2, file_name_idw_2, file_name_idw_3, file_name_krig, autokrig_pdf, sink_file)

    rm = robjects.r['rm']
    rm('data')
    rm('grd')
    rm('fmla')
    sink()

    return timeslice_info















