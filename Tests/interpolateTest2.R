library(gstat)
library(sp)
library(maptools)
library(raster)
library(automap)
library(rgdal)

loadAndTransformData <- function(shape_dir, layer)
{
  data = readOGR(shape_dir, layer)
  data2 = spTransform(data, CRS("+init=epsg:28353") )
  return(data2)
}

genGrid <- function(data, sink_file)
{
  sink(sink_file)
  coords = data@coords
  x_coords = coords[,1]
  y_coords = coords[,2]
  x_range = range(x_coords)
  y_range = range(y_coords)
  grd = expand.grid(x=seq(from=x_range[1], to=x_range[2], by=100), y=seq(from=y_range[1], to=y_range[2], by=100))
  coordinates(grd) <- ~ x + y
  gridded(grd) <- TRUE
  proj4string(grd) = CRS(proj4string(data))
  sink()
  return(grd)
}

interpAndSave <- function(relatn_formula, data, grd, mask_lyr, file_name_idw_2, file_name_idw_3, file_name_krig, autokrig_pdf, sink_file)
{
  sink(sink_file)
  # data2 = data
  # grd2 = grd
  
  idw2 <- idw(formula=relatn_formula, locations = data, newdata = grd, idp = 2)
  proj4string(idw2) <- CRS("+init=epsg:28353")
  # outfilename <- tempfile(pattern="file", tmpdir = tempdir())
  # writeGDAL(idw2, "idw2_v2.tif", drivername = "GTiff")
  
  idw2.output = as.data.frame(idw2)
  names(idw2.output)[1:3] <- c("long", "lat", "var1.pred")
  
  r2 <- raster(idw2)
  r2.m <- mask(r2, mask_lyr)
  # plot(r2)
  writeRaster(r2.m, file_name_idw_2, "GTiff", overwrite=TRUE)
  
  idw3 <- idw(formula=relatn_formula, locations = data, newdata = grd, idp = 3)
  proj4string(idw3) <- CRS("+init=epsg:28353")
  # outfilename <- tempfile(pattern="file", tmpdir = tempdir())
  # writeGDAL(idw, "idw3_v2.tif", drivername = "GTiff")
  # file.rename (outfilename, "idw.tif")
  
  # idw.output = as.data.frame(idw)
  idw3.output = as.data.frame(idw3)
  names(idw3.output)[1:3] <- c("long", "lat", "var1.pred")
  
  r3 <- raster(idw3)
  r3.m <- mask(r3, mask_lyr)
  # plot(r3)
  writeRaster(r3.m, file_name_idw_3, "GTiff", overwrite=TRUE)
  
  krig <- autoKrige(formula=relatn_formula, data, grd)
  pdf(autokrig_pdf)
  plot(krig)
  dev.off()
  krig.pred <- krig$krige_output
  r_krig <- raster(krig.pred)
  r_krig.m <- mask(r_krig, mask_lyr)
  writeRaster(r_krig.m, file_name_krig, "GTiff", overwrite=TRUE)
  sink()
}

dothework <- function(shape_dir, layer, fmla)
{
  sinkfile <- "sink.txt"
  data = loadAndTransformData(shape_dir, layer)
  grd = genGrid(data, sinkfile)
  mask = readOGR("/Volumes/Samsung_T3/heatwave", "SA")
  # fmla = avgEhfSmr ~ 1
  
  file_name_idw_2 <- "idw2.tiff"
  file_name_idw_3 <- "idw3.tiff"
  file_name_krig <- "krig.tiff"
  autokrig_pdf <- "krig.pdf"
  
  interpAndSave(fmla, data, grd, mask, file_name_idw_2, file_name_idw_3, file_name_krig, autokrig_pdf, sinkfile)
}
 
shape_dir <- "/Volumes/Samsung_T3/heatwave/Processed/spaceDomain/csiro.mk36/his"
layer <- "csiro.mk36_his_1961"
fmla = avgEhfSmr ~ 1

dothework(shape_dir, layer, fmla)