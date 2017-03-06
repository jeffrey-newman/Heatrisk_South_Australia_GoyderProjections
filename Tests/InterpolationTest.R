
# install.packages(c("ggplot2", "gstat", "sp", "maptools"))

library(ggplot2)
library(gstat)
library(sp)
library(maptools)
library(rgdal)

data <- readShapePoints("/Users/a1091793/Documents/Projects/Heatwave risk South Australia Goyder Projections/Heatrisk_South_Australia_GoyderProjections/test.shp")
data2 <- readOGR("/Users/a1091793/Documents/Projects/Heatwave risk South Australia Goyder Projections/Heatrisk_South_Australia_GoyderProjections", layer="test")
  
plot(data2)

x.range <- range(data2@coords[,1])
y.range <- range(data2@coords[,2])
grd <- expand.grid(x=seq(from=x.range[1], to=x.range[2], by=0.005), y=seq(from=y.range[1], to=y.range[2], by=0.005))
coordinates(grd) <- ~ x+y
gridded(grd) <- TRUE
plot(grd, cex=1.5)
points(data2, pch=1, col='red', cex=1)
title("Interpolation Grid and Sample Points")

idw<-idw(formula=avgEhfSmr ~ 1, locations=data2, newdata=grd)
idw.output=as.data.frame(idw)
names(idw.output)[1:3]<-c("long","lat","var1.pred")

plot<-ggplot(data=idw.output,aes(x=long,y=lat))#start with the base-plot 
layer1<-c(geom_tile(data=idw.output,aes(fill=var1.pred)))#then create a tile layer and fill with predicted values
plot+layer1+scale_fill_gradient(low="#FEEBE2", high="#7A0177")+coord_equal()

variogcloud<-variogram(avgEhfSmr~1, locations=data2, data=data2, cloud=TRUE)
plot(variogcloud)

semivariog<-variogram(avgEhfSmr~1, locations=data2, data=data2)
plot(semivariog)

interpolateAndMap <- function(shape_file){
    data <- readOGR(shape_file)
    x.range <- range(data@coords[,1])
    y.range <- range(data@coords,[,2])
    grd <- expand.grid(x=seq(from=x.range[1], to=x.range[2], by=0.005), y=seq(from=y.range[1], to=y.range[2], by=0.005))
    coordinates(grd) <- ~ x+y
    gridded(grd) <- TRUE
    plot(grd, cex=1.5)
    points(data2, pch=1, col='red', cex=1)
    title("Interpolation Grid and Sample Points")

    idw<-idw(formula=avgEhfSmr ~ 1, locations=data2, newdata=grd)
    idw.output=as.data.frame(idw)
    names(idw.output)[1:3]<-c("long","lat","var1.pred")

    plot<-ggplot(data=idw.output,aes(x=long,y=lat))#start with the base-plot
    layer1<-c(geom_tile(data=idw.output,aes(fill=var1.pred)))#then create a tile layer and fill with predicted values
    plot+layer1+scale_fill_gradient(low="#FEEBE2", high="#7A0177")+coord_equal()

    variogcloud<-variogram(avgEhfSmr~1, locations=data2, data=data2, cloud=TRUE)
    plot(variogcloud)

    semivariog<-variogram(avgEhfSmr~1, locations=data2, data=data2)
    plot(semivariog)
}