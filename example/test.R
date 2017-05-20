library(maps)
library(Spaloess)
library(stlplus)
library(datadr)
library(plyr)
source("example/spacetime.control.R")
source('example/drsstl.R')
source('example/predict.drsstl.R')

#load("data/tmax_all.rda")
#load("data/station_info.rda")
#n <- 500
#set.seed(99)
#first_stations <- sample(unique(tmax_all$station.id), n)
#small_dt <- subset(tmax_all, station.id %in% first_stations)
#small_dt$station.id <- as.character(small_dt$station.id)
#small_dt$month <- match(as.character(small_dt$month), month.abb)
#small_dt <- arrange(small_dt, year, month)
#small_dt$monthidx <- rep(1:576, each = n)
#small_dt <- subset(small_dt, monthidx <= 15*12)
#new.grid <- expand.grid(
#  lon = seq(-126, -67, by = 3),
#  lat = seq(25, 49, by = 3)
#)
#instate <- !is.na(map.where("state", new.grid$lon, new.grid$lat))
#new.grid <- new.grid[instate, ]
#elev.fit <- spaloess( elev ~ lon + lat,
#  data = station_info,
#  degree = 2,
#  span = 0.05,
#  distance = "Latlong",
#  normalize = FALSE,
#  napred = FALSE,
#  alltree = FALSE,
#  family="symmetric",
#  control=loess.control(surface = "direct")
#)
#grid.fit <- predloess(
#  object = elev.fit,
#  newdata = data.frame(
#    lon = new.grid$lon,
#    lat = new.grid$lat
#  )
#)
#new.grid$elev <- grid.fit
#save(small_dt, new.grid, file = 'example.rda')


load('example/example.rda')

mcontrol <- spacetime.control(
    time = 'monthidx', y = 'tmax', loc = 'station.id', 
    space = c('lon', 'lat', 'elev'), n.p = 12, 
    loc.n = 500, surf = "interpolate"
)

rst1 <- drsstl(
    data = small_dt, s.window = 7, s.degree = 1, t.window = 81, t.degree = 1, lonlat.deg = c(2, 2), 
    elev.transform = TRUE, elev.deg = 2, space.span = c(0.2, 0.1), sstl.control = mcontrol
)
rst2 <- drsstl(
    data = small_dt, s.window = 7, s.degree = 1, t.window = 111, t.degree = 1, lonlat.deg = c(2, 2), 
    elev.transform = TRUE, elev.deg = 1, space.span = c(0.3, 0.2), sstl.control = mcontrol
)

rst3 <- predict.drsstl(
    original = small_dt, newdata = new.grid, s.window = 13, s.degree = 1, 
    t.window = 81, t.degree = 1, lonlat.deg = c(2, 2), elev.transform = TRUE,
    elev.deg = 2, space.span = c(0.2, 0.1), sstl.control = mcontrol
)

rst4 <- predict.drsstl(
    original = small_dt, newdata = new.grid, s.window = 13, s.degree = 1, 
    t.window = NULL, t.degree = 1, lonlat.deg = c(2, 2), elev.transform = TRUE,
    elev.deg = 2, space.span = c(0.2, 0.1), sstl.control = mcontrol
)