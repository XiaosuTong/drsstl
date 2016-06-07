# nolint start
##install_github("XiaosuTong/Spaloess")
#source("Rhipe/hathi.initial.R")
#library(drSpaceTime)
#library(Spaloess)
#library(plyr)
#library(stlplus)
#library(maps)#

#load("Projects/Spatial/NCAR/RData/USinfo.RData") ##hathi
#names(UStinfo) <- c("station.id","elev","lon","lat")#

#load("Projects/Spatial/NCAR/RData/info.RData") ##gacrux#
#

#ccontrol <- mapreduce.control(
#  libLoc=.libPaths(), reduceTask=95, io_sort=128, slow_starts = 0.5,
#  map_jvm = "-Xmx200m", reduce_jvm = "-Xmx200m",
#  map_memory = 1024, reduce_memory = 1024,
#  reduce_input_buffer_percent=0.4, reduce_parallelcopies=10,
#  reduce_merge_inmem=0, task_io_sort_factor=100,
#  spill_percent=0.9, reduce_shuffle_input_buffer_percent = 0.8,
#  reduce_shuffle_merge_percent = 0.4
#)
#new.grid <- expand.grid(
#  lon = seq(-126, -67, by = 0.5),
#  lat = seq(25, 49, by = 0.5)
#)
#instate <- !is.na(map.where("state", new.grid$lon, new.grid$lat))
#new.grid <- new.grid[instate, ]
#elev.fit <- spaloess( elev ~ lon + lat,
#  data = UStinfo,
#  degree = 2,
#  span = 0.015,
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
#new.grid$elev <- grid.fit#
#

##if the fitting results are in memory#

#mcontrol <- spacetime.control(
#  vari="tmax", time="date", n=576, n.p=12, stat_n=7738, surf="interpolate",
#  s.window=13, t.window = 241, degree=2, span=0.015, Edeg=2,
#  statbytime = 1
#)
#fitted <- drsstl(
#  data=tmax_all,
#  output=NULL,
#  stat_info="station_info",
#  model_control=mcontrol
#)
#we <- predNewLocs(
#  fitted = fitted, newdata = new.grid, model_control = mcontrol
#)#
#

##if the fitting results are on HDFS#

#mcontrol <- spacetime.control(
#  vari="resp", time="date", n=576, n.p=12, stat_n=7738, surf="interpolate",
#  s.window=13, t.window = 241, degree=2, span=0.015, Edeg=2,
#  statbytime = 1
#)
#fitted <- drsstl(
#  data = "/user/tongx/spatem/tmax.txt",
#  output = "/user/tongx/spatem/output",
#  stat_info="/user/tongx/spatem/station_info.RData",
#  model_control=mcontrol, cluster_control=ccontrol
#)#

#predNewLocs(
#  fitted="/user/tongx/spatem/output/output_bymth", newdata=new.grid, output = "/user/tongx/spatem",
#  stat_info="/user/tongx/spatem/station_info.RData", model_control = mcontrol, cluster_control = ccontrol
#)

# nolint end
