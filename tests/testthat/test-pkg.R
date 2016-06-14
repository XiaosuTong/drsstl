library(Spaloess)
library(maps)
library(plyr)


test_that("local fit and pred", {

  new.grid <- expand.grid(
   lon = seq(-126, -67, by = 1),
   lat = seq(25, 49, by = 1)
  )
  instate <- ! is.na(map.where("state", new.grid$lon, new.grid$lat))
  new.grid <- new.grid[instate, ]

  elev.fit <- spaloess( elev ~ lon + lat,
   data = station_info,
   degree = 2,
   span = 0.05,
   distance = "Latlong",
   normalize = FALSE,
   napred = FALSE,
   alltree = FALSE,
   family = "symmetric",
   control = loess.control(surface = "interpolate")
  )
  grid.fit <- predloess(
    object = elev.fit,
    newdata = data.frame(
      lon = new.grid$lon,
      lat = new.grid$lat
    )
  )
  new.grid$elev <- grid.fit#


  #if the fitting results are in memory#

  #n <- length(unique(tmax_all$station.id)
  n <- 1000

  mcontrol <- spacetime.control(
   vari = "tmax", time = "date", n = 576, n.p = 12, stat_n = n, surf = "interpolate",
   s.window = 13, t.window = 241, degree = 2, span = 0.25, Edeg = 0,
   mthbytime = 1
  )

  set.seed(99)
  first_stations <- sample(unique(tmax_all$station.id), n)
  small_dt <- subset(tmax_all, station.id %in% first_stations)
  small_dt$station.id <- as.character(small_dt$station.id)
  small_dt$month <- as.character(small_dt$month)

  fitted <- drsstl(
   data = small_dt,
   output = NULL,
   model_control = mcontrol
  )
  we <- predNewLocs(
   fitted = fitted, newdata = new.grid, model_control = mcontrol
  )

  # prove it here!
  expect_equivalent(sum(is.na(we)), 0)

})

# nolint start
# load("Projects/Spatial/NCAR/RData/USinfo.RData") ##hathi
# names(UStinfo) <- c("station.id","elev","lon","lat")#
#
# load("Projects/Spatial/NCAR/RData/info.RData") ##gacrux#
#
# ccontrol <- mapreduce.control(
#  libLoc=.libPaths(), reduceTask=95, io_sort=128, slow_starts = 0.5,
#  map_jvm = "-Xmx200m", reduce_jvm = "-Xmx200m",
#  map_memory = 1024, reduce_memory = 1024,
#  reduce_input_buffer_percent=0.4, reduce_parallelcopies=10,
#  reduce_merge_inmem=0, task_io_sort_factor=100,
#  spill_percent=0.9, reduce_shuffle_input_buffer_percent = 0.8,
#  reduce_shuffle_merge_percent = 0.4
# )
#
# #if the fitting results are on HDFS#
# mcontrol <- spacetime.control(
#  vari="resp", time="date", n=576, n.p=12, stat_n=7738, surf="interpolate",
#  s.window=13, t.window = 241, degree=2, span=0.015, Edeg=2,
#  mthbytime = 1
# )
# fitted <- drsstl(
#  data = "/user/tongx/spatem/tmax.txt",
#  output = "/user/tongx/spatem/output",
#  stat_info="/user/tongx/spatem/station_info.RData",
#  model_control=mcontrol, cluster_control=ccontrol
# )#
#
# predNewLocs(
#  fitted="/user/tongx/spatem/output/output_bymth", newdata=new.grid, output = "/user/tongx/spatem",
#  stat_info="/user/tongx/spatem/station_info.RData", model_control = mcontrol, cluster_control = ccontrol
# )
# nolint end
