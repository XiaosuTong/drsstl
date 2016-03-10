#' Conduct the stlplus fitting at each location in parallel
#'
#' call stlplus function on time series at each location in parallel.
#' Every station uses the same smoothing parameter
#'
#' @param input
#'     The path of input sequence file on HDFS. It should be by location division.
#' @param output
#'     The path of output sequence file on HDFS. It is by location division but with seasonal and trend components
#' @param model_control
#'     The list contains all smoothing parameters
#' @param cluster_control
#'     A list contains all mapreduce tuning parameters.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     FileInput <- "/wsc/tongx/spatem/tmax/sim/bymth256"
#'     FileOutput <- "/wsc/tongx/spatem/tmax/sim/bymthfit256"
#'     me <- mapreduce.control(libLoc=lib.loc, BLK = 256)
#'     you <- spacetime.control(vari="resp", time="date", seaname="month", n=4448736, n.p=12, s.window=13, t.window = 241, degree=2, span=0.015, Edeg=2)
#'     \dontrun{
#'       spatialfit(FileInput, FileOutput, info="/wsc/tongx/spatem/stationinfo/UStinfo.RData", you, me)
#'     }


spatialfit <- function(input, output, target="resp", sub=1, infill=TRUE, model_control=spacetime.control(), cluster_control=mapreduce.control()) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
#      value <- map.values[[r]][, c("station.id", "date"), drop=FALSE]
#      value$remainder <- with(map.values[[r]], resp - trend - seasonal)
#      value <- inner_join(value, UStinfo, by="station.id")
#      value$elev2 <- log2(value$elev + 128)
      if(Mlcontrol$Edeg == 2) {
        fml <- as.formula(paste(target, "~ lon + lat + elev2"))
        dropSq <- FALSE
        condParam <- "elev2"
      } else if(Mlcontrol$Edeg == 1) {
        fml <- as.formula(paste(target, "~ lon + lat + elev2"))
        dropSq <- "elev2"
        condParam <- "elev2"
      } else if (Mlcontrol$Edeg == 0) {
        fml <- as.formula(paste(target, "~ lon + lat"))
        dropSq <- FALSE
        condParam <- FALSE
      }
      if (sub == 1) {
        value <- arrange(as.data.frame(map.values[[r]]), station.id)
        value <- cbind(value, a1950UStinfo[, c("lon","lat","elev")])
        value$elev2 <- log2(value$elev + 128)
        lo.fit <- spaloess( fml, 
          data    = v, 
          degree  = Mlcontrol$degree, 
          span    = Mlcontrol$span,
          para    = condParam,
          drop    = dropSq,
          family  = "symmetric",
          normalize = FALSE,
          distance = "Latlong",
          control = loess.control(surface = Mlcontrol$surf),
          napred = FALSE
        )
      } else {
        d_ply(
          .data = value,
          .vari = "date",
          .fun = function(v) {
            lo.fit <- spaloess( fml, 
              data    = v, 
              degree  = Mlcontrol$degree, 
              span    = Mlcontrol$span,
              para    = condParam,
              drop    = dropSq,
              family  = "symmetric",
              normalize = FALSE,
              distance = "Latlong",
              control = loess.control(surface = Mlcontrol$surf),
              napred = FALSE
            )
            v$spafit <- lo.fit$fitted
            #rhcollect(unique(v$date), v)
          }
        )
      }
      rhcollect(map.keys[[r]], length(map.values))
    })
  })
  job$parameters <- list(
    Mlcontrol = model_control,
    Clcontrol = cluster_control,
    info = info,
    sub = sub
  )
  job$shared <- c(info)
  job$setup <- expression(
    map = {
      load(strsplit(info, "/")[[1]][length(strsplit(info, "/")[[1]])])
      suppressMessages(library(plyr, lib.loc=Clcontrol$libLoc))
      suppressMessages(library(dplyr, lib.loc=Clcontrol$libLoc))
      library(Spaloess, lib.loc=Clcontrol$libLoc)
    }
  )
  job$mapred <- list(
    mapreduce.task.timeout = 0,
    mapreduce.job.reduces = 0,  #cdh5
    mapreduce.map.java.opts = "-Xmx3072m",
    mapreduce.map.memory.mb = 4096,
    dfs.blocksize = cluster_control$BLK,
    rhipe_map_bytes_read = 200*2^20,
    rhipe_map_buffer_size = 10000,
    mapreduce.map.output.compress = TRUE,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK"
  )
  job$input <- rhfmt(input, type="sequence")
  job$output <- rhfmt(output, type="sequence")
  job$mon.sec <- 10
  job$jobname <- output
  job$readback <- FALSE  

  job.mr <- do.call("rhwatch", job)  

}
