#' Conduct the stlplus fitting at each location in parallel
#'
#' call stlplus function on time series at each location in parallel.
#' Every station uses the same smoothing parameter
#'
#' @param input
#'     The path of input sequence file on HDFS. It should be by location division.
#' @param output
#'     The path of output sequence file on HDFS. It is by location division but with seasonal and trend components
#' @param info
#'     the RData path on HDFS which contains all station metadata
#' @param target
#'     the variable name for the spatial fit, either "resp" or "remainder"
#' @param sub 
#'     integer, specifies how many month records are in each input key-value pair. 
#' @param model_control
#'     The list contains all smoothing parameters
#' @param cluster_control
#'     A list contains all mapreduce tuning parameters.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     FileInput <- "/wsc/tongx/spatem/tmax/sim/bymth256"
#'     FileOutput <- "/wsc/tongx/spatem/tmax/sim/bymthfit512"
#'     me <- mapreduce.control(libLoc="/home/tongx/R_LIBS", BLK = 512)
#'     you <- spacetime.control(vari="resp", time="date", seaname="month", n=4448736, n.p=12, s.window=13, t.window = 241, degree=2, span=0.015, Edeg=2)
#'     \dontrun{
#'       spatialfit(FileInput, FileOutput, target=you$vari,na=TRUE, sub=1, info="/wsc/tongx/spatem/stationinfo/a1950UStinfo.RData", model_control=you, cluster_control=me)
#'     }


spatialfit <- function(input, output, info, na = TRUE, target="resp", sub=1, model_control=spacetime.control(), cluster_control=mapreduce.control()) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
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
          data    = value, 
          degree  = Mlcontrol$degree, 
          span    = Mlcontrol$span,
          para    = condParam,
          drop    = dropSq,
          family  = "symmetric",
          normalize = FALSE,
          distance = "Latlong",
          control = loess.control(surface = "direct"),
          napred = na
        )
        if(na) {
          indx <- which(!is.na(value[, target]))
          rst <- rbind(cbind(indx, fitted=lo.fit$fitted), cbind(which(is.na(value[, target])), fitted=lo.fit$pred$fitted))
          rst <- arrange(as.data.frame(rst), indx)
          rhcollect(map.keys[[r]], rst$fitted)
        } else {
          rhcollect(map.keys[[r]], lo.fit$fitted)
        }
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
              napred = na
            )
            rhcollect(unique(v$date), lo.fit$fitted)
          }
        )
      }
    })
  })
  job$parameters <- list(
    Mlcontrol = model_control,
    Clcontrol = cluster_control,
    info = info,
    sub = sub,
    na = na,
    target = target
  )
  job$shared <- c(info)
  job$setup <- expression(
    map = {
      load(strsplit(info, "/")[[1]][length(strsplit(info, "/")[[1]])])
      suppressMessages(library(plyr, lib.loc=Clcontrol$libLoc))
      library(Spaloess, lib.loc=Clcontrol$libLoc)
    }
  )
  job$mapred <- list(
    mapreduce.task.timeout = 0,
    mapreduce.job.reduces = 0,  #cdh5
    mapreduce.map.java.opts = "-Xmx3584m",
    mapreduce.map.memory.mb = 5120, 
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
