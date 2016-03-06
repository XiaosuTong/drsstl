#' Replicate the data in time dimension.
#'
#' For simulation purpose to enlarge the data for each location.
#'
#' @param input
#'     The path of input sequence file on HDFS. It should be by location division.
#' @param output
#'     The path of output sequence file on HDFS. It is by location division but duplicate the time series to be longer.
#' @param Rep
#'     The replication time for the time series of each location
#' @param control
#'     A list contains all mapreduce tuning parameters.
#' @details
#'     repbyoTime is used for duplicate the time series at each location.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     FileInput <- "/wsc/tongx/spatem/tmax/sim/bystation.orig"
#'     FileOutput <- "/wsc/tongx/spatem/tmax/sim/bystation"
#'     me <- mapreduce.control(libLoc=lib.loc, BLK = 256)
#'     \dontrun{
#'       repbyTime(FileInput, FileOutput, Srep=18.5, me) 
#'     }

repbyTime <- function(input, output, Srep, control=mapreduce.control()){

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      Index <- which(is.na(map.values[[r]]$resp))
      map.values[[r]][Index, "resp"] <- map.values[[r]]$fitted[Index]
      value <- subset(arrange(map.values[[r]], year, match(month, month.abb)), select = -c(fitted, year))
      Rep <- floor(round(2^Srep)/48)
      value <- rdply(Rep, value, .id=NULL)
      value <- rbind(value, tail(value, (round(2^Srep) - Rep*48)*12))
      value$date <- 1:nrow(value)
      value$month <- match(value$month, month.abb)
      row.names(value) <- NULL
      rhcollect(map.keys[[r]], value)
    })
  })
  job$setup <- expression(
    map = {suppressMessages(library(plyr, lib.loc=control$libLoc))}
  )
  job$parameters <- list(
    control = control,
    Srep = Srep
  )
  job$input <- rhfmt(input, type = "sequence")
  job$output <- rhfmt(output, type = "sequence")
  job$mapred <- list(
    mapreduce.map.java.opts = "-Xmx3072m",
    mapreduce.map.memory.mb = 4096, 
    mapreduce.job.reduces = control$reduceTask,  #cdh5
    dfs.blocksize = control$BLK
  )
  job$mon.sec <- 10
  job$jobname <- output  
  job$readback <- FALSE
  job.mr <- do.call("rhwatch", job)

}