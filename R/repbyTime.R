#' Swap to division by time
#'
#' Switch input key-value pairs which is division by location
#' to the key-value pairs which is division by time.
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
#'     FileInput <- "/wsc/tongx/Spatial/tmp/tmax/simulate/bystation.orig"
#'     FileOutput <- "/wsc/tongx/Spatial/tmp/tmax/simulate/bystation.small"
#'     \dontrun{
#'       repbyTime(FileInput, FileOutput, Rep=5800, me)
#'     }

repbyTime <- function(input, output, Rep=5800, control=mapreduce.control()){

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      Index <- which(is.na(map.values[[r]]$resp))
      map.values[[r]][Index, "resp"] <- map.values[[r]]$fitted[Index]
      value <- subset(arrange(map.values[[r]], year, match(month, month.abb)), select = -c(fitted, year))
      value <- rdply(Rep, value, .id=NULL)
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
    Rep = Rep
  )
  job$input <- rhfmt(input, type = "sequence")
  job$output <- rhfmt(output, type = "sequence")
  job$mapred <- list(
    mapreduce.output.fileoutputformat.compress.type = "BLOCK",
    mapred.reduce.tasks = control$reduceTask,  #cdh3,4
    mapreduce.job.reduces = control$reduceTask,  #cdh5
    mapreduce.task.io.sort.mb = control$io_sort,
    mapreduce.map.sort.spill.percent = control$spill_percent,
    mapreduce.reduce.shuffle.parallelcopies = control$parallelcopies,
    mapreduce.reduce.merge.inmem.threshold = control$reduce_merge_inmem,
    mapreduce.reduce.input.buffer.percent = control$reduce_input_buffer,
    mapreduce.task.timeout = 0
  )
  job$mon.sec <- 10
  job$jobname <- output  
  job$readback <- FALSE
  job.mr <- do.call("rhwatch", job)

}