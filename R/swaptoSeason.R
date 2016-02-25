#' Swap to division by season
#'
#' Switch input key-value pairs which is division by location
#' to the key-value pairs which is division by season.
#'
#' @param input
#'     The path of input sequence file on HDFS. It should be by location.
#' @param output
#'     The path of output sequence file on HDFS. It is by season division.
#' @param reduceTask
#'     The number of the reduce tasks.
#' @param control
#'     all parameters that are needed for space-time fitting
#' @details
#'     swaptoSeason is used for switch division by location to division by season.
#'     The output files will be intput to parallel seasonal fitting of stlplus.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     FileInput <- "/ln/tongx/Spatial/tmp/tmax/a1950/bymonth.fit/symmetric/direct/2/sp0.015.bystation"
#'     FileOutput <- "/ln/tongx/Spatial/tmp/tmax/a1950/byseason"
#'     \dontrun{
#'       swaptoSeason(FileInput, FileOutput, reduceTask=10, Clcontrol=mapreduce.control(libLoc=.libPaths()))
#'     }

swaptoSeason <- function(input, output, Clcontrol) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      plyr::d_ply(
        .data = map.values[[r]],
        .variable = c("month"),
        .fun = function(k, station = map.keys[[r]]) {
          key <- c(station, unique(as.character(k$month)))
          value <- subset(k, select = -c(month))
          attr(value, "loc") <- attributes(map.values[[r]])$loc
          rhcollect(key, value)
      })
    })
  })
  job$parameters <- list(
    Clcontrol = Clcontrol
  )
  job$setup <- expression(
    map = {library(plyr, lib.loc=Clcontrol$libLoc)}
  )
  job$mapred <- list(
    mapred.reduce.tasks = Clcontrol$reduceTask,  #cdh3,4
    mapreduce.job.reduces = Clcontrol$reduceTask,  #cdh5
    mapred.tasktimeout = 0, #cdh3,4
    mapreduce.task.timeout = 0, #cdh5
    rhipe_reduce_buff_size = 10000
  )
  job$input <- rhfmt(input, type="sequence")
  job$output <- rhfmt(output, type="sequence")
  job$mon.sec <- 20
  job$jobname <- output
  job$readback <- FALSE  

  job.mr <- do.call("rhwatch", job)  

}

