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
#'     FileInput <- "/ln/tongx/Spatial/a1950/bystation"
#'     FileOutput <- "/ln/tongx/Spatial/a1950/byseason"
#'     \dontrun{
#'       swaptoSeason(FileInput, FileOutput, control=spacetime.control(libLoc=.libPaths()))
#'     }

swaptoSeason <- function(input, output, reduceTask=0, control=spacetime.control()) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      d_ply(
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
    control = control
  )
  job$setup <- expression(
    map = {library(plyr, lib.loc=control$libLoc)}
  )
  job$mapred <- list(
    mapred.reduce.tasks = reduceTask,  #cdh3,4
    mapreduce.job.reduces = reduceTask,  #cdh5
    mapred.tasktimeout = 0, #cdh3,4
    mapreduce.task.timeout = 0, #cdh5
    rhipe_reduce_buff_size = 10000
  )
  job$combiner <- TRUE
  job$input <- rhfmt(input, type="sequence")
  job$output <- rhfmt(output, type="sequence")
  job$mon.sec <- 20
  job$jobname <- output
  job$readback <- FALSE  

  job.mr <- do.call("rhwatch", job)  

}

