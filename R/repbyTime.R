#' Swap to division by time
#'
#' Switch input key-value pairs which is division by location
#' to the key-value pairs which is division by time.
#'
#' @param input
#'     The path of input sequence file on HDFS. It should be by location division.
#' @param output
#'     The path of output sequence file on HDFS. It is by location division but duplicate the time series to be longer.
#' @param reduceTask
#'     The number of the reduce tasks.
#' @param Rep
#'     The replication time for the time series of each location
#' @param buffSize
#'     To control how many location will be send to one mapper
#' @details
#'     swaptoTime is used for switch division by location to division by time.
#' @author 
#'     Xiaosu Tong 
#' @export

repbyTime <- function(input, output, Rep=8000, buffSize=7, reduceTask, control=spacetime.control()){

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      value <- rdply(Rep, arrange(map.values[[r]], year, match(month, month.abb)), .id=NULL)
      value$date <- 1:nrow(value)
      rhcollect(map.keys[[r]], value)
    })
  })
  job$setup <- expression(
    map = {library(plyr, lib.loc=control$libLoc)}
  )
  job$parameters <- list(
    control = control,
    Rep = Rep
  )
  job$input <- rhfmt(input, type = "sequence")
  job$output <- rhfmt(output, type = "sequence")
  job$mapred <- list(
    mapred.reduce.tasks = reduceTask,  #cdh3,4
    mapreduce.job.reduces = reduceTask,  #cdh5
    rhipe_map_buff_size = buffSize
  )
  job$mon.sec <- 20
  job$jobname <- output  
  job$readback <- FALSE
  job.mr <- do.call("rhwatch", job)

}