#' Swap to division by time
#'
#' Switch input key-value pairs which is division by location
#' to the key-value pairs which is division by time.
#'
#' @param input
#'     The path of input sequence file on HDFS. It should be by location division.
#' @param output
#'     The path of output sequence file on HDFS. It is by time division.
#' @param reduceTask
#'     The number of the reduce tasks.
#' @param elevFlag
#'     Logical argument, if TRUE, then the elevation attribute from input value data.frame is log2 transformation
#'     If FALSE, a log2 transformation is added to the elevation attribute.
#' @param control
#'     all parameters that are needed for space-time fitting
#' @details
#'     swaptoTime is used for switch division by location to division by time.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     FileInput <- "/ln/tongx/Spatial/a1950/bystation"
#'     FileOutput <- "/ln/tongx/Spatial/a1950/bymonth"
#'     \dontrun{
#'       swaptoTime(FileInput, FileOutput, elevFlag=TRUE)
#'     }

swaptoTime <- function(input, output, elevFlag=TRUE, reduceTask=1, control=spacetime.control()) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      plyr::d_ply(
        .data = map.values[[r]],
        .variable = c("year","month"),
        .fun = function(k, station = map.keys[[r]]) {
          key <- c(unique(k$year), unique(as.character(k$month)))
          value <- subset(k, select = -c(year, month))
          value$station.id <- station
          value$lon <- as.numeric(attributes(map.values[[r]])$loc[1])
          value$lat <- as.numeric(attributes(map.values[[r]])$loc[2])
          if (!elevFlag) {
            value$elev2 <- as.numeric(attributes(map.values[[r]])$loc[3])
          } else {
            value$elev2 <- log2(as.numeric(attributes(map.values[[r]])$loc[3]) + 128)
          }
          rhcollect(key, value)
      })
    })
  })
  job$reduce <- expression(
    pre = {
      combine <- data.frame()
    },
    reduce = {
      combine <- rbind(combine, do.call(rbind, reduce.values))
    },
    post = {
      rhcollect(reduce.key, combine)
    }
  )
  job$parameters <- list(
    control = control,
    elevFlag = elevFlag
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
  job$mon.sec <- 5
  job$jobname <- output
  job$readback <- FALSE  

  job.mr <- do.call("rhwatch", job)  

}

