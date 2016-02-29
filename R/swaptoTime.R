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
#'     FileInput <- "/wsc/tongx/Spatial/tmp/tmax/simulate/bystation.small"
#'     FileOutput <- "/wsc/tongx/Spatial/tmp/tmax/simulate/bymonth"
#'     me <- mapreduce.control(libLoc="/home/tongx/R_LIBS", io_sort=512)
#'     \dontrun{
#'       swaptoTime(FileInput, FileOutput, me)
#'     }

swaptoTime <- function(input, output, control=mapreduce.control()) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      for(i in 1:nrow(map.values[[r]])) {
        key <- c(map.values[[r]]$date[i], as.character(map.values[[r]]$month[i]))
        value <- subset(map.values[[r]][i, ], select = -c(date, month))
        value$station.id <- map.keys[[r]]
        value$lon <- as.numeric(attributes(map.values[[r]])$loc[1])
        value$lat <- as.numeric(attributes(map.values[[r]])$loc[2])
        value$elev2 <- as.numeric(attributes(map.values[[r]])$loc[3])
        rhcollect(key, value)
      }
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
  job$setup <- expression(
    map = {library(plyr, lib.loc=control$libLoc)}
  )
  job$parameters <- list(
    control = control
  )
  job$mapred <- list(
    mapred.reduce.tasks = control$reduceTask,  #cdh3,4
    mapreduce.job.reduces = control$reduceTask,  #cdh5
    mapreduce.task.io.sort.mb = control$io_sort,
    mapreduce.map.sort.spill.percent = control$spill_percent,
    mapreduce.reduce.shuffle.parallelcopies = control$parallelcopies,
    mapreduce.reduce.merge.inmem.threshold = control$reduce_merge_inmem,
    mapreduce.reduce.input.buffer.percent = control$reduce_input_buffer,
    mapreduce.task.timeout  = 0,
    rhipe_reduce_buff_size = control$reduce_buff_size
 )
  job$combiner <- TRUE
  job$input <- rhfmt(input, type="sequence")
  job$output <- rhfmt(output, type="sequence")
  job$mon.sec <- 10
  job$jobname <- output
  job$readback <- FALSE  

  job.mr <- do.call("rhwatch", job)  

}

