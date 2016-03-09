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
#'     FileInput <- "/wsc/tongx/spatem/tmax/sim/bystatfit512"
#'     FileOutput <- "/wsc/tongx/spatem/tmax/sim/byyear"
#'     me <- mapreduce.control(libLoc=lib.loc, io_sort=512, BLK=256, reduce_input_buffer_percent=0.7, reduce_merge_inmem=0, task_io_sort_factor=100, spill_percent=1)
#'     \dontrun{
#'       swaptoTimeMap(FileInput, FileOutput, me)
#'       swaptoTimeRed(FileOutput, "/wsc/tongx/spatem/tmax/sim/byyr256test", me)
#'     }
swaptoTime <- function(input, output){


}

swaptoTimeMap <- function(input, output, control=mapreduce.control()) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      value <- map.values[[r]]
      value$year <- ceiling(value$date/12)
      value$station.id <- map.keys[[r]]
      d_ply(
        .data = value,
        .vari = "year",
        .fun = function(k) {
          rhcollect(unique(k$year), subset(k, select = -c(year)))
      })
    })
  })
  job$setup <- expression(
    map = {library(plyr, lib.loc=control$libLoc)}
  )
  job$parameters <- list(
    control = control
  )
  job$mapred <- list(
    mapreduce.job.reduces = 0,  #cdh5
    mapreduce.task.timeout  = 0,
    dfs.blocksize = control$BLK,
    rhipe_map_bytes_read = 100*2^20,
    mapreduce.map.java.opts = "-Xmx3072m",
    mapreduce.map.memory.mb = 5120
  )
  job$combiner <- TRUE
  job$input <- rhfmt(input, type="sequence")
  job$output <- rhfmt(output, type="sequence")
  job$mon.sec <- 10
  job$jobname <- output
  job$readback <- FALSE  

  job.mr <- do.call("rhwatch", job)  

}


swaptoTimeRed <- function(input, output, control=mapreduce.control()) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      rhcollect(map.keys[[r]], map.values[[r]])
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
    mapreduce.job.reduces = 1000,  #cdh5
    mapreduce.task.io.sort.mb = control$io_sort,
    mapreduce.map.sort.spill.percent = control$spill_percent,
    mapreduce.reduce.shuffle.parallelcopies = control$parallelcopies,
    mapreduce.reduce.merge.inmem.threshold = control$reduce_merge_inmem,
    mapreduce.reduce.input.buffer.percent = control$reduce_input_buffer,
    mapreduce.task.io.sort.factor = control$task_io_sort_factor,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK",
    mapreduce.task.timeout  = 0,
    rhipe_reduce_buff_size = 10000,
    dfs.blocksize = control$BLK,
    #rhipe_map_bytes_read = 100*2^20,
    rhipe_map_buff_size = 10000,
    mapreduce.map.java.opts = "-Xmx3072m",
    mapreduce.map.memory.mb = 5120,
    rhipe_reduce_bytes_read = 1024*2^20,
    mapreduce.reduce.java.opts = "-Xmx3584m",
    mapreduce.reduce.memory.mb = 5120,
    mapreduce.job.reduce.slowstart.completedmaps = 0.8   
  )
  job$combiner <- TRUE
  job$input <- rhfmt(input, type="sequence")
  job$output <- rhfmt(output, type="sequence")
  job$mon.sec <- 10
  job$jobname <- output
  job$readback <- FALSE  

  job.mr <- do.call("rhwatch", job)  

}