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
#'     FileInput <- "/wsc/tongx/spatem/tmax/simulate/bystatfit"
#'     FileOutput <- "/wsc/tongx/spatem/tmax/simulate/bytmp"
#'     FileOutput <- "/wsc/tongx/spatem/tmax/simulate/bymonth.stlfit"
#'     me <- mapreduce.control(libLoc=lib.loc, io_sort=100)
#'     \dontrun{
#'       swaptoYear(FileInput, FileOutput, me)
#'     }

swaptoYear <- function(input, output, control=mapreduce.control()) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      map.values[[r]]$year <- ceiling(map.values[[r]]$date/12)
      d_ply(
        .data = map.values[[r]],
        .vari = "year",
        .fun = function(k) {
          rhcollect(c(map.keys[[r]], unique(k$year)), subset(k, select = -c(year)))
      })
    })
  })
#  job$reduce <- expression(
#    pre = {
#      combine <- data.frame()
#    },
#    reduce = {
#      combine <- rbind(combine, do.call(rbind, reduce.values))
#    },
#    post = {
#      rhcollect(reduce.key, combine)
#    }
#  )
  job$setup <- expression(
    map = {library(plyr, lib.loc=control$libLoc)}
  )
  job$parameters <- list(
    control = control
  )
  job$mapred <- list(
    #mapred.reduce.tasks = control$reduceTask,  #cdh3,4
    mapreduce.job.reduces = 0, #control$reduceTask,  #cdh5
    dfs.blocksize = 512*2^20,
    mapreduce.map.java.opts = "-Xmx3072m",
    mapreduce.map.memory.mb = 4096 
 )
  job$combiner <- TRUE
  job$input <- rhfmt(input, type="sequence")
  job$output <- rhfmt(output, type="sequence")
  job$mon.sec <- 10
  job$jobname <- output
  job$readback <- FALSE  

  job.mr <- do.call("rhwatch", job)  

}


swaptoTime <- function(input, output, control=mapreduce.control()) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      lapply(1:nrow(map.values[[r]]), function(k) {
        key <- map.values[[r]]$date[k]
        value <- c(map.keys[[r]], map.values[[r]]$resp[k])
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
  job$setup <- expression(
    map = {library(plyr, lib.loc=control$libLoc)}
  )
  job$parameters <- list(
    control = control
  )
  job$mapred <- list(
#    #mapred.reduce.tasks = control$reduceTask,  #cdh3,4
    mapreduce.job.reduces = 200,  #cdh5
    mapreduce.task.io.sort.mb = control$io_sort,
    mapreduce.map.sort.spill.percent = control$spill_percent,
    mapreduce.reduce.shuffle.parallelcopies = control$parallelcopies,
    mapreduce.reduce.merge.inmem.threshold = control$reduce_merge_inmem,
    mapreduce.reduce.input.buffer.percent = control$reduce_input_buffer,
    mapreduce.task.timeout  = 0,
    rhipe_reduce_buff_size = control$reduce_buff_size,
    rhipe_map_buff_size = 100000,
    rhipe_map_bytes_read = 256*2^20,
    dfs.blocksize = 512*2^20,
    mapreduce.map.java.opts = "-Xmx3072m",
    mapreduce.map.memory.mb = 4096,
    "yarn.nodemanager.resource.cpu-vcores" = 5
 )
  job$combiner <- TRUE
  job$input <- rhfmt(input, type="sequence")
  job$output <- rhfmt(output, type="sequence")
  job$mon.sec <- 10
  job$jobname <- output
  job$readback <- FALSE  

  job.mr <- do.call("rhwatch", job)  

}
