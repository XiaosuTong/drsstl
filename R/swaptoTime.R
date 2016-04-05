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
#'     me <- mapreduce.control(libLoc="/home/tongx/R_LIBS", io_sort=512, BLK=256, reduce_input_buffer_percent=0.7, reduce_merge_inmem=0, task_io_sort_factor=100, spill_percent=1)
#'     \dontrun{
#'       swaptoTimeMap(FileInput, FileOutput, me)
#'       swaptoTimeRed(FileOutput, "/wsc/tongx/spatem/tmax/sim/byyr256test", me)
#'     }
swaptoTime <- function(input, output, cluster_control=mapreduce.control()){

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      lapply(seq(1, 786432, 2), function(i) {
        rhcollect(i, c(map.values[[r]][c(i, i+786432, i + 786432*2)], i, map.keys[[r]], map.values[[r]][c(i+1, i+1+786432, i+1 + 786432*2)], i+1, map.keys[[r]]))
      })
    })
  })
  job$reduce <- expression(
    pre = {
      combine <- numeric()
    },
    reduce = {
      combine <- c(combine, do.call("c", reduce.values))
    },
    post = {
      rhcollect(reduce.key, combine)
    }
  )
  job$setup <- expression(
    map = {library(plyr, lib.loc=Clcontrol$libLoc)}
  )
  job$parameters <- list(
    Clcontrol = cluster_control
  )
  job$mapred <- list(
    mapreduce.map.java.opts = cluster_control$map_jvm,
    mapreduce.map.memory.mb = cluster_control$map_memory, 
    mapreduce.reduce.java.opts = cluster_control$reduce_jvm,
    mapreduce.reduce.memory.mb = cluster_control$reduce_memory,
    mapreduce.job.reduces = cluster_control$reduceTask,  #cdh5
    dfs.blocksize = cluster_control$BLK,
    mapreduce.task.io.sort.mb = cluster_control$io_sort,
    mapreduce.map.sort.spill.percent = cluster_control$spill_percent,
    mapreduce.reduce.shuffle.parallelcopies = cluster_control$reduce_parallelcopies,
    mapreduce.task.io.sort.factor = cluster_control$task_io_sort_factor,
    mapreduce.reduce.shuffle.merge.percent = cluster_control$reduce_shuffle_merge_percent,
    mapreduce.reduce.merge.inmem.threshold = cluster_control$reduce_merge_inmem,
    mapreduce.reduce.input.buffer.percent = cluster_control$reduce_input_buffer_percent,
    mapreduce.reduce.shuffle.input.buffer.percent = cluster_control$reduce_shuffle_input_buffer_percent,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK",
    mapreduce.task.timeout  = 0,
    mapreduce.job.reduce.slowstart.completedmaps = cluster_control$slow_starts,
    rhipe_reduce_buff_size = cluster_control$reduce_buffer_size,
    rhipe_reduce_bytes_read = cluster_control$reduce_buffer_read,
    rhipe_map_buff_size = cluster_control$map_buffer_size, 
    rhipe_map_bytes_read = cluster_control$map_buffer_read 
  )
  job$combiner <- TRUE
  job$input <- rhfmt(input, type="sequence")
  job$output <- rhfmt(output, type="sequence")
  job$mon.sec <- 10
  job$jobname <- output
  job$readback <- FALSE  

  job.mr <- do.call("rhwatch", job)  

}


## bystatfit128 or bystatfit256 did have difference about time
## for tmaxs128, io_sort is 1024 can avoid spilling
## reduce_parallelcopies is not quite helpful
## 0.52 is the best slow_starts
## reduce_input_buffer_percent is 0.0 is slow, 0.7 is the optimal
## reduce_shuffle_input_buffer_percent and reduce_shuffle_merge_percent together cannot to be too small like all 0.1
## reduce_shuffle_input_buffer_percent = 0.7 and reduce_shuffle_merge_percent =0.4 can aviod spill
## even though the multplication of these two kept the same, larger reduce_shuffle_input_buffer_percent be faster

## for tmaxs256, io_sort is 768 can avoid spilling
## small slow_starts value may be faster

#FileInput <- "/wsc/tongx/spatem/tmax/sims/bystatfit128"
#FileOutput <- "/wsc/tongx/spatem/tmax/test/bymthse128"
#me <- mapreduce.control(
#  libLoc=lib.loc, reduceTask=358, io_sort=1024, BLK=128, slow_starts = 0.7,
#  map_jvm = "-Xmx3584m", reduce_jvm = "-Xmx4096m", map_memory = 5120, reduce_memory = 5120,
#  reduce_input_buffer_percent=0.9, reduce_parallelcopies=10,
#  reduce_merge_inmem=0, task_io_sort_factor=100,
#  spill_percent=0.9, reduce_shuffle_input_buffer_percent = 0.9,
#  reduce_shuffle_merge_percent = 0.99,
#  reduce_buffer_read = 100, map_buffer_read = 100,
#  reduce_buffer_size = 10000, map_buffer_size = 10
#)
#swaptoTime(FileInput, FileOutput, cluster_control=me)