#' Swap to division by location
#'
#' Switch input key-value pairs which is division by time
#' to the key-value pairs which is division by location.
#'
#' @param input
#'     The path of input file on HDFS. It should be by location division.
#' @param output
#'     The path of output file on HDFS. It is by time division.
#' @param cluster_control
#'     Should be a list object generated from \code{mapreduce.control} function.
#'     The list including all necessary Rhipe parameters and also user tunable 
#'     MapReduce parameters.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @seealso
#'     \code{\link{spacetime.control}}, \code{\link{mapreduce.control}}
#' @details
#'     The value of each input key-value pair is a vector of spatial smoothed
#'     value in the same order based on location index. For each of element
#'     of this vector a intermediate key-value pair is collected. The value 
#'     of final output key-value pair is a vector in order to keep the size
#'     as small as possible, and also guarantee the combiner can set to be TRUE. 
#' @examples
#'     FileInput <- "/wsc/tongx/spatem/tmax/sim/bymthfit"
#'     FileOutput <- "/wsc/tongx/spatem/tmax/sim/bystat"
#'     ccontrol <- mapreduce.control(
#'       libLoc=lib.loc, reduceTask=169, io_sort=768, BLK=256, slow_starts = 0.5,
#'       map_jvm = "-Xmx3072m", reduce_jvm = "-Xmx4096m", 
#'       map_memory = 5120, reduce_memory = 5120,
#'       reduce_input_buffer_percent=0.4, reduce_parallelcopies=10,
#'       reduce_merge_inmem=0, task_io_sort_factor=100,
#'       spill_percent=0.9, reduce_shuffle_input_buffer_percent = 0.8,
#'       reduce_shuffle_merge_percent = 0.4,
#'       reduce_buffer_read = 100, map_buffer_read = 100,
#'       reduce_buffer_size = 10000, map_buffer_size = 100
#'     )
#'     swaptoLoc(FileInput, FileOutput, cluster_control=ccontrol)

swaptoLoc <- function(input, output, final=FALSE, cluster_control=mapreduce.control()) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      
      if(!final) {
        date <- (as.numeric(map.keys[[r]][1]) - 1)*12 + as.numeric(map.keys[[r]][2])
        lapply(1:length(map.values[[r]]), function(i){
          rhcollect(i, c(date, map.values[[r]][i]))
          NULL
        })
      } else {
        value <- unname(unlist(map.values[[r]]))
        lapply(1:7738, function(i) {
          rhcollect(i, c(map.keys[[r]], value[i], value[i+7738], value[i+7738*2], value[i+7738*3]))
        })
      }

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
  job$parameters <- list(
   final = final
  )
  job$combiner <- TRUE
  job$input <- rhfmt(input , type = "sequence")
  job$output <- rhfmt(output, type = "sequence")
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
  job$mon.sec <- 10
  job$jobname <- output  
  job$readback <- FALSE  

  job.mr <- do.call("rhwatch", job)

}


## for bymthfit128, io_sort 512 can avoid spilling
## reduceTask to be 130 can make each file to be around 256MB
## but reduceTask 169 can make sure after stfit, each file is about 256 MB
## reduce_parallelcopies is not quite helpful
## 0.5 is the best slow_starts
## reduce_input_buffer_percent is 0.0 is slow, 0.2 is the optimal
## reduce_shuffle_input_buffer_percent and reduce_shuffle_merge_percent together cannot to be too small like all 0.1
## reduce_shuffle_input_buffer_percent = 0.8 and reduce_shuffle_merge_percent =0.4 can aviod spill
## even though the multplication of these two kept the same, larger reduce_shuffle_input_buffer_percent be faster

##for bymthfit256, io_sort 768 can avoid spilling, but the jvm_opt cannot be larger than 2560 

# FileInput <- "/wsc/tongx/spatem/tmax/sims/bymthfit128"
# FileOutput <- "/wsc/tongx/spatem/tmax/test/bystat256"
# me <- mapreduce.control(
#   libLoc=lib.loc, reduceTask=169, io_sort=512, BLK=256, slow_starts = 0.5,
#   map_jvm = "-Xmx3584m", reduce_jvm = "-Xmx4096m", map_memory = 5120, reduce_memory = 5120,
#   reduce_input_buffer_percent=0.9, reduce_parallelcopies=10,
#   reduce_merge_inmem=0, task_io_sort_factor=20,
#   spill_percent=0.9, reduce_shuffle_input_buffer_percent = 0.9,
#   reduce_shuffle_merge_percent = 0.9,
#   reduce_buffer_read = 100, map_buffer_read = 100,
#   reduce_buffer_size = 10000, map_buffer_size = 100
# )
# system.time(swaptoLoc(FileInput, FileOutput, cluster_control=me))


# FileInput <- "/wsc/tongx/spatem/tmax/sims/bymthfitse256"
# FileOutput <- "/wsc/tongx/spatem/tmax/sims/bystatse256"
# me <- mapreduce.control(
#   libLoc=lib.loc, reduceTask=358, io_sort=512, BLK=256, slow_starts = 0.9,
#   map_jvm = "-Xmx3584m", reduce_jvm = "-Xmx4096m", map_memory = 5120, reduce_memory = 5120,
#   reduce_input_buffer_percent=0.9, reduce_parallelcopies=10,
#   reduce_merge_inmem=0, task_io_sort_factor=20,
#   spill_percent=0.9, reduce_shuffle_input_buffer_percent = 0.9,
#   reduce_shuffle_merge_percent = 0.9,
#   reduce_buffer_read = 100, map_buffer_read = 100,
#   reduce_buffer_size = 10000, map_buffer_size = 100
# )
# system.time(swaptoLoc(FileInput, FileOutput, final = TRUE, cluster_control=me))