#' Swap to division by time
#'
#' Switch input key-value pairs which is division by location
#' to the key-value pairs which is division by time.
#'
#' @param input
#'     The path of input file on HDFS. It should be by location division.
#' @param output
#'     The path of output file on HDFS. It is by time division.
#' @param cluster_control
#'     Should be a list object generated from \code{mapreduce.control} function.
#'     The list including all necessary Rhipe parameters and also user tunable 
#'     MapReduce parameters.
#' @param model_control
#'     Should be a list object generated from \code{spacetime.control} function.
#'     The list including all necessary smoothing parameters of nonparametric fitting.
#' @details
#'     \code{swaptoTime} is used for switching division by location to division by time.
#'     The input key is location index, and input value is a vectorized matrix with 
#'     \code{Mlcontrol$n} rows and 3 columns in order of resp, seasonal, trend. For each row 
#'     of matrix, a new key-value pair is generated. Since the matrix is vectorized
#'     by column, the trend in ith row is \code{i+Mlcontrol$n}. Index \code{j} controls the index 
#'     of multiple location in one time point.
#' @author 
#'     Xiaosu Tong 
#' @seealso
#'     \code{\link{spacetime.control}}, \code{\link{mapreduce.control}}
#' @export
#' @examples
#'     FileInput <- "/wsc/tongx/spatem/tmax/sims/bystatfit"
#'     FileOutput <- "/wsc/tongx/spatem/tmax/sims/bymth"
#'     ccontrol <- mapreduce.control(
#'       libLoc=lib.loc, reduceTask=358, io_sort=1024, BLK=128, slow_starts = 0.5,
#'       map_jvm = "-Xmx3072m", reduce_jvm = "-Xmx4096m", 
#'       map_memory = 5120, reduce_memory = 5120,
#'       reduce_input_buffer_percent=0.2, reduce_parallelcopies=10,
#'       reduce_merge_inmem=0, task_io_sort_factor=100,
#'       spill_percent=0.9, reduce_shuffle_input_buffer_percent = 0.7,
#'       reduce_shuffle_merge_percent = 0.5,
#'       reduce_buffer_read = 100, map_buffer_read = 100,
#'       reduce_buffer_size = 10000, map_buffer_size = 10
#'     )
#'     mcontrol <- spacetime.control(
#'       vari = "resp", time = "date", seaname = "month", 
#'       n = 786432, n.p = 12, s.window = "periodic", t.window = 241, 
#'       degree = 2, span = 0.015, Edeg = 2, statbytime = 2
#'     )
#'     swaptoTime(FileInput, FileOutput, cluster_control=ccontrol, model_control=mcontrol)
swaptoTime <- function(input, output, cluster_control=mapreduce.control(), model_control=spacetime.control()){

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      lapply(seq(1, Mlcontrol$n, Mlcontrol$statbytime), function(i) {
        value <- numeric()
        for(j in 0:(Mlcontrol$statbytime-1)){
          # j is the station (index - 1) in each time point, 2 here since we have two components, seasonal and trend 
          value <- c(value, map.values[[r]][c(i, i+Mlcontrol$n+j, i+Mlcontrol$n*2+j)], i+j, map.keys[[r]] )
        }
        rhcollect(i, value)
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
    Clcontrol = cluster_control,
    Mlcontrol = model_control
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
## for bystatfit128, io_sort is 1024 can avoid spilling
## opt_jvm can be 3072 but no difference with 2560
## reduce_parallelcopies is not quite helpful
## 0.5 is the best slow_starts
## reduce_input_buffer_percent is 0.0 is slow, 0.7 is the optimal
## reduce_shuffle_input_buffer_percent and reduce_shuffle_merge_percent together cannot to be too small like all 0.1
## reduce_shuffle_input_buffer_percent = 0.7 and reduce_shuffle_merge_percent =0.5 can aviod spill
## even though the multplication of these two kept the same, larger reduce_shuffle_input_buffer_percent be faster

## for bystatfit256, spilling cannot be avioded because of the size
## jvm_opt cannot larger than 2048


#  FileInput <- "/wsc/tongx/spatem/tmax/sims/bystatfit128"
#  FileOutput <- "/wsc/tongx/spatem/tmax/test/bymthse128"
#  me <- mapreduce.control(
#    libLoc=lib.loc, reduceTask=358, io_sort=512, BLK=128, slow_starts = 0.5,
#    map_jvm = "-Xmx3584m", reduce_jvm = "-Xmx4096m", map_memory = 5120, reduce_memory = 5120,
#    reduce_input_buffer_percent=0.9, reduce_parallelcopies=10,
#    reduce_merge_inmem=0, task_io_sort_factor=20,
#    spill_percent=0.9, reduce_shuffle_input_buffer_percent = 0.9,
#    reduce_shuffle_merge_percent = 0.9,
#    reduce_buffer_read = 100, map_buffer_read = 100,
#    reduce_buffer_size = 10000, map_buffer_size = 10
#  )
#  system.time(swaptoTime(FileInput, FileOutput, cluster_control=me))


## io_sort 128, 512
## spill_percent 0.5, 0.9
## slow_starts 0.1, 0.5
## reduce_parallelcopies 5, 10
## reduce_shuffle_input_buffer_percent 0.1, 0.9
## reduce_shuffle_merge_percent 0.5, 0.9
## reduce_input_buffer_percent 0.1, 0.9
## task_io_sort_factor 10, 20
