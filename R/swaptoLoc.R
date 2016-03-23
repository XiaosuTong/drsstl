#' Swap to division by location
#'
#' Switch input key-value pairs which is division by time
#' to the key-value pairs which is division by location.
#'
#' @param input
#'     The path of input sequence file on HDFS. It should be by location division.
#' @param output
#'     The path of output sequence file on HDFS. It is by time division.
#' @param sub
#'     The number of different locations within one hadoop key-value pair.
#' @param model_control
#'     The list contains all smoothing parameters
#' @param cluster_control
#'     A list contains all mapreduce tuning parameters.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     FileInput <- "/wsc/tongx/spatem/tmax/sim/bymthfit256"
#'     FileOutput <- "/wsc/tongx/spatem/tmax/sim/bystat256"
#'     me <- mapreduce.control(libLoc="/home/tongx/R_LIBS", io_sort=512, BLK=256, reduce_input_buffer_percent=0.7, reduce_merge_inmem=0, task_io_sort_factor=100, spill_percent=1)
#'     \dontrun{
#'       swaptoLoc(FileInput, FileOutput, sub=1, cluster_control=me)
#'     }
swaptoLoc <- function(input, output, sub, cluster_control=mapreduce.control()) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      date <- (as.numeric(map.keys[[r]][1]) - 1)*12 + as.numeric(map.keys[[r]][2])
      if(sub == 1) {
        lapply(1:length(map.values[[r]]), function(i){
          rhcollect(i, c(date, map.values[[r]][i]))
        })
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
      rhcollect(reduce.key, as.matrix(combine, rownames.force=FALSE))
    }
  )
  job$parameters <- list(
   sub = sub
  )
  job$combiner <- TRUE
  job$input <- rhfmt(input , type = "sequence")
  job$output <- rhfmt(output, type = "sequence")
  job$mapred <- list(
    mapreduce.map.java.opts = "-Xmx2560m",
    mapreduce.map.memory.mb = 5120, 
    mapreduce.reduce.java.opts = "-Xmx4608m",
    mapreduce.reduce.memory.mb = 5120,
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
    mapreduce.job.reduce.slowstart.completedmaps = 0.9,
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


#result <- data.frame()
# 
#for(k in 1:3) {
#  for (i in c(512, 256, 128)) {
#    for(j in c(1, 0.8, 0.5, 0.2)) {
#
#        print(c(k,i,j))
#        me <- mapreduce.control(
#          libLoc=lib.loc, reduceTask=537, io_sort=i, BLK=256, 
#          reduce_input_buffer_percent=0.9, reduce_parallelcopies=10, 
#          reduce_merge_inmem=0, task_io_sort_factor=100, 
#          spill_percent=j, reduce_shuffle_input_buffer_percent = 0.9,
#          reduce_shuffle_merge_percent = 0.99,
#          reduce_buff_read = 150, map_buffer_read = 150
#        )
#        time <- system.time(swaptoLoc(FileInput, FileOutput, sub=1, cluster_control=me)) 
#        rst <- data.frame(rep = k, iosort=i, percent=j, elap = as.numeric(time[3]))
#        result <- rbind(result, rst)
#        
#        Sys.sleep(300)
#
#    }
#  }
#}
