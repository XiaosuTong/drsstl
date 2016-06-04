#' Swap to division by-location
#'
#' Switch input key-value pairs which is division by-month
#' to the key-value pairs which is division by-location.
#'
#' @param input
#'     The path of input file on HDFS. It should be by-month division.
#' @param output
#'     The path of output file on HDFS. It is by-location division.
#' @param cluster_control
#'     Should be a list object generated from \code{mapreduce.control} function.
#'     The list including all necessary Rhipe parameters and also user tunable 
#'     MapReduce parameters.
#' @param model_control
#'     Should be a list object generated from \code{spacetime.control} function.
#'     The list including all necessary smoothing parameters of nonparametric fitting.
#' @param final
#'     There two steps of switching to by-location division in the routine. In the 
#'     first one, which final is set to be FALSE, the intermediate value is vectorized
#'     to minimize the size. In the second one, which final is set to be TRUE, the output
#'     value is saved as data.frame.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @seealso
#'     \code{\link{spacetime.control}}, \code{\link{mapreduce.control}}

#' @examples
#'     FileInput <- "/tmp/bymthfit"
#'     FileOutput <- "/tmp/bystat"
#'     ccontrol <- mapreduce.control(
#'       libLoc=NULL, reduceTask=5, io_sort=128, slow_starts = 0.5,
#'       reduce_input_buffer_percent=0.4, reduce_parallelcopies=10,
#'       reduce_merge_inmem=0, task_io_sort_factor=100,
#'       spill_percent=0.9, reduce_shuffle_input_buffer_percent = 0.8,
#'       reduce_shuffle_merge_percent = 0.4
#'     )
#'     mcontrol <- spacetime.control(
#'       vari="resp", time="date", n=576, n.p=12, stat_n=7738,
#'       s.window=13, t.window = 241, degree=2, span=0.015, Edeg=2
#'     )
#'     swaptoLoc(FileInput, FileOutput, cluster_control=ccontrol, model_control=mcontrol)

swaptoLoc <- function(input, output, final=FALSE, cluster_control=mapreduce.control(), model_control=spacetime.control()) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      
      if(!final) {
        date <- (as.numeric(map.keys[[r]][1]) - 1)*Mlcontrol$n.p + as.numeric(match(map.keys[[r]][2], month.abb))
        lapply(1:length(map.values[[r]]), function(i){
          rhcollect(i, c(date, map.values[[r]][i]))
          NULL
        })
      } else {
        N <- Mlcontrol$stat_n
        value <- unname(unlist(map.values[[r]]))
        lapply(1:N, function(i) {
          rhcollect(i, data.frame(date=map.keys[[r]], smoothed=value[i+N], seasonal=value[i+N*2], trend=value[i+N*3], Rspa=value[i+N*4]))
          NULL
        })
      }

    })
  })
  if(!final) {
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
  } else {
    job$reduce <- expression(
      pre = {
        combine <- data.frame()
      },
      reduce = {
        combine <- rbind(combine, do.call("rbind", reduce.values))
      },
      post = {
        combine <- arrange(combine, date)
        rownames(combine) <- NULL
        rhcollect(reduce.key, combine)
      }
    )
  }
  job$parameters <- list(
    final = final,
    Mlcontrol = model_control,
    Clcontrol = cluster_control
  )
  job$setup <- expression(
    map = {
      suppressMessages(library(plyr, lib.loc=Clcontrol$libLoc))
    },
    reduce = {
      suppressMessages(library(plyr, lib.loc=Clcontrol$libLoc))
    }
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

  #return(job.mr[[1]]$jobid)

}

