#' Conduct the stlplus fitting at each location in parallel
#'
#' call stlplus function on time series at each location in parallel.
#' Every station uses the same smoothing parameter
#'
#' @param input
#'     The path of input sequence file on HDFS. It should be by location division.
#' @param output
#'     The path of output sequence file on HDFS. It is by location division but with seasonal and trend components
#' @param model_control
#'     The list contains all smoothing parameters
#' @param cluster_control
#'     A list contains all mapreduce tuning parameters.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     FileInput <- "/wsc/tongx/spatem/tmax/sim/bystat256"
#'     FileOutput <- "/wsc/tongx/spatem/tmax/sim/bystatfit512"
#'     me <- mapreduce.control(libLoc="/home/tongx/R_LIBS", BLK = 512)
#'     you <- spacetime.control(vari="resp", time="date", seaname="month", n=3145728, n.p=12, s.window=13, t.window = 241, degree=2, span=0.015, Edeg=2)
#'     \dontrun{
#'       stlfit(FileInput, FileOutput, you, me)
#'     }

stlfit <- function(input, output, model_control=spacetime.control(), cluster_control=mapreduce.control()) {
  
  job <- list()
  job$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      value <- arrange(data.frame(matrix(map.values[[r]], ncol=2, byrow=TRUE)), X1)

      fit <- stlplus::stlplus(
        x=value$X2, t=value$X1, n.p=Mlcontrol$n.p, 
        s.window=Mlcontrol$s.window, s.degree=Mlcontrol$s.degree, 
        t.window=Mlcontrol$t.window, t.degree=Mlcontrol$t.degree, 
        inner=Mlcontrol$inner, outer=Mlcontrol$outer
      )$data
      # value originally is a data.frame with 4 columns, vectorize it 
      #rhcollect(map.keys[[r]], unname(unlist(cbind(value, subset(fit, select = c(seasonal, trend))))))
      names(value) <- c(Mlcontrol$time, Mlcontrol$vari)
      value <- cbind(subset(value, select = -c(date)), subset(fit, select = c(seasonal, trend)))
      rhcollect(map.keys[[r]], unname(unlist(value)))
    })
  })
  job$parameters <- list(
    Mlcontrol = model_control,
    Clcontrol = cluster_control
  )
  job$setup <- expression(
    map = {
      suppressMessages(library(stlplus, lib.loc=Clcontrol$libLoc))
      library(plyr, lib.loc=Clcontrol$libLoc)
    }
  )
  job$input <- rhfmt(input, type = "sequence")
  job$output <- rhfmt(output, type = "sequence")
  job$mapred <- list(
    mapreduce.task.timeout = 0,
    mapreduce.job.reduces = cluster_control$reduceTask,  #cdh5
    mapreduce.map.java.opts = cluster_control$map_jvm,
    mapreduce.map.memory.mb = cluster_control$map_memory,     
    dfs.blocksize = cluster_control$BLK,
    rhipe_reduce_buff_size = cluster_control$reduce_buffer_size,
    rhipe_reduce_bytes_read = cluster_control$reduce_buffer_read,
    rhipe_map_buff_size = cluster_control$map_buffer_size, 
    rhipe_map_bytes_read = cluster_control$map_buffer_read,
    mapreduce.map.output.compress = TRUE,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK"
  )
  job$mon.sec <- 10
  job$readback <- FALSE
  job$jobname <- output
  job.mr <- do.call("rhwatch", job)

}


# FileInput <- "/wsc/tongx/spatem/tmax/sims/bystat256"
# FileOutput <- "/wsc/tongx/spatem/tmax/test/bystatfit256"
# 
# me <- mapreduce.control(
#   libLoc=lib.loc, reduceTask=0, BLK=256, 
#   map_jvm = "-Xmx4096m", map_memory = 5120,
#   map_buffer_read = 200, map_buffer_size = 10000
# )
# you <- spacetime.control(
#   vari="resp", time="date", seaname="month", 
#   n=786432, n.p=12, s.window="periodic", t.window = 241, 
#   degree=2, span=0.015, Edeg=2
# )
# stlfit(FileInput, FileOutput, model_control=you, cluster_control=me)
