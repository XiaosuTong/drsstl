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
      value <- arrange(as.data.frame(map.values[[r]]), V1)
      fit <- stlplus::stlplus(
        x=value$V2, t=value$V1, n.p=Mlcontrol$n.p, 
        s.window=Mlcontrol$s.window, s.degree=Mlcontrol$s.degree, 
        t.window=Mlcontrol$t.window, t.degree=Mlcontrol$t.degree, 
        inner=Mlcontrol$inner, outer=Mlcontrol$outer
      )$data
      names(value) <- c(Mlcontrol$time, Mlcontrol$vari)
      value <- cbind(value, subset(fit, select = c(seasonal, trend)))
      rhcollect(map.keys[[r]], value)
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
    mapreduce.job.reduces = 0,  #cdh5
    mapreduce.map.java.opts = "-Xmx3584m",
    mapreduce.map.memory.mb = 5120,     
    dfs.blocksize = cluster_control$BLK,
    rhipe_map_bytes_read = 200*2^20,
    rhipe_map_buffer_size = 10000,
    mapreduce.map.output.compress = TRUE,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK"
  )
  job$mon.sec <- 10
  job$readback <- FALSE
  job$jobname <- output
  job.mr <- do.call("rhwatch", job)

}
