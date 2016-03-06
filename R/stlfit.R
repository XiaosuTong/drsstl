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
#'     FileInput <- "/wsc/tongx/spatem/tmax/sim/bystation"
#'     FileOutput <- "/wsc/tongx/spatem/tmax/sim/bystatfit"
#'     me <- mapreduce.control(libLoc=lib.loc, BLK = 512)
#'     you <- spacetime.control(vari="resp", time="date", seaname="month", n=4448736, n.p=12, s.window=13, t.window = 241)
#'     \dontrun{
#'       stlfit(FileInput, FileOutput, you, me)
#'     }

stlfit <- function(input, output, model_control=spacetime.control(), cluster_control=mapreduce.control()) {
  
  job <- list()
  job$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      #value <- arrange(map.values[[r]], date)
      fit <- stlplus::stlplus(
        x=map.values[[r]][, control$vari], t=map.values[[r]][, control$time], n.p=control$n.p, 
        s.window=control$s.window, s.degree=control$s.degree, 
        t.window=control$t.window, t.degree=control$t.degree, 
        inner=control$inner, outer=control$outer
      )$data
      value <- cbind(subset(map.values[[r]], select=c(-month)), subset(fit, select = c(seasonal, trend)))
      rhcollect(map.keys[[r]], value)
    })
  })
  job$parameters <- list(
    control = model_control,
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
    mapreduce.output.fileoutputformat.compress.type = "BLOCK",
    mapreduce.map.java.opts = "-Xmx3072m",
    mapreduce.map.memory.mb = 4096,
    dfs.blocksize = cluster_control$BLK
    #rhipe_map_bytes_read = buffer*2^20
  )
  job$readback <- FALSE
  job$jobname <- output
  job.mr <- do.call("rhwatch", job)

}
