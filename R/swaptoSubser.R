#' Swap to division by subseries
#'
#' Switch input key-value pairs which is division by location
#' to the key-value pairs which is division by subseries.
#'
#' @param input
#'     The path of input file on HDFS. It should be by location.
#' @param output
#'     The path of output file on HDFS. It is by subseries division.
#' @param cluster_control
#'     Should be a list object generated from \code{mapreduce.control} function.
#'     The list including all necessary Rhipe parameters and also user tunable 
#'     MapReduce parameters.
#' @param model_control
#'     Should be a list object generated from \code{spacetime.control} function.
#'     The list including all necessary smoothing parameters of nonparametric fitting.
#' @details
#'     \code{swaptoSubser} is used for switch division by location to division by subseries.
#'     The smoothing procedure can be applied to each subseries in parallel. So the
#'      output files will be intput to parallel seasonal fitting of stlplus.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @seealso
#'     \code{\link{spacetime.control}}, \code{\link{mapreduce.control}}
#' @examples
#'     FileInput <- "/wsc/tongx/spatem/tmax/sims/bystat"
#'     FileOutput <- "/wsc/tongx/spatem/tmax/sims/bysubser"
#'     ccontrol <- mapreduce.control(libLoc=.libPaths(), reduceTask=179)
#'     mcontrol <- spacetime.control(
#'       vari = "resp", time = "date", seaname = "month", 
#'       n = 786432, n.p = 12, s.window = "periodic", t.window = 241, 
#'       degree = 2, span = 0.015, Edeg = 2, statbytime = 2
#'     )
#'     swaptoSubser(FileInput, FileOutput, cluster_control=ccontrol, model_control=mcontrol)


swaptoSubser <- function(input, output, cluster_control, model_control) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      plyr::d_ply(
        .data = map.values[[r]],
        .variable = c("month"),
        .fun = function(k, station = map.keys[[r]]) {
          key <- c(station, unique(as.character(k$month)))
          value <- subset(k, select = -c(month))
          attr(value, "loc") <- attributes(map.values[[r]])$loc
          rhcollect(key, value)
      })
    })
  })
  job$parameters <- list(
    Clcontrol = cluster_control,
    Mlcontrol = model_control
  )
  job$setup <- expression(
    map = {library(plyr, lib.loc=Clcontrol$libLoc)}
  )
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
  job$input <- rhfmt(input, type="sequence")
  job$output <- rhfmt(output, type="sequence")
  job$mon.sec <- 20
  job$jobname <- output
  job$readback <- FALSE  

  job.mr <- do.call("rhwatch", job)  

}

