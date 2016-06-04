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

