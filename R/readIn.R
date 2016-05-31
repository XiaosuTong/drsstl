#' Readin Raw text data files and save it as by time division on HDFS.
#'
#' Input raw text data file is divided into by time subsets and saved on HDFS
#'
#' @param input
#'     The path of input file on HDFS. It should be raw text file.
#' @param output
#'     The path of output file on HDFS. It is by time division.
#' @param cluster_control
#'     all parameters that are needed for mapreduce job
#' @param info
#'     The RData on HDFS which contains all station metadata. Make sure
#'     copy the RData of station_info to HDFS first using rhput.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     FileInput <- "/tmp/tmax.txt"
#'     FileOutput <- "/tmp/bymth"
#'     ccontrol <- mapreduce.control(
#'       libLoc=lib.loc, reduceTask=95, io_sort=100, slow_starts = 0.5,
#'       map_jvm = "-Xmx200m", reduce_jvm = "-Xmx200m", 
#'       map_memory = 1024, reduce_memory = 1024,
#'       reduce_input_buffer_percent=0.9, reduce_parallelcopies=5,
#'       spill_percent=0.9, reduce_shuffle_input_buffer_percent = 0.9,
#'       reduce_shuffle_merge_percent = 0.5
#'     )
#'     \dontrun{
#'       rhput("./station_info.RData", "/tmp/station_info.RData")
#'     }
#'     readIn(
#'       FileInput, FileOutput, info="/tmp/station_info.RData", cluster_control=ccontrol
#'     )


readIn <- function(input, output, info, cluster_control = mapreduce.control(), cshift=1) {

  job <- list()
  job$map <- expression({
    y <- do.call("rbind", 
      lapply(map.values, function(r) {
        row <- strsplit(r, " +")[[1]]
        c(row[1:(13 + cshift)], substring(row[16], 1:12, 1:12))
      })
    )
    #file <- Sys.getenv("mapred.input.file") #get the file name that Hadoop is reading
    #k <- as.numeric(
    #  substr(tail(strsplit(tail(strsplit(file, "/")[[1]],1), "[.]")[[1]], 1), 2, 4)
    #)
    miss <- as.data.frame(
      matrix(as.numeric(y[, (1:12) + (13 + cshift)]), ncol = 12)
    )
    tmp <- as.data.frame(
      matrix(as.numeric(y[, (1:12) + (1 + cshift)]), ncol = 12)
    )

    name <- match(y[, (1 + cshift)], station_info$station.id)

    if (cshift == 2) {
      year <- (as.numeric(y[, 2]) - 55) + (as.numeric(y[, 1]) - 1)*48
    } else if (cshift == 1) {
      year <- as.numeric(y[, 1]) - 55
    } else {
      stop("the column shift cannot be other value than 1 or 2!")
    }

    tmp <- tmp/10
    tmp[miss == 1] <- NA
    names(tmp) <- 1:12
    tmp <- cbind(station.id = as.numeric(name), tmp, year = year, stringsAsFactors = FALSE)
    value <- data.frame(
      station.id = rep(tmp$station.id, 12),
      year = rep(tmp$year, 12),
      month = rep(names(tmp)[1:12], each = dim(tmp)[1]),
      resp = c(
        tmp[, 2], tmp[, 3], tmp[, 4], tmp[, 5], tmp[, 6], tmp[, 7], 
        tmp[, 8], tmp[, 9], tmp[, 10], tmp[, 11], tmp[, 12], tmp[, 13]
      ),
      stringsAsFactors = FALSE
    )
    value <- subset(value, !is.na(station.id))
    d_ply(
      .data = value,
      .vari = c("year","month"),
      .fun = function(r) {
        rhcollect(c(unique(r$year), unique(r$month)), as.matrix(subset(r, select = -c(month, year)), rownames.force=FALSE))
      }
    )
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
  job$setup <- expression(
    map = {
      library(plyr, lib.loc = Clcontrol$libLoc)
      load(strsplit(info, "/")[[1]][length(strsplit(info, "/")[[1]])])
    }
  )
  job$shared <- c(info)
  job$parameters <- list(
    Clcontrol = cluster_control,
    info = info,
    cshift = cshift
  )
  job$input <- rhfmt(input, type = "text")
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
  job$combiner <- TRUE
  job$jobname <- output
  job$readback <- FALSE
  job$mon.sec <- 10
  job.mr <- do.call("rhwatch", job)

  return(job.mr[[1]]$jobid)

}


## tmaxs128 or tmaxs256 did not have too much difference about time
## for tmaxs128, io_sort is 512 can avoid spilling
## reduce_parallelcopies is not quite helpful
## 0.52 is the best slow_starts
## reduce_input_buffer_percent is 0.0 is slow, 0.7 is the optimal
## reduce_shuffle_input_buffer_percent and reduce_shuffle_merge_percent together cannot to be too small like all 0.1
## reduce_shuffle_input_buffer_percent = 0.7 and reduce_shuffle_merge_percent =0.4 can aviod spill
## even though the multplication of these two kept the same, larger reduce_shuffle_input_buffer_percent be faster

## for tmaxs256, io_sort is 768 can avoid spilling
## small slow_starts value may be faster
