#' Readin Raw text data files and save it as by time division on HDFS.
#'
#' Input raw text data file is divided into by time subsets and saved on HDFS
#'
#' @param input
#'     The path of input sequence file on HDFS. It should be raw text file.
#' @param output
#'     The path of output sequence file on HDFS. It is by time division.
#' @param cluster_control
#'     all parameters that are needed for mapreduce job
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'    me <- mapreduce.control(
#'      libLoc=lib.loc, reduceTask=1300, io_sort=512, BLK=256, 
#'      reduce_input_buffer_percent=0.99, reduce_parallelcopies=10, 
#'      reduce_merge_inmem=0, task_io_sort_factor=100, 
#'      spill_percent=0.99, reduce_shuffle_input_buffer_percent = 0.9,
#'      reduce_shuffle_merge_percent = 0.99
#'    )
#'    \dontrun{
#'      readIn("/wsc/tongx/spatem/nRaw/tmax","/wsc/tongx/spatem/tmax/sim/bymth", info="/wsc/tongx/spatem/stationinfo/a1950UStinfo.RData", me) 
#'    }
readIn <- function(input, output, info, cluster_control = mapreduce.control()) {

  job <- list()
  job$map <- expression({
    y <- do.call("rbind", 
      lapply(map.values, function(r) {
        row <- strsplit(r, " +")[[1]]
        c(row[1:3], row[4:15], substring(row[16], 1:12, 1:12))
      })
    )
    #file <- Sys.getenv("mapred.input.file") #get the file name that Hadoop is reading
    #k <- as.numeric(
    #  substr(tail(strsplit(tail(strsplit(file, "/")[[1]],1), "[.]")[[1]], 1), 2, 4)
    #)
    miss <- as.data.frame(
      matrix(as.numeric(y[, (1:12) + 15]), ncol = 12)
    )
    tmp <- as.data.frame(
      matrix(as.numeric(y[, (1:12) + 3]), ncol = 12)
    )

    name <- match(y[, 3], a1950UStinfo$station.id)

    year <- (as.numeric(y[, 2]) - 55) + (as.numeric(y[, 1]) - 1)*48
    tmp <- tmp/10
    tmp[miss == 1] <- NA
    names(tmp) <- 1:12
    tmp <- cbind(station.id = as.numeric(name), tmp, year = year, stringsAsFactors = FALSE)
    value <- data.frame(
      station.id = rep(tmp$station.id, 12),
      year = rep(tmp$year, 12),
      month = rep(names(tmp)[2:13], each = dim(tmp)[1]),
      resp = c(
        tmp[, 2], tmp[, 3], tmp[, 4], tmp[, 5], tmp[, 6], tmp[, 7], 
        tmp[, 8], tmp[, 9], tmp[, 10], tmp[, 11], tmp[, 12], tmp[, 13]
      ),
      stringsAsFactors = FALSE
    )
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
      load("a1950UStinfo.RData")
    }
  )
  job$shared <- c(info)
  job$parameters <- list(Clcontrol = cluster_control)
  job$input <- rhfmt(input, type = "text")
  job$output <- rhfmt(output, type = "sequence")
  job$mapred <- list( 
    mapreduce.map.java.opts = "-Xmx3072m",
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
    rhipe_reduce_buff_size = 10000,
    rhipe_reduce_bytes_read = 150*2^20,
    rhipe_map_buff_size = 10000, 
    mapreduce.job.reduce.slowstart.completedmaps = 0.9 
  )
  job$combiner <- TRUE
  job$jobname <- output
  job$readback <- FALSE
  job.mr <- do.call("rhwatch", job)

}

result <- data.frame()

for (i in round(179*(seq(1,7,1)))) {

    me <- mapreduce.control(
      libLoc=lib.loc, reduceTask=i, io_sort=512, BLK=256, 
      reduce_input_buffer_percent=0.9, reduce_parallelcopies=10, 
      reduce_merge_inmem=0, task_io_sort_factor=100, 
      spill_percent=1.0, reduce_shuffle_input_buffer_percent = 0.9,
      reduce_shuffle_merge_percent = 0.99
    )
    time <- system.time(readIn("/wsc/tongx/spatem/nRaw/tmax","/wsc/tongx/spatem/tmax/sim/bymth256", info="/wsc/tongx/spatem/stationinfo/a1950UStinfo.RData", me)) 
    rst <- data.frame(user=as.numeric(time[1]), sys=as.numeric(time[2]), elap = as.numeric(time[3]))
    result <- rbind(result, rst)
    
    Sys.sleep(300)
    
}