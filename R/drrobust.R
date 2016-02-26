#' Calculate the robust weight for STL
#'
#' Calculate the robust weight for each observation of the time series
#'
#' @param input
#'     The path of input sequence file on HDFS. It should be by location division.
#' @param output
#'     The path of output sequence file on HDFS. It is by time division.
#' @param vari
#'     variable name in string of the response variable
#' @param reduceTask
#'     The reduce task number, also the number of output files. If set to be 0, then there is no shuffle and sort stage after map.
#' @param spill.percent
#'     The threshold usage proportion for both the map output memory buffer and record boundaries index to start the process of spilling to disk.
#' @param io.sort
#'     The size, in megabytes, of the memory buffer to use while sorting map output.
#' @param zero.weight 
#'     value to use as zero for zero weighting
#' @details
#'     Calculate the robust weight for each location
#' @author 
#'     Xiaosu Tong 
#' @examples
#'     FileInput <- "/ln/tongx/Spatial/tmp/tmax/a1950/byseason.inner"
#'     FileOutput <- "/ln/tongx/Spatial/tmp/tmax/a1950/byseason.outer"
#'     \dontrun{
#'        drrobust(FileInput, FileOutput, vari="resp")
#'     }
#' @export

drrobust <- function(input, output, vari, Clcontrol, zero.weight=1e-6) {

  jobW <- list()
  jobW$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      n <- nrow(map.values[[r]])
      mid1 <- floor(n / 2 + 1)
      mid2 <- n - mid1 + 1
      R.abs <- abs(map.values[[r]][, vari] - map.values[[r]]$trend - map.values[[r]]$seasonal)
      h <- 3 * sum(sort(R.abs)[mid1:mid2])
      h9 <- 0.999 * h
      h1 <- 0.001 * h
      w <- (1 - (R.abs / h)^2)^2
      w[R.abs <= h1] <- 1
      w[R.abs >= h9] <- 0
      w[w == 0] <- zero.weight
      w[is.na(w)] <- 1
      map.values[[r]]$weight <- w
      rhcollect(map.keys[[r]], map.values[[r]])
    })
  })
  jobW$parameters <- list(
    vari = vari, zero.weight = zero.weight
  )
  jobW$input <- rhfmt(input, type = "sequence")
  jobW$output <- rhfmt(output, type = "sequence")
  jobW$mapred <- list(
    mapred.reduce.tasks = Clcontrol$reduceTask,  #cdh3,4
    mapreduce.job.reduces = Clcontrol$reduceTask,  #cdh5
    io.sort.mb = Clcontrol$io.sort,
    rhipe_reduce_buff_size = 10000,
    io.sort.spill.percent = Clcontrol$spill.percent 
  )
  jobW$readback <- FALSE
  jobW$jobname <- output
  job.mr <- do.call("rhwatch", jobW)

}