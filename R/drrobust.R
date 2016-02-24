#' Calculate the robust weight for STL
#'
#' Calculate the robust weight for each observation of the time series
#'
#' @param input
#'     The path of input sequence file on HDFS. It should be by location division.
#' @param output
#'     The path of output sequence file on HDFS. It is by time division.
#' @param seasonal
#'     The number of the reduce tasks.
#' @param trend
#'     Logical argument, if TRUE, then the elevation attribute from input value data.frame is log2 transformation
#'     If FALSE, a log2 transformation is added to the elevation attribute.
#' @param zero.weight
#'     all parameters that are needed for space-time fitting
#' @details
#'     swaptoTime is used for switch division by location to division by time.
#' @author 
#'     Xiaosu Tong 
#' @examples
#'     FileInput <- "/ln/tongx/Spatial/tmp/tmax/a1950/byseason.season"
#'     FileOutput <- "/ln/tongx/Spatial/tmp/tmax/a1950/byseason.outer"
#'     \dontrun{
#'        drrobust(FileInput, FileOutput, vari="resp")
#'     }
#' @export

drrobust <- function(input, output, vari, reduceTask=0, io.sort.mb=256, io.sort.spill.percent=0.85, zero.weight=1e-6) {

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
    mapred.reduce.tasks = reduceTask,  #cdh3,4
    mapreduce.job.reduces = reduceTask,  #cdh5
    io.sort.mb = io.sort,
    io.sort.spill.percent = spill.percent 
  )
  jobW$readback <- FALSE
  jobW$jobname <- output
  job.mr <- do.call("rhwatch", jobW)

}