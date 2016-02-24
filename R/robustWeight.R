#' Calculate the robust weight for STL
#'
#' Calculate the robust weight for each observation of the time series
#'
#' @param n
#'     The path of input sequence file on HDFS. It should be by location division.
#' @param Y
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
#' @export

robustWeight <- function(n, Y, seasonal, trend, zero.weight) {

  mid1 <- floor(n / 2 + 1)
  mid2 <- n - mid1 + 1
  R <- Y - seasonal - trend
  R.abs <- abs(R)
  h <- 3 * sum(sort(R.abs)[mid1:mid2])
  h9 <- 0.999 * h
  h1 <- 0.001 * h
  w <- (1 - (R.abs / h)^2)^2
  w[R.abs <= h1] <- 1
  w[R.abs >= h9] <- 0
  w[w == 0] <- zero.weight
  w[is.na(w)] <- 1

  return(w)

}