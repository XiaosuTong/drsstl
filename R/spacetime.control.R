#' Set parameter for drSpaceTime fitting
#'
#'  Set control parameters for drSpaceTime fits
#'
#' @param inner
#'     The iteration time for inner loop of stlplus for time dimension fitting
#' @param output
#'     The iteration time for outer loop of stlplus for time dimension fitting
#' @param surface
#'     should the fitted surface be computed exactly or via interpolation from a kd tree?
#' @param lib.loc
#'     the library searching path, sepcifically the path including Spaloess and 
#'     stlplus package on the front end server. If all packages have been pushed
#'     the the tar.gz file on hdfs, then this argument is set to be NULL.
#' @param iterations
#'     the number of iterations used for the space-time back-fitting.
#' @return
#'     A list with space-time fitting parameters.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     spacetime.control()


spacetime.control <- function(inner=4, outer=1, surface = c("interpolate", "direct"), lib.loc=NULL, iterations = 1L) {
  
  list(
  	inner = inner, outer = outer, surface = match.arg(surface), 
  	lib.loc = lib.loc, iterations = iterations
  )

}