#' Set mapreduce parameter for drSpaceTime fitting
#'
#'  Set control parameters of mapreduce for drSpaceTime fits
#'
#' @param reduceTask
#'     The reduce task number, also the number of output files. If set to be 0, then there is no shuffle and sort stage after map.
#' @param spill.percent
#'     The threshold usage proportion for both the map output memory buffer and record boundaries index to start the process of spilling to disk.
#' @param io.sort
#'     The size, in megabytes, of the memory buffer to use while sorting map output.
#' @return
#'     A list with space-time fitting parameters.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     mapreduce.control()

mapreduce.control <- function(reduceTask=0, spill.percent = 0.8, io.sort = 256, libLoc = NULL) {
  
  list(reduceTask = reduceTask, spill.percent = spill.percent, io.sort = io.sort, libLoc=libLoc)

}