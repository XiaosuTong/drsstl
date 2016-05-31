#' Apply sstl routine to spatial-temporal dataset
#'
#' sstl routine is applied to a spatial-temporal dataset either in memory or on HDFS.
#'
#' @param data
#'     The input path of raw text file on HDFS or a data.frame object in memory
#' @param output
#'     The output path of fitting results on HDFS. If data is a data.frame object,
#'     the output should be set as default NULL. Since the drsstl function will return
#'     the fitting results in memory.
#' @param model_control
#'     Should be a list object generated from \code{spacetime.control} function.
#'     The list including all necessary smoothing parameters of nonparametric fitting.
#' @param cluster_control
#'     Should be a list object generated from \code{mapreduce.control} function.
#'     The list including all necessary Rhipe parameters and also user tunable 
#'     MapReduce parameters. It is only necessary for data on HDFS situation. If data
#'     is data.frame in memory, this parameter should be kept as default NULL.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     head(tmax_all)
#'     mcontrol <- spacetime.control(
#'       vari="tmax", n=576, n.p=12, stat_n=7738,
#'       s.window=13, t.window = 241, degree=2, span=0.015, Edeg=2
#'     )
#'     ccontrol <- mapreduce.control(
#'       libLoc=lib.loc, reduceTask=169, io_sort=512, BLK=128, slow_starts = 0.5,
#'       map_jvm = "-Xmx200m", reduce_jvm = "-Xmx200m", 
#'       map_memory = 1024, reduce_memory = 1024,
#'       reduce_input_buffer_percent=0.4, reduce_parallelcopies=10,
#'       reduce_merge_inmem=0, task_io_sort_factor=100,
#'       spill_percent=0.9, reduce_shuffle_input_buffer_percent = 0.8,
#'       reduce_shuffle_merge_percent = 0.4
#'     )
#'
#'     If the data is on HDFS
#'     drsstl("/tmp/tmax.txt","/tmp/output", model_control=mcontrol, cluster_control=ccontrol)
#'
#'     If the data is tmax_all which is in memory
#'     drsstl(tmax_all, model_control=mcontrol, cluster_control=ccontrol)

drsstl <- function(data, output = NULL, model_control=spacetime.control(), cluster_control=NULL) {

  if(class(data) == "data.frame") {

    rst <- sstl_local(data = data, mlcontrol=model_control)
    return(rst)
    
  } else if (class(data) == "character") {

    if(is.null(output)) {
      stop("An output path on HDFS should be specified")
    }
    if(is.null(cluster_clcontrol)) {
      stop("A cluster control must be specified for data on HDFS")
    }

    sstl_mr(input = data, output = output, mlcontrol=model_control, clcontrol=cluster_clcontrol)

  } else {
    stop("The input data should be either a data.frame in memory or a HDFS path of input data")
  }


}