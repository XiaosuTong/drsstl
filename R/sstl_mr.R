#' Apply sstl routine to dataset saved on HDFS
#'
#' Input raw text file on HDFS is the original input dataset. After a series of MapReudce
#' jobs, the final fitting results are saved as output_bymth and output_bystat subdirectory
#' inside of output path on HDFS.
#'
#' @param input
#'     The input path of raw text file on HDFS
#' @param mlcontrol
#'     Should be a list object generated from \code{spacetime.control} function.
#'     The list including all necessary smoothing parameters of nonparametric fitting.
#' @param clcontrol
#'     Should be a list object generated from \code{mapreduce.control} function.
#'     The list including all necessary Rhipe parameters and also user tunable 
#'     MapReduce parameters.
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
#'       map_jvm = "-Xmx3072m", reduce_jvm = "-Xmx4096m", 
#'       map_memory = 5120, reduce_memory = 5120,
#'       reduce_input_buffer_percent=0.4, reduce_parallelcopies=10,
#'       reduce_merge_inmem=0, task_io_sort_factor=100,
#'       spill_percent=0.9, reduce_shuffle_input_buffer_percent = 0.8,
#'       reduce_shuffle_merge_percent = 0.4
#'     )
#'     sstl_mr("/tmp/tmax.txt","/tmp/output", mlcontrol=mcontrol, clcontrol=clcontrol)

sstl_mr <- function(input, output, stat_info, mlcontrol=spacetime.control(), clcontrol=mapreduce.control()) {

  FileInput <- input
  FileOutput <- file.path(output, "bymth")

  readIn(
  	input = FileInput, 
  	output = FileOutput, 
  	info   = stat_info, 
  	cluster_control = clcontrol
  )

  FileInput <- FileOutput
  FileOutput <- file.path(output, "bymthfit")

  spaofit(
  	input  = FileInput, 
  	output = FileOutput, 
  	info   = stat_info, 
  	model_control   = mlcontrol, 
  	cluster_control = clcontrol
  )

  FileInput <- FileOutput
  FileOutput <- file.path(output, "bystat")

  swaptoLoc(
  	input = FileInput, 
  	output = FileOutput, 
  	cluster_control = clcontrol
  )


  FileInput <- FileOutput
  FileOutput <- file.path(output, "bystatfit")

  stlfit(
  	input  = FileInput, 
  	output = FileOutput, 
  	model_control = mlcontrol, 
  	cluster_control = clcontrol
  )

  
  FileInput <- FileOutput
  FileOutput <- file.path(output, "bymthse")

  swaptoTime(
  	input  = FileInput, 
  	output = FileOutput, 
  	cluster_control = clcontrol, 
  	model_control   = mlcontrol
  )


  FileInput <- FileOutput
  FileOutput <- file.path(output, "output_bymth")

  sparfit(
  	input  = FileInput, 
  	output = FileOutput,
    info   = stat_info,
    model_control = mlcontrol, 
    cluster_control = clcontrol
  )  


  FileInput <- FileOutput
  FileOutput <- file.path(output, "output_bystat")

  swaptoLoc(
  	input  = FileInput, 
  	output = FileOutput, 
  	final = TRUE, 
  	cluster_control = clcontrol
  )

  return(NULL)

}