#' Set mapreduce parameter for drsstl fitting
#'
#'  Set control parameters of mapreduce for drsstl fits
#'
#' @param reduceTask
#'     The reduce task number, also the number of output files. If set to be 0, then there is no shuffle and sort stage after map.
#' @param libLoc
#'     The library path where each worker should go to load R packages. If all R packages have been push to HDFS, leave this 
#'     parameter as default NULL.
#' @param BLK
#'     The block size of output file on HDFS.
#' @param map_jvm,reduce_jvm
#'     For mapreduce.map.java.opts and mapreduce.reduce.java.opts
#' @param map_memory,reduce_memory
#'     For mapreduce.map.memory.mb and mapreduce.reduce.memory.mb
#' @param slow_starts
#'     For mapreduce.job.reduce.slowstart.completedmaps
#' @param reduce_buffer_read,map_buffer_read,reduce_buffer_size,map_buffer_size
#'     For all rhipe arguments.
#' @param spill_percent
#'     For mapreduce.sort.spill.percent parameter
#' @param io_sort
#'     For mapreduce.task.io.sort.mb, the size, in megabytes, of the memory buffer to use while sorting map output.
#' @param task_io_sort_factor
#'     For mapreduce.task.io.sort.factor
#' @param reduce_parallelcopies
#'     For mapreduce.reduce.shuffle.parallelcopies
#' @param reduce_shuffle_input_buffer_percent
#'     For mapreduce.reduce.shuffle.input.buffer.percent
#' @param reduce_shuffle_merge_percent
#'     For mapreduce.reduce.shuffle.merge.percent
#' @param reduce_merge_inmem
#'     For mapreduce.reduce.merge.inmem.shreshold
#' @param reduce_input_buffer_percent
#'     For mapreduce.reduce.input.buffer.percent
#' @return
#'     A list with mapreduce tuning parameters.
#' @author
#'     Xiaosu Tong
#' @export
#' @examples
#'     mapreduce.control()

mapreduce.control <- function(
  reduceTask = 0, libLoc = NULL, BLK = 128, map_jvm = "-Xmx200m", reduce_jvm = "-Xmx200m",
  map_memory = 1024, reduce_memory = 1024, slow_starts = 0.5,
  spill_percent = 0.8, io_sort = 128, task_io_sort_factor = 100,
  reduce_parallelcopies = 5, reduce_shuffle_input_buffer_percent = 0.70,
  reduce_shuffle_merge_percent = 0.66, reduce_merge_inmem = 0,
  reduce_input_buffer_percent = 0, reduce_buffer_read = 150, map_buffer_read = 150,
  reduce_buffer_size = 10000, map_buffer_size = 10000) {

  list(
    reduceTask = reduceTask, libLoc=libLoc,
    # three parameters control the map spill stage
    spill_percent = spill_percent, io_sort = io_sort, task_io_sort_factor = task_io_sort_factor,
    # mapreduce.reduce.shuffle.parallelcopies
    reduce_parallelcopies = reduce_parallelcopies,
    # mapreduce.reduce.shuffle.input.buffer.percent
    reduce_shuffle_input_buffer_percent = reduce_shuffle_input_buffer_percent,
    # mapreduce.reduce.shuffle.merge.percent
    reduce_shuffle_merge_percent = reduce_shuffle_merge_percent,
    # mapreduce.reduce.merge.inmem.shreshold
    reduce_merge_inmem = reduce_merge_inmem,
    # mapreduce.reduce.input.buffer.percent
    reduce_input_buffer_percent = reduce_input_buffer_percent,
    # Rhipe argument
    reduce_buffer_read = reduce_buffer_read * 2 ^ 20,
    reduce_buffer_size = reduce_buffer_size,
    map_buffer_read = map_buffer_read * 2 ^ 20,
    map_buffer_size = map_buffer_size,
    BLK = BLK * 2 ^ 20,
    # mapreduce.map.java.opts
    map_jvm = map_jvm,
    reduce_jvm = reduce_jvm,
    # mapreduce.map.memory.mb
    map_memory = map_memory,
    reduce_memory = reduce_memory,
    slow_starts = slow_starts
  )

}
