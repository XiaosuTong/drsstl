#' Set mapreduce parameter for drSpaceTime fitting
#'
#'  Set control parameters of mapreduce for drSpaceTime fits
#'
#' @param reduceTask
#'     The reduce task number, also the number of output files. If set to be 0, then there is no shuffle and sort stage after map.
#' @param spill_percent
#'     For mapreduce.sort.spill.percent parameter, 
#'     the threshold usage proportion for both the map output memory buffer and record boundaries 
#'     index to start the process of spilling to disk.
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
  reduceTask=0, libLoc = NULL, BLK = 512,
  spill_percent = 0.8, io_sort = 100, task_io_sort_factor = 10,
  reduce_parallelcopies = 5, reduce_shuffle_input_buffer_percent = 0.70,
  reduce_shuffle_merge_percent = 0.66, reduce_merge_inmem = 1000,
  reduce_input_buffer_percent = 0, reduce_buff_read = 150, map_buffer_read = 150,
  reduce_buffer_size = 10000, map_buffer_size= 10000) {
  
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
    reduce_buff_read = reduce_buff_read*2^20,
    reduce_buffer_size = reduce_buffer_size,
    map_buffer_read = map_buffer_read*2^20,
    map_buffer_size = map_buffer_size,
    BLK = BLK*2^20
  )

}