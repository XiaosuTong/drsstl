utils::globalVariables(c("rhfmt"))

#' Apply the spatial loess fitting to the original observations at
#' all locations in each month in parallel
#'
#' Call \code{spaloess} function on the spatial domain in each month in parallel.
#' Every spatial domain uses the same smoothing parameters. NA observations will be
#' imputed.
#'
#' @param input
#'     The path of input sequence file on HDFS. It should be by-month division.
#' @param output
#'     The path of output sequence file on HDFS. It is also by-month division but with
#'     seasonal and trend components
#' @param info
#'     The RData on HDFS which contains all station metadata. Make sure
#'     copy the RData of station_info to HDFS first using rhput.
#' @param cluster_control
#'     Should be a list object generated from \code{mapreduce.control} function.
#'     The list including all necessary Rhipe parameters and also user tunable
#'     MapReduce parameters.
#' @param model_control
#'     Should be a list object generated from \code{spacetime.control} function.
#'     The list including all necessary smoothing parameters of nonparametric fitting.
#' @author
#'     Xiaosu Tong
#' @export
#' @examples
#' \dontrun{
#'     FileInput <- "/tmp/bymth"
#'     FileOutput <- "/tmp/bymthfit"
#'     ccontrol <- mapreduce.control(libLoc=NULL, reduceTask=0)
#'     mcontrol <- spacetime.control(
#'       vari="resp", time="date", n=576, n.p=12, stat_n=7738,
#'       s.window=13, t.window = 241, degree=2, span=0.015, Edeg=2
#'     )
#'
#'     spaofit(
#'       FileInput, FileOutput,
#'       info="/tmp/station_info.RData",
#'       model_control=mcontrol, cluster_control=ccontrol
#'     )
#' }
spaofit <- function(input, output, info, model_control=spacetime.control(), cluster_control=mapreduce.control()) {

  job <- list()
  job$map <- expression({
    lapply(seq_along(map.values), function(r) {
      if(Mlcontrol$Edeg == 2) {
        fml <- as.formula("resp ~ lon + lat + elev2")
        dropSq <- FALSE
        condParam <- "elev2"
      } else if(Mlcontrol$Edeg == 1) {
        fml <- as.formula("resp ~ lon + lat + elev2")
        dropSq <- "elev2"
        condParam <- "elev2"
      } else if (Mlcontrol$Edeg == 0) {
        fml <- as.formula("resp ~ lon + lat")
        dropSq <- FALSE
        condParam <- FALSE
      }

      value <- arrange(as.data.frame(map.values[[r]]), station.id)
      value <- cbind(value, station_info[, c("lon","lat","elev")])
      value$elev2 <- log2(value$elev + 128)
      NApred <- any(is.na(value$resp))

      lo.fit <- spaloess( fml,
        data        = value,
        degree      = Mlcontrol$degree,
        span        = Mlcontrol$span,
        parametric  = condParam,
        drop.square = dropSq,
        family      = Mlcontrol$family,
        normalize   = FALSE,
        distance    = "Latlong",
        control     = loess.control(surface = Mlcontrol$surf, iterations = Mlcontrol$siter, cell = Mlcontrol$cell),
        napred      = NApred,
        alltree     = match.arg(Mlcontrol$surf, c("interpolate", "direct")) == "interpolate"
      )

      if(NApred) {
        indx <- which(!is.na(value$resp))
        rst <- rbind(
          cbind(indx, fitted=lo.fit$fitted),
          cbind(which(is.na(value$resp)), fitted=lo.fit$pred$fitted)
        )
        rst <- arrange(as.data.frame(rst), indx)
        rhcollect(map.keys[[r]], rst$fitted)
      } else {
        rhcollect(map.keys[[r]], lo.fit$fitted)
      }

    })
  })
  job$parameters <- list(
    Mlcontrol = model_control,
    Clcontrol = cluster_control,
    info = info
  )
  job$shared <- c(info)
  job$setup <- expression(
    map = {
      load(strsplit(info, "/")[[1]][length(strsplit(info, "/")[[1]])])
      suppressMessages(library(plyr, lib.loc=Clcontrol$libLoc))
      library(Spaloess, lib.loc=Clcontrol$libLoc)
    }
  )
  job$mapred <- list(
    mapreduce.task.timeout = 0,
    mapreduce.job.reduces = cluster_control$reduceTask,  #cdh5
    mapreduce.map.java.opts = cluster_control$map_jvm,
    mapreduce.map.memory.mb = cluster_control$map_memory,
    dfs.blocksize = cluster_control$BLK,
    rhipe_map_bytes_read = cluster_control$map_buffer_read,
    rhipe_map_buffer_size = cluster_control$map_buffer_size,
    mapreduce.map.output.compress = TRUE,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK"
  )
  job$input <- rhfmt(input, type="sequence")
  job$output <- rhfmt(output, type="sequence")
  job$mon.sec <- 10
  job$jobname <- output
  job$readback <- FALSE

  job.mr <- do.call("rhwatch", job)

  #return(job.mr[[1]]$jobid)

}
