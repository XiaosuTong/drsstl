#' Prediction at new locations based on the fitting results on HDFS.
#'
#' The prediction at new locations are calculated based on the fitting results
#' saved on HDFS based on the original dataset.
#'
#' @param newdata
#'     A data.frame includes all locations' longitude, latitude, and elevation,
#'     where the prediction is to be calculated.
#' @param input
#'     The path of input file on HDFS. It should be by-month division with all fitting results
#'     of original dataset
#' @param output
#'     The path of output on HDFS where all the intermediate outputs will be saved.
#' @param info
#'     The RData path on HDFS which contains all station metadata of original dataset
#' @param clcontrol
#'     Should be a list object generated from \code{mapreduce.control} function.
#'     The list including all necessary Rhipe parameters and also user tunable
#'     MapReduce parameters.
#' @param mlcontrol
#'     Should be a list object generated from \code{spacetime.control} function.
#'     The list including all necessary smoothing parameters of nonparametric fitting.
#' @author
#'     Xiaosu Tong
#' @export
#' @seealso
#'     \code{\link{spacetime.control}}, \code{\link{mapreduce.control}}
#'
#' @examples
#' \dontrun{
#'     clcontrol <- mapreduce.control(libLoc=NULL, reduceTask=95, io_sort=100, slow_starts = 0.5)
#'     mlcontrol <- spacetime.control(
#'       vari="resp", time="date", n=576, n.p=12, stat_n=7738,
#'       s.window="periodic", t.window = 241, degree=2, span=0.015, Edeg=2
#'     )
#'
#'     new.grid <- expand.grid(
#'       lon = seq(-126, -67, by = 0.1),
#'       lat = seq(25, 49, by = 0.1)
#'     )
#'     instate <- !is.na(map.where("state", new.grid$lon, new.grid$lat))
#'     new.grid <- new.grid[instate, ]
#'
#'     elev.fit <- spaloess( elev ~ lon + lat,
#'       data = station_info,
#'       degree = 2,
#'       span = 0.015,
#'       distance = "Latlong",
#'       normalize = FALSE,
#'       napred = FALSE,
#'       alltree = FALSE,
#'       family="symmetric",
#'       control=loess.control(surface = "direct")
#'     )
#'     grid.fit <- predloess(
#'       object = elev.fit,
#'       newdata = data.frame(
#'         lon = new.grid$lon,
#'         lat = new.grid$lat
#'       )
#'     )
#'     new.grid$elev2 <- log2(grid.fit + 128)
#'
#'     predNew_mr(
#'       newdata=new.grid, input="/tmp/output_bymth", output = "/tmp",
#'       info="/tmp/station_info.RData", mlcontrol=mlcontrol, clcontrol=clcontrol
#'     )
#' }

predNew_mr <- function(newdata, input, output, info, mlcontrol=spacetime.control(), clcontrol=mapreduce.control()) {

  if(!is.data.frame(newdata)) {
    stop("new locations must be a data.frame")
  }

  FileInput <- input
  FileOutput <- file.path(output, "newpred/bymth")

  D <- ncol(newdata)
  NM <- names(newdata)
  N <- nrow(newdata)

  if (D == 2 & mlcontrol$Edeg != 0) {
    stop("elevation is not in the spatial attributes")
  }
  if (D > 3) {
    stop("spatial dimension cannot over 3")
  }
  if (D == 2) {
    newdata$elev <- 1
  }
  if (!all(NM %in% c("lon","lat","elev"))) {
    stop("new locations have different spatial attributes than the historical data on HDFS")
  }

  job1 <- list()
  job1$map <- expression({
    lapply(seq_along(map.values), function(r) {
      if(Mlcontrol$Edeg == 2) {
        newdata$elev2 <- log2(newdata$elev + 128)
        fml <- as.formula("smoothed ~ lon + lat + elev2")
        dropSq <- FALSE
        condParam <- "elev2"
      } else if(Mlcontrol$Edeg == 1) {
        newdata$elev2 <- log2(newdata$elev + 128)
        fml <- as.formula("smoothed ~ lon + lat + elev2")
        dropSq <- "elev2"
        condParam <- "elev2"
      } else if (Mlcontrol$Edeg == 0) {
        fml <- as.formula("smoothed ~ lon + lat")
        dropSq <- FALSE
        condParam <- FALSE
      }

      if (length(map.keys[[r]]) == 2) {
        if(nchar(map.keys[[r]][2]) >= 3) {
          date <- (as.numeric(map.keys[[r]][1]) - 1) * 12 + match(map.keys[[r]][2], month.abb)
        } else {
          date <- (as.numeric(map.keys[[r]][1]) - 1) * 12 + as.numeric(map.keys[[r]][2])
        }
      } else if (length(map.keys[[r]]) == 1) {
        date <- map.keys[[r]]
      }

      value <- arrange(as.data.frame(map.values[[r]]), station.id)
      value <- cbind(value, station_info[, c("lon","lat","elev")])
      value <- subset(value, select = -c(station.id))
      if (Mlcontrol$Edeg != 0) {
        value$elev2 <- log2(value$elev + 128)
      }
      NApred <- any(is.na(value[, Mlcontrol$vari]))

      lo.fit <- spaloess( fml,
        data        = value,
        degree      = Mlcontrol$degree,
        span        = Mlcontrol$span,
        parametric  = condParam,
        drop_square = dropSq,
        family      = Mlcontrol$family,
        normalize   = FALSE,
        distance    = "Latlong",
        control     = loess.control(surface = Mlcontrol$surf, iterations = Mlcontrol$siter, cell = Mlcontrol$cell),
        napred      = NApred,
        alltree     = match.arg(Mlcontrol$surf, c("interpolate", "direct")) == "interpolate"
      )
      if (Mlcontrol$Edeg != 0) {
        newPred <- unname(predloess(
          object = lo.fit,
          newdata = data.frame(
            lon = newdata$lon,
            lat = newdata$lat,
            elev2 = newdata$elev2
          )
        ))
      } else {
        newPred <- unname(predloess(
          object = lo.fit,
          newdata = data.frame(
            lon = newdata$lon,
            lat = newdata$lat
          )
        ))
      }

      for (i in 1:N) {
        rhcollect(i, c(date, newPred[i]))
      }
    })
  })
  job1$reduce <- expression(
    pre = {
      combine <- numeric()
    },
    reduce = {
      combine <- c(combine, do.call("c", reduce.values))
    },
    post = {
      rhcollect(reduce.key, combine)
    }
  )
  job1$parameters <- list(
    newdata = newdata,
    info = info,
    N = N,
    Clcontrol = clcontrol,
    Mlcontrol = mlcontrol
  )
  job1$shared <- c(info)
  job1$setup <- expression(
    map = {
      load(strsplit(info, "/")[[1]][length(strsplit(info, "/")[[1]])])
      suppressMessages(library(plyr, lib.loc=Clcontrol$libLoc))
      suppressMessages(library(Spaloess, lib.loc=Clcontrol$libLoc))
    }
  )
  job1$mapred <- list(
    mapreduce.task.timeout = 0,
    mapreduce.job.reduces = clcontrol$reduceTask,  #cdh5
    mapreduce.map.java.opts = clcontrol$map_jvm,
    mapreduce.map.memory.mb = clcontrol$map_memory,
    dfs.blocksize = clcontrol$BLK,
    rhipe_map_bytes_read = clcontrol$map_buffer_read,
    rhipe_map_buffer_size = clcontrol$map_buffer_size,
    mapreduce.map.output.compress = TRUE,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK"
  )
  job1$input <- rhfmt(FileInput, type="sequence")
  job1$output <- rhfmt(FileOutput, type="sequence")
  job1$mon.sec <- 10
  job1$jobname <- FileOutput
  job1$readback <- FALSE
  do.call("rhwatch", job1)

  FileInput <- FileOutput
  FileOutput <- file.path(output, "newpred/stlfit")

  job2 <- list()
  job2$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      value <- arrange(data.frame(matrix(map.values[[r]], ncol=2, byrow=TRUE)), X1)

      fit <- stlplus::stlplus(
        x=value$X2, t=value$X1, n.p=Mlcontrol$n.p,
        s.window=Mlcontrol$s.window, s.degree=Mlcontrol$s.degree,
        t.window=Mlcontrol$t.window, t.degree=Mlcontrol$t.degree,
        inner=Mlcontrol$inner, outer=Mlcontrol$outer
      )$data
      # value originally is a data.frame with 3 columns, vectorize it
      names(value) <- c(Mlcontrol$time, Mlcontrol$vari)
      value <- unname(unlist(cbind(subset(value, select = -c(date)), subset(fit, select = c(seasonal, trend)))))

      lapply( (1:Mlcontrol$n), function(i) {
        rhcollect(i, c(value[c(i, i + Mlcontrol$n, i + Mlcontrol$n * 2)], i, map.keys[[r]]))
      })

    })
  })
  job2$reduce <- expression(
    pre = {
      combine <- numeric()
    },
    reduce = {
      combine <- c(combine, do.call("c", reduce.values))
    },
    post = {
      rhcollect(reduce.key, combine)
    }
  )
  job2$parameters <- list(
    Mlcontrol = mlcontrol,
    Clcontrol = clcontrol
  )
  job2$setup <- expression(
    map = {
      suppressMessages(library(stlplus, lib.loc=Clcontrol$libLoc))
      suppressMessages(library(plyr, lib.loc=Clcontrol$libLoc))
    }
  )
  job2$input <- rhfmt(FileInput, type = "sequence")
  job2$output <- rhfmt(FileOutput, type = "sequence")
  job2$mapred <- list(
    mapreduce.task.timeout = 0,
    mapreduce.job.reduces = clcontrol$reduceTask,  #cdh5
    mapreduce.map.java.opts = clcontrol$map_jvm,
    mapreduce.map.memory.mb = clcontrol$map_memory,
    dfs.blocksize = clcontrol$BLK,
    rhipe_reduce_buff_size = clcontrol$reduce_buffer_size,
    rhipe_reduce_bytes_read = clcontrol$reduce_buffer_read,
    rhipe_map_buff_size = clcontrol$map_buffer_size,
    rhipe_map_bytes_read = clcontrol$map_buffer_read,
    mapreduce.map.output.compress = TRUE,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK"
  )
  job2$mon.sec <- 10
  job2$readback <- FALSE
  job2$jobname <- FileOutput
  do.call("rhwatch", job2)


  prefix <- strsplit(input, "/")[[1]][1:(length(strsplit(input, "/")[[1]]) - 1)]
  FileInput <- c(file.path(do.call("file.path", as.list(prefix)), "bymthse"), FileOutput)
  FileOutput <- file.path(output, "newpred/merge")


  job3 <- list()
  job3$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      file <- Sys.getenv("mapred.input.file")
      value <- arrange(data.frame(matrix(map.values[[r]], ncol=5, byrow=TRUE)), X4, X5)
      names(value) <- c("smoothed","seasonal","trend", "date", "station.id")
      value$remainder <- with(value, smoothed - trend - seasonal)
      if (grepl(input, file)) {
        value$new <- 1
      } else {
        value$new <- 0
      }
      rhcollect(as.numeric(map.keys[[r]]), value)
    })
  })
  job3$reduce <- expression(
    pre = {
      combine <- data.frame()
    },
    reduce = {
      combine <- rbind(combine, do.call("rbind", reduce.values))
    },
    post = {
      rownames(combine) <- NULL
      rhcollect(reduce.key, combine)
    }
  )
  job3$parameters <- list(
    Mlcontrol = mlcontrol,
    Clcontrol = clcontrol,
    info = info,
    input = FileInput[2]
  )
  job3$shared <- c(info)
  job3$setup <- expression(
    map = {
      load(strsplit(info, "/")[[1]][length(strsplit(info, "/")[[1]])])
      suppressMessages(library(plyr, lib.loc=Clcontrol$libLoc))
      suppressMessages(library(Spaloess, lib.loc=Clcontrol$libLoc))
    }
  )
  job3$mapred <- list(
    mapreduce.task.timeout = 0,
    mapreduce.job.reduces = clcontrol$reduceTask,  #cdh5
    mapreduce.map.java.opts = clcontrol$map_jvm,
    mapreduce.map.memory.mb = clcontrol$map_memory,
    dfs.blocksize = clcontrol$BLK,
    rhipe_map_bytes_read = clcontrol$map_buffer_read,
    rhipe_map_buffer_size = clcontrol$map_buffer_size,
    mapreduce.map.output.compress = TRUE,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK"
  )
  job3$input <- rhfmt(FileInput, type="sequence")
  job3$output <- rhfmt(FileOutput, type="sequence")
  job3$mon.sec <- 10
  job3$jobname <- FileOutput
  job3$readback <- FALSE
  do.call("rhwatch", job3)


  FileInput <- FileOutput
  FileOutput <- file.path(output, "newpred/result_bymth")


  job4 <- list()
  job4$map <- expression({
    lapply(seq_along(map.values), function(r) {
      if(Mlcontrol$Edeg == 2) {
        newdata$elev2 <- log2(newdata$elev + 128)
        station_info$elev2 <- log2(station_info$elev + 128)
        fml <- as.formula("remainder ~ lon + lat + elev2")
        dropSq <- FALSE
        condParam <- "elev2"
      } else if(Mlcontrol$Edeg == 1) {
        newdata$elev2 <- log2(newdata$elev + 128)
        station_info$elev2 <- log2(station_info$elev + 128)
        fml <- as.formula("remainder ~ lon + lat + elev2")
        dropSq <- "elev2"
        condParam <- "elev2"
      } else if (Mlcontrol$Edeg == 0) {
        fml <- as.formula("remainder ~ lon + lat")
        dropSq <- FALSE
        condParam <- FALSE
      }

      value <- arrange(map.values[[r]], new, station.id)
      if (Mlcontrol$Edeg != 0) {
        value <- cbind(value, rbind(station_info[, c("lon","lat","elev2")], newdata[, c("lon","lat","elev2")]))
      } else {
        value <- cbind(value, rbind(station_info[, c("lon","lat")], newdata[, c("lon","lat")]))
      }
      lo.fit <- spaloess( fml,
        data        = value,
        degree      = Mlcontrol$degree,
        span        = Mlcontrol$span,
        parametric  = condParam,
        drop_square = dropSq,
        family      = Mlcontrol$family,
        normalize   = FALSE,
        distance    = "Latlong",
        control     = loess.control(surface = Mlcontrol$surf, iterations = Mlcontrol$siter, cell = Mlcontrol$cell),
        napred      = FALSE,
        alltree     = match.arg(Mlcontrol$surf, c("interpolate", "direct")) == "interpolate"
      )
      value$Rspa <- lo.fit$fitted
      value <- subset(value, new == 1)
      if (Mlcontrol$Edeg != 0) {
        value <- subset(value, select = -c(remainder, lon, lat, elev2, date, new))[,c(4,1,2,3,5)]
      } else {
        value <- subset(value, select = -c(remainder, lon, lat, date, new))[,c(4,1,2,3,5)]
      }
      rownames(value) <- NULL
      rhcollect(map.keys[[r]], value)

    })
  })
  job4$parameters <- list(
    Mlcontrol = mlcontrol,
    Clcontrol = clcontrol,
    info = info,
    newdata = newdata
  )
  job4$shared <- c(info)
  job4$setup <- expression(
    map = {
      load(strsplit(info, "/")[[1]][length(strsplit(info, "/")[[1]])])
      suppressMessages(library(plyr, lib.loc=Clcontrol$libLoc))
      suppressMessages(library(Spaloess, lib.loc=Clcontrol$libLoc))
    }
  )
  job4$mapred <- list(
    mapreduce.task.timeout = 0,
    mapreduce.job.reduces = 0,  #cdh5
    mapreduce.map.java.opts = clcontrol$map_jvm,
    mapreduce.map.memory.mb = clcontrol$map_memory,
    dfs.blocksize = clcontrol$BLK,
    rhipe_map_bytes_read = clcontrol$map_buffer_read,
    rhipe_map_buffer_size = clcontrol$map_buffer_size,
    mapreduce.map.output.compress = TRUE,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK"
  )
  job4$input <- rhfmt(FileInput, type="sequence")
  job4$output <- rhfmt(FileOutput, type="sequence")
  job4$mon.sec <- 10
  job4$jobname <- FileOutput
  job4$readback <- FALSE
  do.call("rhwatch", job4)


  FileInput <- FileOutput
  FileOutput <- file.path(output, "newpred/result_bystat")


  job5 <- list()
  job5$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      map.values[[r]]$date <- map.keys[[r]]
      lapply(1:nrow(map.values[[r]]), function(i) {
        rhcollect(map.values[[r]][i, 1], map.values[[r]][i, -1])
      })
    })
  })
  job5$reduce <- expression(
    pre = {
      combine <- data.frame()
    },
    reduce = {
      combine <- rbind(combine, do.call("rbind", reduce.values))
    },
    post = {
      combine <- arrange(combine, date)
      rownames(combine) <- NULL
      rhcollect(reduce.key, combine)
    }
  )
  job5$setup <- expression(
    reduce = {
      suppressMessages(library(plyr, lib.loc=Clcontrol$libLoc))
    }
  )
  job5$parameters <- list(
    Clcontrol = clcontrol
  )
  job5$mapred <- list(
    mapreduce.map.java.opts = clcontrol$map_jvm,
    mapreduce.map.memory.mb = clcontrol$map_memory,
    mapreduce.reduce.java.opts = clcontrol$reduce_jvm,
    mapreduce.reduce.memory.mb = clcontrol$reduce_memory,
    mapreduce.job.reduces = clcontrol$reduceTask,  #cdh5
    dfs.blocksize = clcontrol$BLK,
    mapreduce.task.io.sort.mb = clcontrol$io_sort,
    mapreduce.map.sort.spill.percent = clcontrol$spill_percent,
    mapreduce.reduce.shuffle.parallelcopies = clcontrol$reduce_parallelcopies,
    mapreduce.task.io.sort.factor = clcontrol$task_io_sort_factor,
    mapreduce.reduce.shuffle.merge.percent = clcontrol$reduce_shuffle_merge_percent,
    mapreduce.reduce.merge.inmem.threshold = clcontrol$reduce_merge_inmem,
    mapreduce.reduce.input.buffer.percent = clcontrol$reduce_input_buffer_percent,
    mapreduce.reduce.shuffle.input.buffer.percent = clcontrol$reduce_shuffle_input_buffer_percent,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK",
    mapreduce.task.timeout  = 0,
    mapreduce.job.reduce.slowstart.completedmaps = clcontrol$slow_starts,
    rhipe_reduce_buff_size = clcontrol$reduce_buffer_size,
    rhipe_reduce_bytes_read = clcontrol$reduce_buffer_read,
    rhipe_map_buff_size = clcontrol$map_buffer_size,
    rhipe_map_bytes_read = clcontrol$map_buffer_read
  )
  job5$combiner <- TRUE
  job5$input <- rhfmt(FileInput, type="sequence")
  job5$output <- rhfmt(FileOutput, type="sequence")
  job5$mon.sec <- 10
  job5$jobname <- FileOutput
  job5$readback <- FALSE
  do.call("rhwatch", job5)

}
