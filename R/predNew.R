#' @export

predNew <- function(newdata, input, info, model_control=spacetime.control(), cluster_control=mapreduce.control()) {
	
	if(!is.data.frame(newdata)) {
		stop("new locations must be a data.frame")
	}
  output <- "/wsc/tongx/spatem/tmax/newpred/spaofit"

  D <- ncol(newdata)
  NM <- names(newdata)
  N <- nrow(newdata)

  if(D == 2 & model_control$Edeg != 0) {
    stop("elevation is not in the spatial attributes")
  }
  if(D > 3) {
  	stop("spatial dimension cannot over 3")
  }
  if(D == 2) {
  	newdata$elev <- 1
  }
  if(!all(NM %in% c("lon","lat","elev2"))) {
  	stop("new locations have different spatial attributes than the historical data on HDFS")
  }

  job1 <- list()
  job1$map <- expression({
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

      if(nchar(map.keys[[r]][2]) >= 3) {
        date <- (as.numeric(map.keys[[r]][1]) - 1)*12 + match(map.keys[[r]][2], month.abb)
      } else {
        date <- (as.numeric(map.keys[[r]][1]) - 1)*12 + as.numeric(map.keys[[r]][2])
      }

      value <- arrange(as.data.frame(map.values[[r]]), station.id)
      value <- cbind(value, a1950UStinfo[, c("lon","lat","elev")])
      value <- subset(value, select = -c(station.id))
      value$elev2 <- log2(value$elev + 128)
      lo.fit <- spaloess( fml, 
        data    = value, 
        degree  = Mlcontrol$degree, 
        span    = Mlcontrol$span,
        para    = condParam,
        drop    = dropSq,
        family  = "symmetric",
        normalize = FALSE,
        distance = "Latlong",
        control = loess.control(surface = Mlcontrol$surf),
        napred = FALSE,
        alltree = TRUE
      )
      newPred <- unname(predloess(
        object = lo.fit,
        newdata = data.frame(
          lon = newdata$lon,
          lat = newdata$lat,
          elev2 = newdata$elev2
        )
      ))

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
  	Clcontrol = cluster_control,
    Mlcontrol = model_control
  )
  job1$shared <- c(info)
  job1$setup <- expression(
    map = {
      load(strsplit(info, "/")[[1]][length(strsplit(info, "/")[[1]])])
      suppressMessages(library(plyr, lib.loc=Clcontrol$libLoc))
      library(Spaloess, lib.loc=Clcontrol$libLoc)
    }
  )
  job1$mapred <- list(
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
  job1$input <- rhfmt(input, type="sequence")
  job1$output <- rhfmt(output, type="sequence")
  job1$mon.sec <- 10
  job1$jobname <- output
  job1$readback <- FALSE  
  job.mr <- do.call("rhwatch", job1)  

  input <- output
  output <- "/wsc/tongx/spatem/tmax/newpred/stlfit"

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
      
      lapply((1:Mlcontrol$n), function(i) {
        rhcollect(i, c(value[c(i, i+Mlcontrol$n, i+Mlcontrol$n*2)], i, map.keys[[r]]))
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
    Mlcontrol = model_control,
    Clcontrol = cluster_control
  )
  job2$setup <- expression(
    map = {
      suppressMessages(library(stlplus, lib.loc=Clcontrol$libLoc))
      library(plyr, lib.loc=Clcontrol$libLoc)
    }
  )
  job2$input <- rhfmt(input, type = "sequence")
  job2$output <- rhfmt(output, type = "sequence")
  job2$mapred <- list(
    mapreduce.task.timeout = 0,
    mapreduce.job.reduces = cluster_control$reduceTask,  #cdh5
    mapreduce.map.java.opts = cluster_control$map_jvm,
    mapreduce.map.memory.mb = cluster_control$map_memory,     
    dfs.blocksize = cluster_control$BLK,
    rhipe_reduce_buff_size = cluster_control$reduce_buffer_size,
    rhipe_reduce_bytes_read = cluster_control$reduce_buffer_read,
    rhipe_map_buff_size = cluster_control$map_buffer_size, 
    rhipe_map_bytes_read = cluster_control$map_buffer_read,
    mapreduce.map.output.compress = TRUE,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK"
  )
  job2$mon.sec <- 10
  job2$readback <- FALSE
  job2$jobname <- output
  job.mr <- do.call("rhwatch", job2)
  

  input <- c(output, "/wsc/tongx/spatem/tmax/simo/bymthse256")
  output <- "/wsc/tongx/spatem/tmax/newpred/merge"


  job3 <- list()
  job3$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      file <- Sys.getenv("mapred.input.file")
      value <- arrange(data.frame(matrix(map.values[[r]], ncol=5, byrow=TRUE)), X4, X5)
      names(value) <- c("resp","seasonal","trend", "date", "station.id")
      value$remainder <- with(value, resp - trend - seasonal)
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
    Mlcontrol = model_control,
    Clcontrol = cluster_control,
    info = info,
    input = input
  )
  job3$shared <- c(info)
  job3$setup <- expression(
    map = {
      load(strsplit(info, "/")[[1]][length(strsplit(info, "/")[[1]])])
      suppressMessages(library(plyr, lib.loc=Clcontrol$libLoc))
      library(Spaloess, lib.loc=Clcontrol$libLoc)
    }
  )
  job3$mapred <- list(
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
  job3$input <- rhfmt(input, type="sequence")
  job3$output <- rhfmt(output, type="sequence")
  job3$mon.sec <- 10
  job3$jobname <- output
  job3$readback <- FALSE  
  job.mr <- do.call("rhwatch", job3)  


  input <- output
  output <- "/wsc/tongx/spatem/tmax/newpred/sparfit"


  job4 <- list()
  job4$map <- expression({
    lapply(seq_along(map.values), function(r) {
      if(Mlcontrol$Edeg == 2) {
        fml <- as.formula("remainder ~ lon + lat + elev2")
        dropSq <- FALSE
        condParam <- "elev2"
      } else if(Mlcontrol$Edeg == 1) {
        fml <- as.formula("remainder ~ lon + lat + elev2")
        dropSq <- "elev2"
        condParam <- "elev2"
      } else if (Mlcontrol$Edeg == 0) {
        fml <- as.formula("remainder ~ lon + lat")
        dropSq <- FALSE
        condParam <- FALSE
      }
      
      value <- arrange(map.values[[r]], new, station.id)
      a1950UStinfo$elev2 <- log2(a1950UStinfo$elev + 128)
      value <- cbind(value, rbind(a1950UStinfo[, c("lon","lat","elev2")], newdata))

      lo.fit <- spaloess( fml, 
        data    = value, 
        degree  = Mlcontrol$degree, 
        span    = Mlcontrol$span,
        para    = condParam,
        drop    = dropSq,
        family  = "symmetric",
        normalize = FALSE,
        distance = "Latlong",
        control = loess.control(surface = Mlcontrol$surf),
        napred = FALSE,
        alltree = FALSE
      )
      value$Rspa <- lo.fit$fitted
      value <- subset(value, new == 1)
      rhcollect(map.keys[[r]], subset(value, select = -c(remainder, lon, lat, elev2, date, new)))

    })
  })
  job4$parameters <- list(
    Mlcontrol = model_control,
    Clcontrol = cluster_control,
    info = info,
    newdata = newdata
  )
  job4$shared <- c(info)
  job4$setup <- expression(
    map = {
      load(strsplit(info, "/")[[1]][length(strsplit(info, "/")[[1]])])
      suppressMessages(library(plyr, lib.loc=Clcontrol$libLoc))
      library(Spaloess, lib.loc=Clcontrol$libLoc)
    }
  )
  job4$mapred <- list(
    mapreduce.task.timeout = 0,
    mapreduce.job.reduces = 0,  #cdh5
    mapreduce.map.java.opts = cluster_control$map_jvm,
    mapreduce.map.memory.mb = cluster_control$map_memory,     
    dfs.blocksize = cluster_control$BLK,
    rhipe_map_bytes_read = cluster_control$map_buffer_read,
    rhipe_map_buffer_size = cluster_control$map_buffer_size,
    mapreduce.map.output.compress = TRUE,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK"
  )
  job4$input <- rhfmt(input, type="sequence")
  job4$output <- rhfmt(output, type="sequence")
  job4$mon.sec <- 10
  job4$jobname <- output
  job4$readback <- FALSE  
  job.mr <- do.call("rhwatch", job4)  

  
  input <- output
  output <- "/wsc/tongx/spatem/tmax/newpred/sparfit.bystat"


  job5 <- list()
  job5$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      map.values[[r]]$date <- map.keys[[r]]
      lapply(1:nrow(map.values[[r]]), function(i) {
        rhcollect(map.values[[r]][i, 4], map.values[[r]][i, -4])
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
      rownames(combine) <- NULL
      rhcollect(reduce.key, combine)
    }
  )
  job5$mapred <- list(
    mapreduce.map.java.opts = cluster_control$map_jvm,
    mapreduce.map.memory.mb = cluster_control$map_memory, 
    mapreduce.reduce.java.opts = cluster_control$reduce_jvm,
    mapreduce.reduce.memory.mb = cluster_control$reduce_memory,
    mapreduce.job.reduces = cluster_control$reduceTask,  #cdh5
    dfs.blocksize = cluster_control$BLK,
    mapreduce.task.io.sort.mb = cluster_control$io_sort,
    mapreduce.map.sort.spill.percent = cluster_control$spill_percent,
    mapreduce.reduce.shuffle.parallelcopies = cluster_control$reduce_parallelcopies,
    mapreduce.task.io.sort.factor = cluster_control$task_io_sort_factor,
    mapreduce.reduce.shuffle.merge.percent = cluster_control$reduce_shuffle_merge_percent,
    mapreduce.reduce.merge.inmem.threshold = cluster_control$reduce_merge_inmem,
    mapreduce.reduce.input.buffer.percent = cluster_control$reduce_input_buffer_percent,
    mapreduce.reduce.shuffle.input.buffer.percent = cluster_control$reduce_shuffle_input_buffer_percent,
    mapreduce.output.fileoutputformat.compress.type = "BLOCK",
    mapreduce.task.timeout  = 0,
    mapreduce.job.reduce.slowstart.completedmaps = cluster_control$slow_starts,
    rhipe_reduce_buff_size = cluster_control$reduce_buffer_size,
    rhipe_reduce_bytes_read = cluster_control$reduce_buffer_read,
    rhipe_map_buff_size = cluster_control$map_buffer_size, 
    rhipe_map_bytes_read = cluster_control$map_buffer_read
  )
  job5$combiner <- TRUE
  job5$input <- rhfmt(input, type="sequence")
  job5$output <- rhfmt(output, type="sequence")
  job5$mon.sec <- 10
  job5$jobname <- output
  job5$readback <- FALSE  
  job.mr <- do.call("rhwatch", job5)  

}

#model_control <- spacetime.control(
#      vari="resp", time="date", seaname="month", n=576, n.p=12, 
#      s.window="periodic", t.window = 241, degree=2, span=0.015, Edeg=2
#    )#

#cluster_control <- mapreduce.control(
#        libLoc=lib.loc, reduceTask=45, io_sort=512, BLK=128, slow_starts = 0.9,
#        map_jvm = "-Xmx3584m", reduce_jvm = "-Xmx4096m", map_memory = 5120, reduce_memory = 5120,
#        reduce_input_buffer_percent=0.8, reduce_parallelcopies=20,
#        reduce_merge_inmem=0, task_io_sort_factor=50,
#        spill_percent=0.8, reduce_shuffle_input_buffer_percent = 0.8,
#        reduce_shuffle_merge_percent = 0.8,
#        reduce_buffer_read = 100, map_buffer_read = 100,
#        reduce_buffer_size = 10000, map_buffer_size = 10000
#      )

