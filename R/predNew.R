#' @export

predNew <- function(newdata, input, info, model_control=spacetime.control(), cluster_control=mapreduce.control()) {
	
	if(!is.data.frame(newdata)) {
		stop("new locations must be a data.frame")
	}
  output <- "/wsc/tongx/tmp/newpred/spaofit"

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
  if(!all(NM %in% c("lon","lat","elev"))) {
  	stop("new locations have different spatial attributes than the historical data on HDFS")
  }
  newdata$resp <- NA

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

      if(length(map.keys[[r]]) == 1) {
        date <- map.keys[[r]] 
      } else {
        date <- (as.numeric(map.keys[[r]][1]) - 1)*12 + as.numeric(map.keys[[r]][2])
      }

      value <- arrange(as.data.frame(map.values[[r]]), station.id)
      value <- cbind(value, a1950UStinfo[, c("lon","lat","elev")])
      value <- subset(value, select = -c(station.id))
      value <- rbind(value, newdata)
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
        control = loess.control(surface = Mlcontrol$surf, iterations = 2),
        napred = FALSE,
        alltree = TRUE
      )
      newPred <- predloess(lo.fit, newdata = newdata)

      for (i in 1:N) {
        rhcollect(i, c(date, newPred))
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
    mapreduce.job.reduces = 2,  #cdh5
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
  output <- "/wsc/tongx/tmp/newpred/stlfit"

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
        rhcollect(i, c(value[c(i, i+Mlcontrol$n, i+Mlcontrol$n*2)], map.keys[[r]]))
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
    mapreduce.job.reduces = 179,  #cdh5
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
  
  input <- output
  output <- "/wsc/tongx/tmp/newpred/sparfit"

  job3 <- list()
  job3$map <- expression({
    lapply(seq_along(map.keys), function(r) {  
      
      if() {
        value <- arrange(data.frame(matrix(map.values[[r]], ncol=4, byrow=TRUE)), X4, X5)
        names(value) <- c("resp","seasonal","trend","station.id")
        value$remainder <- with(value, resp - trend - seasonal)
      } else {
        rhcollect(map.keys[[r]], map.values[[r]])
      }

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
      rhcollect(reduce.key, combine)
    }
  )



}