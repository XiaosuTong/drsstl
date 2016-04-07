#' @importFrom stats frequency loess median predict quantile weighted.mean time
#' @importFrom utils head stack tail
#' @export

drinner <- function(Inner_input, Inner_output, n, n.p, vari, time, seaname, 
  s.window, s.degree, t.window, t.degree, l.window, l.degree, periodic,
  s.jump, t.jump, l.jump, critfreq, s.blend, t.blend, l.blend, crtI, crtO,
  sub.labels, sub.start, infill, Clcontrol) {

  # start and end indices for after adding in extra n.p before and after 
  st <- n.p + 1
  nd <- n + n.p

  # package the parameters into list
  paras <- list(
    vari = vari, time = time, seaname = seaname, 
    n.p = n.p, n = n, st = st, nd = nd, periodic = periodic,
    s.window = s.window, s.degree = s.degree, s.jump = s.jump, s.blend = s.blend,  
    l.window = l.window, l.degree = l.degree, l.jump = l.jump, l.blend = l.blend, 
    t.window = t.window, t.degree = t.degree, t.jump = t.jump, t.blend = t.blend,
    crtI = crtI, crtO = crtO, Clcontrol=Clcontrol, infill = infill
  )

  # inner loop
  jobIn <- list()
  jobIn$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      value <- plyr::arrange(map.values[[r]], get(time))
      if(infill & crtI == 1 & crtO == 1) {
        Index <- which(is.na(value[, vari]))
        value[Index, vari] <- value$fitted[Index]
        value$flag <- 1
        value$flag[Index] <- 0
        value <- subset(value, select=-c(fitted))
      } 
      notEnoughData <- sum(!is.na(value[, vari])) < s.window 
      if (notEnoughData) {
        stop("at least one of subseries does not have enough observations")
      }else {
        index <- match(map.keys[[r]][2], month.abb)
        value[, seaname] <- map.keys[[r]][2]
        if (crtI == 1 & crtO == 1) {
          value$trend <- 0
          value$weight <- 1
        }  
        cycleSub.length <- nrow(value)
        cycleSub <- value[, vari]

        # detrending
        cycleSub <- cycleSub - value$trend

        cs1 <- as.numeric(head(value[, time], 1)) - 12
        cs2 <- as.numeric(tail(value[, time], 1)) + 12 

        if (periodic) {
          C <- rep(weighted.mean(cycleSub, w = value$weight, na.rm = TRUE), cycleSub.length + 2)
        } else {
          cs.ev <- seq(1, cycleSub.length, by = s.jump)
          if(tail(cs.ev, 1) != cycleSub.length) cs.ev <- c(cs.ev, cycleSub.length)
          cs.ev <- c(0, cs.ev, cycleSub.length + 1)
          C <- drSpaceTime::.loess_stlplus(
            y = cycleSub, span = s.window, degree = s.degree,
            m = cs.ev, weights = value$weight, blend = s.blend,
            jump = s.jump, at = c(0:(cycleSub.length + 1))
          ) 
        }
        Cdf <- data.frame(C = C, t = as.numeric(c(cs1, value[, time], cs2)))
        rhcollect(map.keys[[r]][1], list(value, Cdf))
      }
    })
  })
  jobIn$reduce <- expression(
    pre = {
      combined <- data.frame()
      Ctotal <- data.frame()
      ma3 <- 0
      L <- numeric()
      D <- numeric()
      y_idx <- logical()
      noNa <- logical()
      t.ev <- seq(1, n, by = t.jump)
      l.ev <- seq(1, n, by = l.jump)
      if(tail(l.ev, 1) != n) l.ev <- c(l.ev, n)
      if(tail(t.ev, 1) != n) t.ev <- c(t.ev, n)
    },
    reduce = {
      combined <- rbind(combined, do.call("rbind", lapply(reduce.values, "[[", 1)))
      Ctotal <- rbind(Ctotal, do.call("rbind", lapply(reduce.values, "[[", 2)))
    },
    post = {
      combined <- plyr::arrange(combined, get(time))
      Ctotal <- plyr::arrange(Ctotal, t)
      y_idx <- !is.na(combined[, vari])
      noNA <- all(y_idx)
      ma3 <- drSpaceTime::c_ma(Ctotal$C, n.p)
      L <- drSpaceTime::.loess_stlplus(
        y = ma3, span = l.window, degree = l.degree, m = l.ev, weights = combined$weight, 
        y_idx = y_idx, noNA = noNA, blend = l.blend, jump = l.jump, at = c(1:n)
      )
      combined$seasonal <- Ctotal$C[st:nd] - L
      # Deseasonalize
      D <- combined[, vari] - combined$seasonal
      combined$trend <- drSpaceTime::.loess_stlplus(
        y = D, span = t.window, degree = t.degree, m = t.ev, weights = combined$weight, 
        y_idx = y_idx, noNA = noNA, blend = t.blend, jump = t.jump, at = c(1:n)
      )
      rhcollect(reduce.key, combined)
    }
  )
  jobIn$setup <- expression(
    map = {
      library(plyr, lib.loc=Clcontrol$libLoc)
      library(yaImpute, lib.loc=Clcontrol$libLoc)
      library(drSpaceTime, lib.loc=Clcontrol$libLoc)
    },
    reduce = {
      library(plyr, lib.loc=Clcontrol$libLoc)
      library(yaImpute, lib.loc=Clcontrol$libLoc)
      library(drSpaceTime, lib.loc=Clcontrol$libLoc)
    }
  )
  jobIn$parameters <- paras
  jobIn$input <- rhfmt(Inner_input, type = "sequence")
  jobIn$output <- rhfmt(Inner_output, type = "sequence")
  jobIn$mapred <- list(
    mapred.reduce.tasks = Clcontrol$reduceTask,  #cdh3,4
    mapreduce.job.reduces = Clcontrol$reduceTask,  #cdh5
    mapred.tasktimeout = 0,
    rhipe_reduce_buff_size = 10000,
    io.sort.mb = Clcontrol$io.sort,
    io.sort.spill.percent = Clcontrol$spill.percent 
  )
  jobIn$readback <- FALSE
  jobIn$jobname <- Inner_output
  job.mr <- do.call("rhwatch", jobIn)

}