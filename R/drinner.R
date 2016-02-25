#' Inner loop of Seasonal Trend Decomposition of Time Series by Loess
#'
#' The inner loop of STL. A new implementation of STL. Allows for NA values, local quadratic smoothing, post-trend smoothing, and endpoint blending. The usage is very similar to that of R's built-in \code{stl()}.
#' Allows seasonal fitting in parallel.
#' 
#' @param Inner_input 
#'     the HDFS path of input files for inner loop
#' @param Inner_output 
#'     the HDFS path of output files for inner loop
#' @param n
#'     the number of all observation in the series
#' @param n.p 
#'     periodicity of the seasonal component. In R's \code{stl} function, this is the frequency of the time series.
#' @param s.window 
#'     either the character string \code{"periodic"} or the span (in lags) of the loess window for seasonal extraction, which should be odd. This has no default.
#' @param s.degree 
#'     degree of locally-fitted polynomial in seasonal extraction. Should be 0, 1, or 2.
#' @param t.window 
#'     the span (in lags) of the loess window for trend extraction, which should be odd. If \code{NULL}, the default, \code{nextodd(ceiling((1.5*period) / (1-(1.5/s.window))))}, is taken.
#' @param t.degree 
#'     degree of locally-fitted polynomial in trend extraction. Should be 0, 1, or 2.
#' @param l.window 
#'     the span (in lags) of the loess window of the low-pass filter used for each subseries. Defaults to the smallest odd integer greater than or equal to \code{n.p} which is recommended since it prevents competition between the trend and seasonal components. If not an odd integer its given value is increased to the next odd one.
#' @param l.degree 
#'     degree of locally-fitted polynomial for the subseries low-pass filter. Should be 0, 1, or 2.
#' @param critfreq 
#'     the critical frequency to use for automatic calculation of smoothing windows for the trend and high-pass filter.
#' @param s.jump,t.jump,l.jump 
#'     integers at least one to increase speed of the respective smoother. Linear interpolation happens between every \code{*.jump}th value.
#' @param s.blend,t.blend,l.blend
#'     vectors of proportion of blending to degree 0 polynomials at the endpoints of the series.
#' @param sub.labels 
#'     optional vector of length n.p that contains the labels of the subseries in their natural order (such as month name, day of week, etc.), used for strip labels when plotting. All entries must be unique.
#' @param sub.start 
#'     which element of sub.labels does the series begin with. See details.
#' @param Clcontrol 
#'     a list contains all tuning parameters for the mapreduce job.
#' @param crtI
#'     index of corrent inner loop
#' @details 
#'     The seasonal component is found by \emph{loess} smoothing the seasonal sub-series (the series of all January values, \ldots); if \code{s.window = "periodic"} smoothing is effectively replaced by taking the mean. The seasonal values are removed, and the remainder smoothed to find the trend. The overall level is removed from the seasonal component and added to the trend component. This process is iterated a few times. The \code{remainder} component is the residuals from the seasonal plus trend fit.
#' Cycle-subseries labels are useful for plotting and can be specified through the sub.labels argument. Here is an example for how the sub.labels and sub.start parameters might be set for one situation. Suppose we have a daily series with n.p=7 (fitting a day-of-week component). Here, sub.labels could be set to c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"). Now, if the series starts with a Wednesday value, then one would specify sub.labels=4, since Wednesday is the fourth element of sub.labels. This ensures that the labels in the plots to start the plotting with Sunday cycle-subseries instead of Wednesday.
#' @references R. B. Cleveland, W. S. Cleveland, J. E. McRae, and I. Terpenning (1990) STL: A Seasonal-Trend Decomposition Procedure Based on Loess. \emph{Journal of Official Statistics}, \bold{6}, 3--73.
#' @author Xiaosu Tong, Ryan Hafen
#' @note This is a complete re-implementation of the STL algorithm, with the loess part in C and the rest in R. Moving a lot of the code to R makes it easier to experiment with the method at a very minimal speed cost. Recoding in C instead of using R's built-in loess results in better performance, especially for larger series.
#' @importFrom stats frequency loess median predict quantile weighted.mean time
#' @importFrom utils head stack tail
#' @export
#' @rdname drinner
drinner <- function(Inner_input, Inner_output, n, n.p, vari, cyctime, seaname, 
  s.window, s.degree, t.window, t.degree, l.window, l.degree, periodic,
  s.jump, t.jump, l.jump, critfreq, s.blend, t.blend, l.blend, crtI, crtO,
  sub.labels, sub.start, infill, Clcontrol) {

  # start and end indices for after adding in extra n.p before and after 
  st <- n.p + 1
  nd <- n + n.p

  # package the parameters into list
  paras <- list(
    vari = vari, cyctime = cyctime, seaname = seaname, 
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
      value <- plyr::arrange(map.values[[r]], get(cyctime))
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
        if (crtI == 1) {
          value$trend <- 0
          value$weight <- 1
        }  
        cycleSub.length <- nrow(value)
        cycleSub <- value[, vari]

        # detrending
        cycleSub <- cycleSub - value$trend

        cs1 <- as.numeric(head(value[, cyctime], 1)) - 1
        cs2 <- as.numeric(tail(value[, cyctime], 1)) + 1  

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
        Cdf <- data.frame(C = C, t = as.numeric(c(cs1, value[, cyctime], cs2)) + index/12.5)
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
      combined <- plyr::arrange(combined, get(cyctime), match(get(seaname), month.abb))
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
    io.sort.mb = Clcontrol$io.sort,
    io.sort.spill.percent = Clcontrol$spill.percent 
  )
  jobIn$readback <- FALSE
  jobIn$jobname <- Inner_output
  job.mr <- do.call("rhwatch", jobIn)

}