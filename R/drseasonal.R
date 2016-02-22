#' Seasonal Decomposition of Time Series by Loess
#'
#' Decompose a time series into seasonal, trend and irregular components using \code{loess}, acronym STL. A new implementation of STL. Allows for NA values, local quadratic smoothing, post-trend smoothing, and endpoint blending. The usage is very similar to that of R's built-in \code{stl()}.
#'
#' @param input the HDFS path of input files
#' @param output the HDFS path of output files
#' @param .vari variable name in string of the response variable
#' @param .t variable name in string of time index in each subseries
#' @param .season variable name in string of the seasonal variable
#' @param n the number of total observations
#' @param n.p the number of observation in each subseries
#' @param s.window either the character string \code{"periodic"} or the span (in lags) of the loess window for seasonal extraction, which should be odd. This has no default.
#' @param s.degree degree of locally-fitted polynomial in seasonal extraction. Should be 0, 1, or 2.
#' @param sub.labels optional vector of length n.p that contains the labels of the subseries in their natural order (such as month name, day of week, etc.), used for strip labels when plotting. All entries must be unique.
#' @param sub.start which element of sub.labels does the series begin with. See details.
#' @param zero.weight value to use as zero for zero weighting
#' @param details if \code{TRUE}, returns a list of the results of all the intermediate iterations.
#' @param s.jump,l.jump integers at least one to increase speed of the respective smoother. Linear interpolation happens between every \code{*.jump}th value.
#' @param s.blend,l.blend vectors of proportion of blending to degree 0 polynomials at the endpoints of the series.
#' @param \ldots additional parameters
#' @details The seasonal component is found by \emph{loess} smoothing the seasonal sub-series (the series of all January values, \ldots); if \code{s.window = "periodic"} smoothing is effectively replaced by taking the mean. The seasonal values are removed, and the remainder smoothed to find the trend. The overall level is removed from the seasonal component and added to the trend component. This process is iterated a few times. The \code{remainder} component is the residuals from the seasonal plus trend fit.
#'
#' Cycle-subseries labels are useful for plotting and can be specified through the sub.labels argument. Here is an example for how the sub.labels and sub.start parameters might be set for one situation. Suppose we have a daily series with n.p=7 (fitting a day-of-week component). Here, sub.labels could be set to c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"). Now, if the series starts with a Wednesday value, then one would specify sub.labels=4, since Wednesday is the fourth element of sub.labels. This ensures that the labels in the plots to start the plotting with Sunday cycle-subseries instead of Wednesday.
#' @return
#' returns an object of class \code{"stlplus"}, containing
#' \item{data}{data frame containing all of the components: \code{raw}, \code{seasonal}, \code{trend}, \code{remainder}, \code{weights}.}
#' \item{pars}{list of parameters used in the procedure.}
#' \item{fc.number}{number of post-trend frequency components fitted.}
#' \item{fc}{data frame of the post-trend frequency components.}
#' \item{time}{vector of time values corresponding to the raw values, if specified.}
#' \item{n}{the number of observations.}
#' \item{sub.labels}{the cycle-subseries labels.}
#' @references R. B. Cleveland, W. S. Cleveland, J. E. McRae, and I. Terpenning (1990) STL: A Seasonal-Trend Decomposition Procedure Based on Loess. \emph{Journal of Official Statistics}, \bold{6}, 3--73.
#' @author Ryan Hafen
#' @note This is a complete re-implementation of the STL algorithm, with the loess part in C and the rest in R. Moving a lot of the code to R makes it easier to experiment with the method at a very minimal speed cost. Recoding in C instead of using R's built-in loess results in better performance, especially for larger series.
#' @examples
#'     FileInput <- "/ln/tongx/Spatial/tmp/tmax/a1950/byseason"
#'     FileOutput <- "/ln/tongx/Spatial/tmp/tmax/a1950/byseason.season"
#'     \dontrun{
#'       drseasonal(input=FileInput, output=FileOutput, .vari="resp", .t="year", .season = "month", n=576, n.p=12, s.window=13, s.degree=1, reduceTask=10, control=spacetime.control(libLoc=.libPaths()))
#'     }
#' @importFrom stats frequency loess median predict quantile weighted.mean time
#' @importFrom utils head stack tail
#' @export
#' @rdname drseasonal
drseasonal <- function(input, output, .vari, .t, .season, n, n.p, s.window, s.degree = 1, s.jump = ceiling(s.window / 10),
l.window = NULL, l.degree = 1, l.jump = ceiling(l.window / 10), critfreq = 0.05, 
s.blend = 0, sub.labels = NULL, sub.start = 1, zero.weight = 1e-6, crtinner = 1, 
crtouter = 1, details = FALSE, reduceTask=0, control=spacetime.control(), ...) {

  nextodd <- function(x) {
    x <- round(x)
    x2 <- ifelse(x %% 2 == 0, x + 1, x)
    # if(any(x != x2))
    # warning("A smoothing span was not odd, was rounded to nearest odd. Check final object parameters to see which spans were used.")
    as.integer(x2)
  }

  wincheck <- function(x) {
    x <- nextodd(x)
    if(any(x <= 0)) stop("Window lengths must be positive.")
    x
  }

  degcheck <- function(x) {
    if(! all(x == 0 | x == 1 | x == 2)) stop("Smoothing degree must be 0, 1, or 2")
  }

  periodic <- FALSE
  if (is.character(s.window)) {
    if (is.na(pmatch(s.window, "periodic")))
      stop("unknown string value for s.window")
    else {
      periodic <- TRUE
      s.window <- 10 * n + 1
      s.degree <- 0
      s.jump <- ceiling(s.window / 10)
    }
  } else {
    s.window <- wincheck(s.window)
  }

  if(is.null(l.window)) {
    l.window <- nextodd(n.p)
  } else {
    l.window <- wincheck(l.window)
  }

  degcheck(s.degree)
  degcheck(l.degree)

  if(is.null(s.jump) || length(s.jump)==0) s.jump <- ceiling(s.window / 10)
  if(is.null(l.jump) || length(l.jump)==0) l.jump <- ceiling(l.window / 10)
  # start and end indices for after adding in extra n.p before and after 
  st <- n.p + 1
  nd <- n + n.p

  # package the parameters into list
  paras <- list(
  	.vari = .vari, .t = .t, .season = .season, n.p = n.p, n = n, st = st, nd = nd, 
  	s.window = s.window, s.degree = s.degree, s.jump = s.jump, periodic = periodic, 
  	l.window = l.window, l.degree = l.degree, l.jump = l.jump, crtinner=crtinner
  )
  
  job <- list()
  job$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      index <- match(map.keys[[r]][2], month.abb)
#      value <- plyr::arrange(map.values[[r]], get(.t))
#      value[, .season] <- index
#      cycleSub.length <- nrow(value)
#      cycleSub <- value[, .vari]
#      if (crtinner == 1) {
#        value$trend <- 0
#        value$weight <- 1
#      }
#      
#      cs1 <- as.numeric(head(value[, .t], 1)) - 1
#      cs2 <- as.numeric(tail(value[, .t], 1)) + 1

#      if (periodic) {
#        C <- rep(weighted.mean(cycleSub, w = value$weight, na.rm = TRUE), cycleSub.length + 2)
#      } else {
#        cs.ev <- seq(1, cycleSub.length, by = s.jump)
#        if(tail(cs.ev, 1) != cycleSub.length) cs.ev <- c(cs.ev, cycleSub.length)
#        cs.ev <- c(0, cs.ev, cycleSub.length + 1)
#        C <- drSpaceTime::.loess_stlplus(
#          y = cycleSub, span = s.window, degree = s.degree,
#          m = cs.ev, weights = value$weight, blend = s.blend,
#          jump = s.jump, at = c(0:(cycleSub.length + 1))
#        ) 
#      }
#      Cdf <- data.frame(C = C, t = as.numeric(paste(c(cs1, value[, .t], cs2), index, sep=".")))
#      rhcollect(map.keys[[r]][1], list(value, Cdf))
      rhcollect(map.keys[[r]][1], index)
    })
  })
#  job$reduce <- expression(
#    pre = {
#      combined <- data.frame()
#      Ctotal <- data.frame()
#      ma3 <- 0
#      L <- 0
#      y_idx <- logical()
#      noNa <- logical()
#      l.ev <- seq(1, n, by = l.jump)
#      if(tail(l.ev, 1) != n) l.ev <- c(l.ev, n)
#    },
#    reduce = {
#      combined <- rbind(combined, do.call("rbind", lapply(redce.values, "[[", 1)))
#      Ctotal <- rbind(Ctotal, do.call("rbind", lapply(reduce.values, "[[", 2)))
#    },
#    post = {
#      combined <- plyr::arrange(combined, get(.t), get(.season))
#      Ctotal <- plyr::arrange(Ctotal, t)
#      y_idx <- !is.na(combined[, .vari])
#      noNA <- all(y_idx)
#      ma3 <- drSpaceTime::c_ma(Ctotal$C, n.p)
#      L <- drSpaceTime::.loess_stlplus(
#        y = ma3, span = l.window, degree = l.degree, m = l.ev, weights = combined$weight, 
#        y_idx = y_idx, noNA = noNA, blend = l.blend, jump = l.jump, at = c(1:n)
#      )
#      combined$seasonal <- Ctotal[st:nd] - L
#    }
#  )
  job$setup <- expression(
    map = {
      library(plyr, lib.loc=control$libLoc)
      library(drSpaceTime, lib.loc=control$libLoc)
    },
    reduce = {
      library(plyr, lib.loc=control$libLoc)
      library(drSpaceTime, lib.loc=control$libLoc)
    }
  )
#  job$parameters <- paras
  job$input <- rhfmt(input, type = "sequence")
  job$output <- rhfmt(output, type = "sequence")
  job$mapred <- list(
    mapred.reduce.tasks = reduceTask,  #cdh3,4
    mapreduce.job.reduces = reduceTask  #cdh5
  )
  job$readback <- FALSE
  job$jobname <- output
  job.mr <- do.call("rhwatch", job)


}