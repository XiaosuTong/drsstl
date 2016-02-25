#' Set parameter for drSpaceTime fitting
#'
#'  Set control parameters for drSpaceTime fits
#' @param vari 
#'     variable name in string of the response variable
#' @param cyctime 
#'     variable name in string of time index in each subseries
#' @param seaname 
#'     variable name in string of the seasonal variable
#' @param n 
#'     the number of total observations
#' @param n.p 
#'     the number of observation in each subseries
#' @param s.window 
#'     either the character string \code{"periodic"} or the span (in lags) of the loess window for seasonal extraction, which should be odd. This has no default.
#' @param s.degree 
#'     degree of locally-fitted polynomial in seasonal extraction. Should be 0, 1, or 2.
#' @param sub.labels 
#'     optional vector of length n.p that contains the labels of the subseries in their natural order (such as month name, day of week, etc.), used for strip labels when plotting. All entries must be unique.
#' @param sub.start 
#'     which element of sub.labels does the series begin with. See details.
#' @param zero.weight 
#'     value to use as zero for zero weighting
#' @param s.jump,t.jump,l.jump 
#'     integers at least one to increase speed of the respective smoother. Linear interpolation happens between every \code{*.jump}th value.
#' @param s.blend,t.blend,l.blend 
#'     vectors of proportion of blending to degree 0 polynomials at the endpoints of the series.
#' @param inner
#'     The iteration time for inner loop of stlplus for time dimension fitting
#' @param outer
#'     The iteration time for outer loop of stlplus for time dimension fitting
#' @param surface
#'     should the fitted surface be computed exactly or via interpolation from a kd tree?
#' @param libLoc
#'     the library searching path, sepcifically the path including Spaloess and 
#'     stlplus package on the front end server. If all packages have been pushed
#'     the the tar.gz file on hdfs, then this argument is set to be NULL.
#' @param iterations
#'     the number of iterations used for the space-time back-fitting.
#' @return
#'     A list with space-time fitting parameters.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     spacetime.control()

spacetime.control <- function(vari, cyctime, seaname, n, n.p, s.window, s.degree = 1,
  s.jump = ceiling(s.window / 10), t.window = NULL, t.degree = 1, t.jump = ceiling(t.window / 10),
  t.blend = 0, l.window = NULL, l.degree = t.degree, l.jump = ceiling(l.window / 10), l.blend = 0,
  critfreq = 0.05, s.blend = 0, sub.labels = NULL, sub.start = 1, inner=4, outer=1, 
  surface = c("interpolate", "direct"), iterations = 1, infill = TRUE) {
  
  list(
    vari=vari, cyctime=cyctime, seaname=seaname, n=n, n.p=n.p, 
    s.window=s.window, s.degree=s.degree, s.jump=s.jump, s.blend=s.blend, 
    t.window=t.window, t.degree= t.degree, t.jump=t.jump, t.blend= t.blend, 
    l.window=l.window, l.degree= l.degree, l.jump=l.jump, l.blend= l.blend,
    critfreq=critfreq, sub.labels=sub.labels, sub.start=sub.start, 
    inner=inner, outer=outer, surface=match.arg(surface), 
    iterations=iterations, infill=infill
  )

}