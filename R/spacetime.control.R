#' Set smoothing parameters for the drSpaceTime fitting
#'
#'  Set control parameters for the smoothing fit of stl and spatial smoothing
#' @param vari
#'     variable name in string of the response variable. The default is "resp"
#' @param time
#'     variable name in string of time index of the whole time series. The default is "date".
#'     In the final results on HDFS, the index of time will be changed to this variable
#'     instead of year and month.
#' @param n
#'     the number of total observations in the time series at each location.
#' @param stat_n
#'     The number of stations.
#' @param n.p
#'     the number of observations in each subseries. It should be 12 for monthly data for example.
#' @param s.window
#'     either the character string \code{"periodic"} or the span (in lags) of
#'     the loess window for seasonal extraction, which should be odd. This has no default.
#' @param s.degree
#'     degree of locally-fitted polynomial in seasonal extraction. Should be 0, 1, or 2.
#' @param t.window
#'     the span (in lags) of the loess window for trend extraction, which should be odd. If \code{NULL}, 
#'     the default, \code{nextodd(ceiling((1.5*period) / (1-(1.5/s.window))))}, is taken.
#' @param t.degree
#'     degree of locally-fitted polynomial in trend extraction. Should be 0, 1, or 2.
#' @param inner
#'     The iteration time for inner loop of stlplus for time dimension fitting
#' @param outer
#'     The iteration time for outer loop of stlplus for time dimension fitting
#' @param statbytime
#'     The number of locations will be grouped together in the by time division after stlfit.
#'     The parameter is only used for \code{swaptoTime}. Since there may be to many time
#'     point in each location, the \code{swaptoTime} looping all time point will add to much
#'     overhead caused by \code{rhcollect}. So every \code{statbytime} time point are collect
#'     into one key-value pair. It is save to leave it as default 1. If the time series
#'     is extremely long, it can be set to be 2.
#' @param degree
#'     smoothing degree for the spatial loess smoothing. It can be 0, 1, or 2.
#' @param span
#'     smoothing span for the spatial loess smoothing.
#' @param Edeg
#'     the degree for the conditioanl parametric model including elevation.
#' @param surf
#'     should the fitted surface be computed exactly or via interpolation from a kd tree.
#' @param family
#'     if '"gaussian"' fitting is by least-squares, and if '"symmetric"' a re-descending M estimator
#'     is used with Tukey's biweight function.
#' @param siter
#'     the number of iterations used for the spatial smoothing procedure if family is set to be
#'     '"symmetric"', which is for robust fitting.
#' @param cell
#'     if interpolation is used this controls the accuracy of the
#'     approximation via the maximum number of points in a cell in
#'     the kd-tree. Cells with more than 'floor(n*span*cell)' points
#'     are subdivided.
#' @param s.jump,t.jump
#'     integers at least one to increase speed of the respective smoother. Linear interpolation
#'     happens between every '*.jump'th value.
#' @return
#'     A list with space-time fitting parameters.
#' @author
#'     Xiaosu Tong
#' @export
#' @references R. B. Cleveland, W. S. Cleveland, J. E. McRae, and I. Terpenning (1990) STL: A Seasonal-Trend Decomposition Procedure Based on Loess. \emph{Journal of Official Statistics}, \bold{6}, 3--73.
#' @examples
#'     spacetime.control(
#'	     n = 576, stat_n = 7738, n.p = 12, s.window = 21, s.degree = 1, t.window = 241,
#'       t.degree = 1, degree = 2, span = 0.015, Edeg = 2, surf = "interpolate"
#'     )

spacetime.control <- function(vari="resp", time="date", n, stat_n, n.p=12, s.window, s.degree = 1,
  t.window = NULL, t.degree = 1, inner=2, outer=1, statbytime = 2, s.jump=10, t.jump=10, cell=0.2,
  degree, span, Edeg, surf = c("direct", "interpolate"), family = c("symmetric", "gaussian"), siter = 2) {

  list(
    vari=vari, time=time, statbytime=statbytime, s.jump = s.jump, t.jump = t.jump, cell = cell,
    n=n, n.p=n.p, inner=inner, outer=outer, s.window=s.window, s.degree=s.degree, t.window=t.window, t.degree= t.degree,
    degree=degree, span=span, Edeg=Edeg, stat_n = stat_n, surf=match.arg(surf), family=match.arg(family), siter=siter
  )

}
