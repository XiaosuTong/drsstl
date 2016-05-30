#' Set smoothing parameters for drSpaceTime fitting
#'
#'  Set control parameters for the smoothing fit
#' @param vari 
#'     variable name in string of the response variable
#' @param time 
#'     variable name in string of time index of the whole time series
#' @param seaname 
#'     variable name in string of the seasonal variable
#' @param n 
#'     the number of total observations
#' @param n.p 
#'     the number of observation in each subseries
#' @param s.window 
#'     either the character string \code{"periodic"} or the span (in lags) of 
#'     the loess window for seasonal extraction, which should be odd. This has no default.
#' @param s.degree 
#'     degree of locally-fitted polynomial in seasonal extraction. Should be 0, 1, or 2.
#' @param inner
#'     The iteration time for inner loop of stlplus for time dimension fitting
#' @param outer
#'     The iteration time for outer loop of stlplus for time dimension fitting
#' @param statbytime
#'     The number of locations will be grouped together in the by time division after stlfit.
#'     The parameter is only used for \code{swaptoTime}. Since there may be to many time
#'     point in each location, the \code{swaptoTime} looping all time point will add to much
#'     overhead caused by \code{rhcollect}. So every \code{statbytime} time point are collect
#'     into one key-value pair.
#' @param degree
#'     smoothing degree for the spatial loess smoothing. It can be 0, 1, or 2.
#' @param span
#'     smoothing span for the spatial loess smoothing.
#' @param Edeg
#'     the degree for the conditioanl parametric model including elevation.
#' @param surf
#'     should the fitted surface be computed exactly or via interpolation from a kd tree.
#' @param iterations
#'     the number of iterations used for the space-time back-fitting.
#' @return
#'     A list with space-time fitting parameters.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @references R. B. Cleveland, W. S. Cleveland, J. E. McRae, and I. Terpenning (1990) STL: A Seasonal-Trend Decomposition Procedure Based on Loess. \emph{Journal of Official Statistics}, \bold{6}, 3--73.
#' @examples
#'     spacetime.control(
#'	     n = 786432, n.p = 12, s.window = 21, s.degree = 1, t.window = 241, 
#'       t.degree = 1, degree = 2, span = 0.015, Edeg = 2, surf = "interpolate"
#'     )

spacetime.control <- function(vari="resp", time="date", seaname="month", n, n.p=12, s.window, s.degree = 1,
  t.window = NULL, t.degree = 1, inner=2, outer=1, statbytime = 2, s.jump=10, t.jump=10, cell=0.2,
  degree, span, Edeg, surf = c("interpolate", "direct"), siter = 2) {
  
  list(
    vari=vari, time=time, seaname=seaname, n=n, n.p=n.p, 
    s.window=s.window, s.degree=s.degree, 
    t.window=t.window, t.degree= t.degree, 
    s.jump = s.jump, t.jump = t.jump,
    cell = cell,
    inner=inner, outer=outer, degree=degree, span=span, Edeg=Edeg, 
    surf=match.arg(surf), siter=siter, statbytime=statbytime
  )

}