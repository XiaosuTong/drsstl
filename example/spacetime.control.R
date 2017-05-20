#' Set smoothing parameters for the drsstl fitting
#'
#' Set control parameters for the smoothing fit of stl and spatial smoothing
#'
#' @param y
#'     variable name in string of the response variable
#' @param time
#'     variable name in string of time index of the whole time series.
#' @param space
#'     vector of variable name in spatial dimension. It should be the name for longitude, latitude,
#'     and elevation, like c("lon", "lat", "elev")
#' @param loc
#'     The index for locations
#' @param loc.n
#'.    The number of locations
#' @param n.p
#'     the number of observations in each subseries. It should be 12 for monthly data for example.
#' @param inner
#'     The iteration time for inner loop of stlplus for time dimension fitting
#' @param outer
#'     The iteration time for outer loop of stlplus for time dimension fitting
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

spacetime.control <- function(y, space, time, loc, loc.n, n.p=12, inner=2, outer=1, s.jump=10, t.jump=10, cell=0.2,
    surf = c("direct", "interpolate"), family = c("symmetric", "gaussian"), siter = 2) {

    list(
        y = y, space = space, time = time, loc = loc, loc.n = loc.n, n.p = 12,
        inner = inner, outer = outer, s.jump = s.jump, t.jump = t.jump, cell = cell,
        surf = match.arg(surf), family = match.arg(family), siter=siter
    )

}
