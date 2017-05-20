#' Apply sstl routine to a data.frame of spatial-temporal dataset in the memory.
#'
#' sstl routine is applied to a spatial-temporal dataset based on divide and recombined
#'
#' @param data
#'      A data.frame includes all observations at locations with longitude, latitude, 
#'      and elevation, where the fitted value is to be calculated.
#' @param s.window
#'      either the character string ‘"periodic"’ or the span (in lags) of the 
#'      loess window for seasonal extraction, which should be odd. This has no default. 
#' @param s.degree
#'      degree of locally-fitted polynomial in seasonal extraction. Should be 0, 1, or 2.
#' @param t.window
#'      the span (in lags) of the loess window for trend extraction, which should be odd.
#'      If ‘NULL’, the default, ‘nextodd(ceiling((1.5*period) / (1-(1.5/s.window))))’, is
#'      taken.
#' @param t.degree
#'      degree of locally-fitted polynomial in trend extraction. Should be 0, 1, or 2.
#' @param lonlat.deg
#'      the smoothing degree of the longitude and latitude in spatial dimension. 
#' @param elev.transform
#'      logical, if it is TRUE, log transformation is applied to elevation variable.
#' @param elev.deg
#'      the smoothing degree of the elevation in spatial dimension
#' @param space.span
#'      the smoothing span for the spatial dimension
#' @param sstl.control
#'     Should be a list object generated from \code{spacetime.control} function.
#'     The list including the rest of smoothing parameters of nonparametric fitting.
#' @author
#'     Xiaosu Tong
#' @export
#' @seealso
#'     \code{\link{spacetime.control}}, \code{\link{predict.drsstl}}
#'
#' @examples
#' \dontrun{
#'     library(Spaloess)
#'     library(stlplus)
#'     library(datadr)
#'     library(plyr)
#'      
#'     mcontrol <- spacetime.control(
#'         time = 'monthidx', y = 'tmax', loc = 'station.id', 
#'         space = c('lon', 'lat', 'elev'), n.p = 12, 
#'         loc.n = n, surf = "interpolate"
#'     )
#'      
#'     system.time(rst <- sstl_local_dr(
#'         data = small_dt, s.window = 13, s.degree = 1, t.window = 181, t.degree = 1, lonlat.deg = c(2, 2), 
#'         elev.transform = TRUE, elev.deg = 2, space.span = c(0.2, 0.1), sstl.control = mcontrol
#'     ))
#' }
drsstl <- function(
    data, s.window, s.degree, t.window, t.degree, lonlat.deg, elev.transform,
    elev.deg, space.span, sstl.control) {

    if(elev.deg == 2) {
        stopifnot(length(sstl.control$space) == 3)
        if (elev.transform) {
            data[, sstl.control$space[3]] <- log2(data[, sstl.control$space[3]] + 128)
        }
        dropSq <- FALSE
        condParam <- sstl.control$space[3]
    } else if(elev.deg == 1) {
        stopifnot(length(sstl.control$space) == 3)
        if (elev.transform) {
            data[, sstl.control$space[3]] <- log2(data[, sstl.control$space[3]] + 128)
        }
        dropSq <- sstl.control$space[3]
        condParam <- sstl.control$space[3]
    } else if (elev.deg == 0) {
        stopifnot(length(sstl.control$space) == 2)
        dropSq <- FALSE
        condParam <- FALSE
    }

    fml <- as.formula(paste(sstl.control$y, "~", paste(sstl.control$space, collapse = " + ")))
    ddf_data <- divide(data, by = sstl.control$time)    

    rst <- suppressMessages(addTransform(ddf_data, function(v) {
        NApred <- any(is.na(v[, sstl.control$y]))
        lo.fit <- Spaloess::spaloess( fml,
            data        = v,
            degree      = lonlat.deg[1],
            span        = space.span[1],
            parametric  = condParam,
            drop_square = dropSq,
            family      = sstl.control$family,
            normalize   = FALSE,
            distance    = "Latlong",
            control     = stats::loess.control(surface = sstl.control$surf, iterations = sstl.control$siter, cell = sstl.control$cell),
            napred      = NApred,
            alltree     = match.arg(sstl.control$surf, c("interpolate", "direct")) == "interpolate"
        )
        if (NApred) {
            indx <- which(!is.na(v[, sstl.control$y]))
            rst <- rbind(
                cbind(indx, fitted=lo.fit$fitted),
                cbind(which(is.na(v[, sstl.control$y])), fitted=lo.fit$pred$fitted)
            )
            rst <- plyr::arrange(as.data.frame(rst), indx)
            v$spaofit <- rst$fitted
        } else {
            v$spaofit <- lo.fit$fitted
        }
        v
    }, params = list(sstl.control = sstl.control, fml = fml, dropSq = dropSq, condParam = condParam, lonlat.deg = lonlat.deg, space.span = space.span), packages = c("datadr")))  

    bystat_ddf <- suppressMessages(divide(rst, by = sstl.control$loc, update = FALSE))   

    rst <- suppressMessages(addTransform(bystat_ddf, function(v) {
        fit <- stlplus::stlplus(
            x        = v$spaofit,
            t        = v[, sstl.control$time],
            n.p      = sstl.control$n.p,
            s.window = s.window,
            s.degree = s.degree,
            t.window = t.window,
            t.degree = t.degree,
            inner    = sstl.control$inner,
            outer    = sstl.control$outer
        )$data
        v <- cbind(v, fit[, c("seasonal", "trend", "remainder")])
        v
    }, params = list(sstl.control = sstl.control, fml = fml, dropSq = dropSq, condParam = condParam, s.window = s.window, s.degree = s.degree, t.window = t.window, t.degree = t.degree), packages = c("datadr")))  

    bymth_ddf <- suppressMessages(divide(rst, by = sstl.control$time, update = FALSE)) 

    fml <- as.formula(paste("remainder", "~", paste(sstl.control$space, collapse = " + ")))
    rst <- suppressMessages(addTransform(bymth_ddf, function(v) {
        lo.fit <- Spaloess::spaloess(fml,
            data        = v,
            degree      = lonlat.deg[2],
            span        = space.span[2],
            parametric  = condParam,
            drop_square = dropSq,
            family      = sstl.control$family,
            normalize   = FALSE,
            distance    = "Latlong",
            control     = stats::loess.control(surface = sstl.control$surf, iterations = sstl.control$siter, cell = sstl.control$cell),
            napred      = FALSE,
            alltree     = match.arg(sstl.control$surf, c("interpolate", "direct")) == "interpolate"
        )
        v$Rspa <- lo.fit$fitted
        subset(v, select = -c(remainder, spaofit))
    }, params = list(sstl.control = sstl.control, fml = fml, dropSq = dropSq, condParam = condParam, lonlat.deg = lonlat.deg, space.span = space.span), packages = c("datadr")))  

    rst <- recombine(rst, combine = combRbind)

    return(rst)

}

