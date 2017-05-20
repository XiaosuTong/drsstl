#' Prediction at new locations based on the fitting results.
#'
#' The prediction at new locations are calculated based on the fitting results
#' saved in memory based on the original dataset.
#'
#' @param newdata
#'      A data.frame includes all locations' longitude, latitude, and elevation,
#'      where the prediction is to be calculated.
#' @param original
#'      The data.frame which contains all fitting results of original dataset.
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
#'     \code{\link{spacetime.control}}
#'
#' @examples
#' \dontrun{
#'     library(maps)
#'     library(Spaloess)
#'     library(stlplus)
#'     library(datadr)
#'     library(plyr)
#'     library(datadr)
#'     mcontrol <- spacetime.control(
#'         time = 'monthidx', y = 'tmax', loc = 'station.id', 
#'         space = c('lon', 'lat', 'elev'), n.p = 12, 
#'         loc.n = n, surf = "interpolate"
#'     )
#'     
#'     new.grid <- expand.grid(
#'       lon = seq(-126, -67, by = 3),
#'       lat = seq(25, 49, by = 3)
#'     )
#'     instate <- !is.na(map.where("state", new.grid$lon, new.grid$lat))
#'     new.grid <- new.grid[instate, ]
#'     elev.fit <- spaloess( elev ~ lon + lat,
#'       data = station_info,
#'       degree = 2,
#'       span = 0.05,
#'       distance = "Latlong",
#'       normalize = FALSE,
#'       napred = FALSE,
#'       alltree = FALSE,
#'       family="symmetric",
#'       control=loess.control(surface = "direct")
#'     )
#'     grid.fit <- predloess(
#'       object = elev.fit,
#'       newdata = data.frame(
#'         lon = new.grid$lon,
#'         lat = new.grid$lat
#'       )
#'     )
#'     new.grid$elev <- grid.fit
#'     
#'     system.time(rst <- predict.drsstl(
#'         original = small_dt, newdata = new.grid, s.window = 13, s.degree = 1, 
#'         t.window = 181, t.degree = 1, lonlat.deg = c(2, 2), elev.transform = TRUE,
#'         elev.deg = 2, space.span = c(0.2, 0.1), sstl.control = mcontrol
#'     ))
#' }
predict.drsstl <- function(
    original, newdata, s.window, s.degree, t.window, t.degree, lonlat.deg, elev.transform,
    elev.deg, space.span, sstl.control) {

    if(elev.deg == 2) {
        stopifnot(length(sstl.control$space) == 3)
        if (elev.transform) {
            original[, sstl.control$space[3]] <- log2(original[, sstl.control$space[3]] + 128)
            newdata[, sstl.control$space[3]] <- log2(newdata[, sstl.control$space[3]] + 128)
        }
        dropSq <- FALSE
        condParam <- sstl.control$space[3]
    } else if(elev.deg == 1) {
        stopifnot(length(sstl.control$space) == 3)
        if (elev.transform) {
            original[, sstl.control$space[3]] <- log2(original[, sstl.control$space[3]] + 128)
            newdata[, sstl.control$space[3]] <- log2(newdata[, sstl.control$space[3]] + 128)
        }
        dropSq <- sstl.control$space[3]
        condParam <- space[3]
    } else if (elev.deg == 0) {
        stopifnot(length(sstl.control$space) == 2)
        dropSq <- FALSE
        condParam <- FALSE
    }
    fml <- as.formula(paste(sstl.control$y, "~", paste(sstl.control$space, collapse = " + ")))
    ddf_data <- divide(original, by = sstl.control$time)   

    N <- nrow(newdata)
    if (class(original[, sstl.control$loc]) != "character") {
        original[, sstl.control$loc] <- as.character(original[, sstl.control$loc])    
    }

    message("First spatial smoothing...")
    rst <- suppressMessages(addTransform(ddf_data, function(v) {
        value <- newdata
        value[, sstl.control$loc] <- paste0('new', 1:N)
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
        newPred <- unname(predloess(
            object = lo.fit,
            newdata = newdata[, sstl.control$space]
        ))
        value$spaofit <- newPred
        return(rbind(value, v[, c(sstl.control$space, sstl.control$loc, "spaofit")]))      
    }, params = list(sstl.control = sstl.control, fml = fml, dropSq = dropSq, condParam = condParam, lonlat.deg = lonlat.deg, space.span = space.span), packages = c("datadr")))  

    bystat_ddf <- suppressMessages(divide(rst, by = sstl.control$loc, update = FALSE))   

    message("Temporal fitting...")
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
    message("Second spatial smoothing...")
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
    new_locs <- rst[, sstl.control$loc] %in% paste0('new', 1:N)

    return(rst[new_locs, !(names(rst) %in% sstl.control$loc)])

}
