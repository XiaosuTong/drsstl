utils::globalVariables(c("year", "month"))

#' Apply sstl routine to a data.frame of spatial-temporal dataset in the memory using datadr.
#'
#' Assuming data has been read into the memory as a data.frame. Each row of the data.frame
#' contains the observation in a given month at given location. Month index and station
#' location information are saved in 5 columns of the data.frame. The name of these columns
#' should be "lon", "lat", "elev", "year", and "month".
#'
#' @param data
#'     The input data.frame which contains observation, lon, lat, elev, and year, month
#' @param mlcontrol
#'     Should be a list object generated from \code{spacetime.control} function.
#'     The list including all necessary smoothing parameters of nonparametric fitting.
#' @param outdiv
#'     Specify the output division type.
#' @author
#'     Xiaosu Tong
#' @export
#' @examples
#'     head(tmax_all)
#'     n <- 100 # just use 1000 stations as example
#'     set.seed(99)
#'     first_stations <- sample(unique(tmax_all$station.id), n)
#'     small_dt <- subset(tmax_all, station.id %in% first_stations)
#'     small_dt$station.id <- as.character(small_dt$station.id)
#'     small_dt$month <- as.character(small_dt$month)
#'
#'     mcontrol <- spacetime.control(
#'       vari="tmax", n=576, n.p=12, stat_n=n, surf = "interpolate",
#'       s.window=13, t.window = 241, degree=2, span=0.35, Edeg=0
#'     )
#'
#'     rst <- sstl_local_dr(small_dt, mlcontrol=mcontrol, outdiv="loc")

sstl_local_dr <- function(data, mlcontrol=spacetime.control(), outdiv) {

  if(mlcontrol$Edeg == 2) {
    stopifnot(all(c("lat","lon","elev") %in% names(data)))
    data$elev2 <- log2(data$elev + 128)
    fml <- as.formula(paste(mlcontrol$vari, "~ lon + lat + elev2"))
    dropSq <- FALSE
    condParam <- "elev2"
  } else if(mlcontrol$Edeg == 1) {
    stopifnot(all(c("lat","lon","elev") %in% names(data)))
    data$elev2 <- log2(data$elev + 128)
    fml <- as.formula(paste(mlcontrol$vari, "~ lon + lat + elev2"))
    dropSq <- "elev2"
    condParam <- "elev2"
  } else if (mlcontrol$Edeg == 0) {
    stopifnot(all(c("lat","lon") %in% names(data)))
    fml <- as.formula(paste(mlcontrol$vari, "~ lon + lat"))
    dropSq <- FALSE
    condParam <- FALSE
  }

  ddf_data <- datadr::divide(data, by=c("year","month"))

  rst <- suppressMessages(datadr::addTransform(ddf_data, function(v) {
    NApred <- any(is.na(v[, mlcontrol$vari]))
    lo.fit <- Spaloess::spaloess( fml,
      data        = v,
      degree      = mlcontrol$degree,
      span        = mlcontrol$span,
      parametric  = condParam,
      drop_square = dropSq,
      family      = mlcontrol$family,
      normalize   = FALSE,
      distance    = "Latlong",
      control     = stats::loess.control(surface = mlcontrol$surf, iterations = mlcontrol$siter, cell = mlcontrol$cell),
      napred      = NApred,
      alltree     = match.arg(mlcontrol$surf, c("interpolate", "direct")) == "interpolate"
    )
    if (NApred) {
      indx <- which(!is.na(v[, mlcontrol$vari]))
      rst <- rbind(
        cbind(indx, fitted=lo.fit$fitted),
        cbind(which(is.na(v[, mlcontrol$vari])), fitted=lo.fit$pred$fitted)
      )
      rst <- plyr::arrange(as.data.frame(rst), indx)
      v$spaofit <- rst$fitted
    } else {
      v$spaofit <- lo.fit$fitted
    }
    v
  }, params = list(mlcontrol = mlcontrol, fml = fml, dropSq = dropSq, condParam = condParam))) 

  bystat_ddf <- suppressMessages(datadr::divide(rst, by="station.id", update=TRUE))

  rst <- suppressMessages(datadr::addTransform(bystat_ddf, function(v) {
    v <- plyr::arrange(v, year, match(month, month.abb))
    fit <- stlplus::stlplus(
      x        = v$spaofit,
      t        = 1:nrow(v),
      n.p      = mlcontrol$n.p,
      s.window = mlcontrol$s.window,
      s.degree = mlcontrol$s.degree,
      t.window = mlcontrol$t.window,
      t.degree = mlcontrol$t.degree,
      inner    = mlcontrol$inner,
      outer    = mlcontrol$outer
    )$data
    v <- cbind(v, fit[, c("seasonal", "trend", "remainder")])
    v
  }, params = list(mlcontrol = mlcontrol, fml = fml, dropSq = dropSq, condParam = condParam)))

  bymth_ddf <- suppressMessages(datadr::divide(rst, by=c("year","month"), update=TRUE))

  if(mlcontrol$Edeg != 0) {
    fml <- as.formula("remainder ~ lon + lat + elev2")
  } else {
    fml <- as.formula("remainder ~ lon + lat")
  }

  rst <- suppressMessages(datadr::addTransform(bymth_ddf, function(v) {
    lo.fit <- Spaloess::spaloess(fml,
      data        = v,
      degree      = mlcontrol$degree,
      span        = mlcontrol$span,
      parametric  = condParam,
      drop_square = dropSq,
      family      = mlcontrol$family,
      normalize   = FALSE,
      distance    = "Latlong",
      control     = stats::loess.control(surface = mlcontrol$surf, iterations = mlcontrol$siter, cell = mlcontrol$cell),
      napred      = FALSE,
      alltree     = match.arg(mlcontrol$surf, c("interpolate", "direct")) == "interpolate"
    )
    v$Rspa <- lo.fit$fitted
    subset(v, select = -c(remainder, elev2))
  }, params = list(mlcontrol = mlcontrol, fml = fml, dropSq = dropSq, condParam = condParam)))

  if (outdiv == "time") {
    return(rst)
  } else if (outdiv == "loc") {
    rst <- suppressMessages(datadr::divide(rst, by=c("station.id"), update=TRUE))
    return(rst)
  }

}
