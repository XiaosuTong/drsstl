#' Prediction at new locations based on the fitting results in memory.
#'
#' The prediction at new locations are calculated based on the fitting results
#' saved in memory based on the original dataset.
#'
#' @param newdata
#'     A data.frame includes all locations' longitude, latitude, and elevation,
#'     where the prediction is to be calculated.
#' @param original
#'     The data.frame which contains all fitting results of original dataset. The data.frame
#'     is saved in memory, not on HDFS.
#' @param mlcontrol
#'     Should be a list object generated from \code{spacetime.control} function.
#'     The list including all necessary smoothing parameters of nonparametric fitting.
#' @author
#'     Xiaosu Tong
#' @export
#' @seealso
#'     \code{\link{spacetime.control}}, \code{\link{mapreduce.control}}
#'
#' @examples
#'     mlcontrol <- spacetime.control(
#'       vari="resp", time="date", n=576, n.p=12, stat_n=7738,
#'       s.window="periodic", t.window = 241, degree=2, span=0.015, Edeg=2
#'     )
#'
#'     new.grid <- expand.grid(
#'       lon = seq(-126, -67, by = 0.5),
#'       lat = seq(25, 49, by = 0.5)
#'     )
#'     instate <- !is.na(map.where("state", new.grid$lon, new.grid$lat))
#'     new.grid <- new.grid[instate, ]
#'
#'     elev.fit <- spaloess( elev ~ lon + lat,
#'       data = station_info,
#'       degree = 2,
#'       span = 0.015,
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
#'     new.grid$elev2 <- log2(grid.fit + 128)
#'     fitted <- drsstl(
#'       data=tmax_all,
#'       output=NULL,
#'       stat_info="station_info",
#'       model_control=mlcontrol
#'     )
#'     predNew_local(
#'       original = fitted, newdata = new.grid, mlcontrol = mlcontrol
#'     )

predNew_local <- function(original, newdata, mlcontrol=spacetime.control()) {

  if(mlcontrol$Edeg == 2) {
    newdata$elev2 <- log2(newdata$elev + 128)
    original$elev2 <- log2(original$elev + 128)
    fml <- as.formula(paste(mlcontrol$vari, "~ lon + lat + elev2"))
    dropSq <- FALSE
    condParam <- "elev2"
  } else if(mlcontrol$Edeg == 1) {
    original$elev2 <- log2(original$elev + 128)
    newdata$elev2 <- log2(newdata$elev + 128)
    fml <- as.formula(paste(mlcontrol$vari, "~ lon + lat + elev2"))
    dropSq <- "elev2"
    condParam <- "elev2"
  } else if (mlcontrol$Edeg == 0) {
    fml <- as.formula(paste(mlcontrol$vari, "~ lon + lat"))
    dropSq <- FALSE
    condParam <- FALSE
  }

  N <- nrow(newdata)

  message("First spatial smoothing...")
  rst <- dlply(.data = original
    , .vari = c("year", "month")
    , .fun = function(v) {
        value <- cbind(data.frame(station.id = 1:N), newdata)
        NApred <- any(is.na(v[, mlcontrol$vari]))
        lo.fit <- spaloess( fml,
          data      = v,
          degree    = mlcontrol$degree,
          span      = mlcontrol$span,
          para      = condParam,
          drop      = dropSq,
          family    = mlcontrol$family,
          normalize = FALSE,
          distance  = "Latlong",
          control   = loess.control(surface = mlcontrol$surf, iterations = mlcontrol$siter, cell = mlcontrol$cell),
          napred    = NApred,
          alltree   = TRUE
        )
        if (mlcontrol$Edeg != 0) {
          newPred <- unname(predloess(
            object = lo.fit,
            newdata = data.frame(
              lon = newdata$lon,
              lat = newdata$lat,
              elev2 = newdata$elev2
            )
          ))
        } else {
          newPred <- unname(predloess(
            object = lo.fit,
            newdata = data.frame(
              lon = newdata$lon,
              lat = newdata$lat
            )
          ))
        }
        value$spaofit <- newPred
        value
      }
  )

  rst <- do.call("rbind", rst)
  time <- strsplit(rownames(rst),"[.]")
  rownames(rst) <- NULL
  rst$year <- as.numeric(unlist(lapply(time, function(r) {
    r[1]
  })))
  rst$month <- unlist(lapply(time, function(r) {
    r[2]
  }))

  message("Temporal fitting...")
  rst <- ddply(.data = rst
    , .vari = "station.id"
    , .fun = function(v) {
        v <- arrange(v, year, match(month, month.abb))
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
    }
  )

  if(mlcontrol$Edeg != 0) {
    fml <- as.formula("remainder ~ lon + lat + elev2")
    tmp <- rbind(
      subset(original, select = c(station.id, lon, lat, elev2, year, month, spaofit, seasonal, trend)),
      subset(rst, select = c(station.id, lon, lat, elev2, year, month, spaofit, seasonal, trend))
    )
  } else {
    fml <- as.formula("remainder ~ lon + lat")
    tmp <- rbind(
      subset(original, select = c(station.id, lon, lat, elev2, year, month, spaofit, seasonal, trend)),
      subset(rst, select = c(station.id, lon, lat, elev2, year, month, spaofit, seasonal, trend))
    )
  }

  message("Second spatial smoothing...")
  rst <- ddply(.data = tmp
    , .vari = c("year", "month")
    , .fun = function(v) {
        v$remainder <- with(v, spaofit - trend - seasonal)
        lo.fit <- spaloess( fml,
          data      = v,
          degree    = mlcontrol$degree,
          span      = mlcontrol$span,
          para      = condParam,
          drop      = dropSq,
          family    = mlcontrol$family,
          normalize = FALSE,
          distance  = "Latlong",
          control   = loess.control(surface = mlcontrol$surf, iterations = mlcontrol$siter, cell = mlcontrol$cell),
          napred    = FALSE,
          alltree   = FALSE
        )
        v$Rspa <- lo.fit$fitted
        subset(v, station.id %in% 1:N, select = -c(remainder))
      }
  )

  return(rst)

}
