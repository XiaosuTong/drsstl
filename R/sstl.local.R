#' Readin Raw text data files and save it as by time division on HDFS.
#'
#' Input raw text data file is divided into by time subsets and saved on HDFS
#'
#' @param data
#'     The input data.frame which contains observation, lon, lat, elev, and year, month
#' @param model_control
#'     Should be a list object generated from \code{spacetime.control} function.
#'     The list including all necessary smoothing parameters of nonparametric fitting.
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     head(tmax_all)
#'     mcontrol <- spacetime.control(
#'       vari="resp", n=576, n.p=12, stat_n=7738,
#'       s.window=13, t.window = 241, degree=2, span=0.015, Edeg=2
#'     )
#'     sstl(tmax_all, model_control=mcontrol)



sstl <- function(data, model_control=spacetime.control()) {

  if(model_control$Edeg == 2) {
    stopifnot(all(c("lat","lon","elev") %in% names(data)))
    data$elev2 <- log2(data$elev + 128)
    fml <- as.formula(paste(model_control$model_control$vari, "~ lon + lat + elev2"))
    dropSq <- FALSE
    condParam <- "elev2"
  } else if(model_control$Edeg == 1) {
    stopifnot(all(c("lat","lon","elev") %in% names(data)))
    data$elev2 <- log2(data$elev + 128)
    fml <- as.formula(paste(model_control$model_control$vari, "~ lon + lat + elev2"))
    dropSq <- "elev2"
    condParam <- "elev2"
  } else if (model_control$Edeg == 0) {
    stopifnot(all(c("lat","lon") %in% names(data)))
    fml <- as.formula(paste(model_control$model_control$vari, "~ lon + lat"))
    dropSq <- FALSE
    condParam <- FALSE
  }

  rst <- ddply(.data = data
    , .vari = c("year", "month")
    , .fun = function(v) {
        lo.fit <- spaloess( fml, 
          data      = v, 
          degree    = model_control$degree, 
          span      = model_control$span,
          para      = condParam,
          drop      = dropSq,
          family    = "symmetric",
          normalize = FALSE,
          distance  = "Latlong",
          control   = loess.control(surface = model_control$surf, iterations = model_control$siter),
          napred    = TRUE,
          alltree   = TRUE
        )
        indx <- which(!is.na(v[, model_control$vari]))
        rst <- rbind(
          cbind(indx, fitted=lo.fit$fitted), 
          cbind(which(is.na(v[, model_control$vari])), fitted=lo.fit$pred$fitted)
        )
        rst <- arrange(as.data.frame(rst), indx)
        v$spaofit <- rst$fitted
        v
      }
  )

  rst <- ddply(.data = rst
    , .vari = "station.id"
    , .fun = function(v) {
        v <- arrange(v, year, match(month, month.abb))
        fit <- stlplus::stlplus(
          x        = v$spaofit, 
          t        = 1:nrow(v), 
          n.p      = n.p, 
          s.window = model_control$s.window, 
          s.degree = model_control$s.degree, 
          t.window = model_control$t.window, 
          t.degree = model_control$t.degree, 
          inner    = model_control$inner, 
          outer    = model_control$outer
        )$data        
        v <- cbind(v, fit[, c("seasonal", "trend", "remainder")])
        v
    }
  )

  if(model_control$Edeg != 0) {
    fml <- as.formula("remainder ~ lon + lat + elev2")
  } else {
    fml <- as.formula("remainder ~ lon + lat")
  }

  rst <- ddply(.data = rst
    , .vari = c("year", "month")
    , .fun = function(v) {
        lo.fit <- spaloess( fml, 
          data      = v, 
          degree    = model_control$degree, 
          span      = model_control$span,
          para      = condParam,
          drop      = dropSq,
          family    = "symmetric",
          normalize = FALSE,
          distance  = "Latlong",
          control   = loess.control(surface = surf, iterations = 2),
          napred    = FALSE,
          alltree   = FALSE
        )
        v$Rspa <- lo.fit$fitted
        v
      }
  )

  return(rst)

}