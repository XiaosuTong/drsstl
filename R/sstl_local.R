#' Apply sstl routine to a data.frame of spatial-temporal dataset in the memory.
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
#' @author 
#'     Xiaosu Tong 
#' @export
#' @examples
#'     head(tmax_all)
#'     mcontrol <- spacetime.control(
#'       vari="tmax", n=576, n.p=12, stat_n=7738,
#'       s.window=13, t.window = 241, degree=2, span=0.015, Edeg=2
#'     )
#'     sstl_local(tmax_all, mlcontrol=mcontrol)



sstl_local <- function(data, mlcontrol=spacetime.control()) {

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
  
  message("First spatial smoothing...")
  rst <- ddply(.data = data
    , .vari = c("year", "month")
    , .fun = function(v) {
        NApred <- any(is.na(v[, mlcontrol$vari]))
        lo.fit <- spaloess( fml, 
          data      = v, 
          degree    = mlcontrol$degree, 
          span      = mlcontrol$span,
          para      = condParam,
          drop      = dropSq,
          family    = "symmetric",
          normalize = FALSE,
          distance  = "Latlong",
          control   = loess.control(surface = mlcontrol$surf, iterations = mlcontrol$siter, cell = Mlcontrol$cell),
          napred    = NApred,
          alltree   = TRUE
        )
        if (NApred) {
          indx <- which(!is.na(v[, mlcontrol$vari]))
          rst <- rbind(
            cbind(indx, fitted=lo.fit$fitted), 
            cbind(which(is.na(v[, mlcontrol$vari])), fitted=lo.fit$pred$fitted)
          )
          rst <- arrange(as.data.frame(rst), indx)
          v$spaofit <- rst$fitted
        } else {
          v$spaofit <- lo.fit$fitted
        }
        v
      }
  )

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
  } else {
    fml <- as.formula("remainder ~ lon + lat")
  }

  message("Second spatial smoothing...")
  rst <- ddply(.data = rst
    , .vari = c("year", "month")
    , .fun = function(v) {
        lo.fit <- spaloess( fml, 
          data      = v, 
          degree    = mlcontrol$degree, 
          span      = mlcontrol$span,
          para      = condParam,
          drop      = dropSq,
          family    = "symmetric",
          normalize = FALSE,
          distance  = "Latlong",
          control   = loess.control(surface = mlcontrol$surf, iterations = mlcontrol$siter),
          napred    = FALSE,
          alltree   = FALSE
        )
        v$Rspa <- lo.fit$fitted
        subset(v, select = -c(remainder))
      }
  )

  return(rst)

}