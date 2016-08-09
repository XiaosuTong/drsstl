#' @import testthat Spaloess plyr Rcpp yaImpute stlplus roxygen2
#' @importFrom stats as.formula loess.control median
#' @importFrom utils head tail
#' @importFrom maps map.where
#' @importFrom datadr divide addTransform recombine combRbind
NULL



#' US Maximum Temperature
#'
#' @name tmax_all
#' @docType data
#' @description
#' A data.frame contains maximum temperature data from NCAR weside
#'
#' @source
#' Full data on NCAR: \url{http://www.image.ucar.edu/Data/US.monthly.met/FullData.shtml#temp}
#' @keywords data
NULL

#' Metadata of Stations
#'
#' @name station_info
#' @docType data
#' @description
#' Metadata for all stations including longitude, latitude, elevation, station.id
#'
#' @source
#' All station metadata: \url{http://www.image.ucar.edu/Data/US.monthly.met/FullData.shtml#temp}
#' @keywords data
NULL
