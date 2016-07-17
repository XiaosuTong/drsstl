# drsstl: Divide and Recombined for Spatial Seasonal Trend Loess

[![Build Status](https://travis-ci.org/XiaosuTong/drSpaceTime.svg?branch=master)](https://travis-ci.org/XiaosuTong/drSpaceTime)

This is an R package that provide a way to analyze spatial-temporal data based 
on SSTL (Spatial Seasonal Trend decomposition using Loess) routine under the 
divide and recombined framework.


## Installation

```r
# from github
devtools::install_github("XiaosuTong/drSpaceTime")
```

## Fitting

The main function to call in the package is `drsstl` function, which is a
wrapper function of `sstl_mr` and `sstl_local` two functions. `drsstl` can
be used to carry out SSTL routine on the dataset either in the memory or
on the HDFS.

## Prediction 

The function for prediction is `predNewLocs` function, which is a wrapper
function of `predNew_mr` and `predNew_local` two functions. Depends on where
the original fitting results are saved, the prediction at new locations 
will be conducted either in local memory or on HDFS using MapReduce.

## Data

In the data subdirectory, `tmax.RData` inlcudes a data.frame named `tmax_all`
which can be used as example to illustrate the usage of `drsstl` in local memory.
`station_info.RData` contains a data.frame which includes all stations information.
It is usefual for both local and mapreduce situation of `drsstl`. User has to 
copy the `station_info.RData` to HDFS if the mapreduce is chosen.

In inst subdirectory, tmax.txt is the raw data file contains the maximum
temperature data. User can copy it to HDFS and conduct `drsstl` under divide
and recombined framework. 

## Acknowledgment

drSpaceTime development is depended on 

- stlplus package by Ryan Hafen
- Rhipe package by Saptarshi Guha
- Spaloess package by Xiaosu Tong
- loess function in base R by R core Team

