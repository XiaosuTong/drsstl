# drSpaceTime: Divide and Recombined for Spatial-Temporal Data

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
wrapper function of `sstl_mr` and `sstl_local` two functions



## Prediction 

## Acknowledgment

drSpaceTime development is depended on 

- stlplus package by Ryan Hafen
- Rhipe package by Saptarshi Guha
- Spaloess package by Xiaosu Tong
- loess function in base R by R core Team

