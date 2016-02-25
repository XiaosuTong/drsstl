#' D&R Seasonal Trend Decomposition of Time Series by Loess
#'
#' Decompose a time series into seasonal, trend and irregular components using \code{loess}, acronym STL. A new implementation of STL. Allows for NA values, local quadratic smoothing, post-trend smoothing, and endpoint blending. The usage is very similar to that of R's built-in \code{stl()}.
#'
#' @references R. B. Cleveland, W. S. Cleveland, J. E. McRae, and I. Terpenning (1990) STL: A Seasonal-Trend Decomposition Procedure Based on Loess. \emph{Journal of Official Statistics}, \bold{6}, 3--73.
#' @author Xiaosu Tong, Ryan Hafen
#' @note This is a complete re-implementation of the STL algorithm, with the loess part in C and the rest in R. Moving a lot of the code to R makes it easier to experiment with the method at a very minimal speed cost. Recoding in C instead of using R's built-in loess results in better performance, especially for larger series.
#' @importFrom stats frequency loess median predict quantile weighted.mean time
#' @importFrom utils head stack tail
#' @export
#' @rdname drstlplus
drstlplus <- function(input, output, model_control=spacetime.control(), 
	cluster_control=mapredcue.control(), ...) {

  nextodd <- function(x) {
    x <- round(x)
    x2 <- ifelse(x %% 2 == 0, x + 1, x)
    # if(any(x != x2))
    # warning("A smoothing span was not odd, was rounded to nearest odd. Check final object parameters to see which spans were used.")
    as.integer(x2)
  }

  wincheck <- function(x) {
    x <- nextodd(x)
    if(any(x <= 0)) stop("Window lengths must be positive.")
    x
  }

  degcheck <- function(x) {
    if(! all(x == 0 | x == 1 | x == 2)) {
      stop("Smoothing degree must be 0, 1, or 2")
    } else {
      x
    }
  }

  get.t.window <- function(t.dg, s.dg, n.s, n.p, omega) {
    if(t.dg == 0) t.dg <- 1
    if(s.dg == 0) s.dg <- 1
    
    coefs_a <- data.frame(
      a = c(0.000103350651767650, 3.81086166990428e-6),
      b = c(-0.000216653946625270, 0.000708495976681902))
    coefs_b <- data.frame(
      a = c(1.42686036792937, 2.24089552678906),
      b = c(-3.1503819836694, -3.30435316073732),
      c = c(5.07481807116087, 5.08099438760489))
    coefs_c <- data.frame(
      a = c(1.66534145060448, 2.33114333880815),
      b = c(-3.87719398039131, -1.8314816166323),
      c = c(6.46952900183769, 1.85431548427732))
    
    # estimate critical frequency for seasonal
    betac0 <- coefs_a$a[s.dg] + coefs_a$b[s.dg] * omega
    betac1 <- coefs_b$a[s.dg] + coefs_b$b[s.dg] * omega + coefs_b$c[s.dg] * omega^2
    betac2 <- coefs_c$a[s.dg] + coefs_c$b[s.dg] * omega + coefs_c$c[s.dg] * omega^2
    f_c <- (1 - (betac0 + betac1 / n.s + betac2 / n.s^2)) / n.p
    
    # choose
    betat0 <- coefs_a$a[t.dg] + coefs_a$b[t.dg] * omega
    betat1 <- coefs_b$a[t.dg] + coefs_b$b[t.dg] * omega + coefs_b$c[t.dg] * omega^2
    betat2 <- coefs_c$a[t.dg] + coefs_c$b[t.dg] * omega + coefs_c$c[t.dg] * omega^2
    
    betat00 <- betat0 - f_c
    
    n.t <- nextodd((-betat1 - sqrt(betat1^2 - 4 * betat00 * betat2)) / (2 * betat00))
    n.t
  }

  s.degree <- degcheck(model_control$s.degree)
  l.degree <- degcheck(model_control$l.degree)
  t.degree <- degcheck(model_control$t.degree)

  s.jump <- model_control$s.jump
  l.jump <- model_control$l.jump
  t.jump <- model_control$t.jump

  periodic <- FALSE
  if (is.character(model_control$s.window)) {
    if (is.na(pmatch(model_control$s.window, "periodic")))
      stop("unknown string value for s.window")
    else {
      periodic <- TRUE
      s.window <- 10 * n + 1
      s.degree <- 0
      s.jump <- ceiling(s.window / 10)
    }
  } else {
    s.window <- wincheck(model_control$s.window)
  }

  if(is.null(model_control$l.window)) {
    l.window <- nextodd(model_control$n.p)
  } else {
    l.window <- wincheck(model_control$l.window)
  }

  if (is.null(model_control$t.window)) {
    t.window <- get.t.window(t.degree, s.degree, s.window, model_control$n.p, model_control$critfreq)
  } else {
    t.window <- wincheck(model_control$t.window)
  }

  if(is.null(s.jump) || length(s.jump)==0) s.jump <- ceiling(s.window / 10)
  if(is.null(l.jump) || length(l.jump)==0) l.jump <- ceiling(l.window / 10)
  if(is.null(t.jump) || length(t.jump)==0) t.jump <- ceiling(t.window / 10)

  FileInput <- 

  for (o in 1:model_control$outer) {

    for (i in 1:model_control$inner) {

      drSpaceTime::swaptoSeason()
      
      drSpaceTime::drinner(
        Inner_input=FileInput, Inner_output=FileOutput, 
        n=model_control$n, n.p=model_control$n.p, vari=model_control$vari, cyctime=model_control$cyctime, 
        seaname=model_control$seaname, 
        s.window = s.window, s.degree = s.degree, 
        t.window = t.window, t.degree = t.degree,
        l.window = l.window, l.degree = l.degree,
        s.jump = s.jump, t.jump = t.jump, l.jump = l.jump, critfreq = 0.05,
        s.blend = model_control$s.blend, t.blend = model_control$t.blend, l.blend = model_control$l.blend,  
        crtI = i, sub.labels = model_control$sub.labels, sub.start = model_control$sub.start, 
        infill= model_control$infill, Clcontrol=cluster_control
      )

    }

    drSpaceTime::drrobust()

  }

}