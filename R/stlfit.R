stlfit <- function(input, output, model_control=spacetime.control(), cluster_control=mapreduce.control()) {
  
  job <- list()
  job$map <- expression({
    lapply(seq_along(map.keys), function(r) {
      #value <- arrange(map.values[[r]], date)
      fit <- stlplus::stlplus(
        x=map.values[[r]][, control$vari], t=map.values[[r]][, control$time], n.p=control$n.p, 
        s.window=control$s.window, s.degree=control$s.degree, 
        t.window=control$t.window, t.degree=control$t.degree, 
        inner=control$inner, outer=control$outer
      )$data
      value <- cbind(map.values[[r]], subset(fit, select = c(seasonal, trend, remainder)))
      rhcollect(map.keys[[r]], value)
    })
  })
  job$parameters <- list(
    control = model_control,
    Clcontrol = cluster_control
  )
  job$setup <- expression(
    map = {
      suppressMessages(library(stlplus, lib.loc=Clcontrol$libLoc))
      library(plyr, lib.loc=Clcontrol$libLoc)
    }
  )
  job$input <- rhfmt(input, type = "sequence")
  job$output <- rhfmt(output, type = "sequence")
  job$mapred <- list(
    mapreduce.task.timeout = 0,
    mapreduce.job.reduces = 0,  #cdh5
    mapreduce.map.java.opts = "-Xmx3584m",
    mapreduce.map.memory.mb = 4096 
  )
  job$readback <- FALSE
  job$jobname <- output
  job.mr <- do.call("rhwatch", job)

}
