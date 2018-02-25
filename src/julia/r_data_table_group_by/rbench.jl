function R_bench(N=10_000_000, K = 100; libpath = "")
  R"""
  if ($libpath != "") {
    .libPaths($libpath)
  }

  # set higher memory limit on Windows
  memory.limit(2^31-1)

  library(data.table)
  library(pipeR)
  library(magrittr)

  source("src/r/1_generate_synthetic.r")
  timings <- rbench(N = $N, K = $K)
  gc()
  """
  @rget timings
  return timings
end
