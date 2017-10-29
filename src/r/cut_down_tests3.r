library(data.table)
N = 2e9/8
K = 100

DT <- data.table(
  id3 = sample(sprintf("id%010d",1:(N/K)), N, TRUE),                    
  v1 =  sample(5, N, TRUE)                          
)

system.time(DT[, sum(v1),keyby = id3])
system.time(DT[, sum(v1),keyby = id3])

system.time(setkey(DT, id3))
system.time(DT[, sum(v1),id3])
system.time(DT[, sum(v1),id3])
