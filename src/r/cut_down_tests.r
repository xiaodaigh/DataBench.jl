library(data.table)
N = 2e9/8
K = 100

DT <- data.table(
  id6 = sample(N/K, N, TRUE),                        # small groups (int)
  id3 = sample(sprintf("id%010d",1:(N/K)), N, TRUE),        
  v1 =  sample(5, N, TRUE)                          # int in range [1,5
)

system.time(DT[, sum(v1),keyby = id6])
system.time(DT[, sum(v1),keyby = id6])

system.time(setkey(DT, id6))
system.time(DT[, sum(v1),keyby = id6])
system.time(DT[, sum(v1),keyby = id6])

