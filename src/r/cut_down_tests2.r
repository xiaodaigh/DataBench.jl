library(data.table)
N = 2e9/8
K = 100

DT <- data.table(
  id4 = sample(K, N, TRUE),                     # small groups (int)
  v1 =  sample(5, N, TRUE)                          # int in range [1,5
)

system.time(DT[, sum(v1),keyby = id4])
system.time(DT[, sum(v1),keyby = id4])

system.time(setkey(DT, id4))

system.time(DT[, sum(v1),id4])
system.time(DT[, sum(v1),id4])
