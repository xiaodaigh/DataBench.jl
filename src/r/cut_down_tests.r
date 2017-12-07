library(data.table)
#N = 2^31 - 1
N = 2e9/8
K = 100

DT <- data.table(
  #id6 = sample(N/K, N, TRUE)                      # small groups (int)
  #,id3 = sample(sprintf("id%010d",1:(N/K)), N, TRUE),        
  id4 = sample(K, N, TRUE)
  ,v1 =  sample(5, N, TRUE)                          # int in range [1,5
)

# group by id6
system.time(DT[, sum(v1), keyby = id4])
system.time(DT[, sum(v1), keyby = id4])
# group by id6
system.time(DT[, sum(v1), keyby = id6])
system.time(DT[, sum(v1), keyby = id6])

# pre-index id6
system.time(setkey(DT, id6))
system.time(DT[, sum(v1), keyby = id6])
system.time(DT[, sum(v1), keyby = id6])


# group by id6
system.time(DT[, .N, keyby = id6])
system.time(DT[, .N, keyby = id6])

# pre-index id6
system.time(setkey(DT, id6))
system.time(DT[, .N, keyby = id6])
system.time(DT[, .N, keyby = id6])


# group by id6
system.time(DT[, sum(v1),keyby = id6])
system.time(DT[, sum(v1),keyby = id6])

# pre-index id6
system.time(setkey(DT, id6))
system.time(DT[, sum(v1),keyby = id6])
system.time(DT[, sum(v1),keyby = id6])

# sort by id3 (strings)
system.time(DT[, sum(v1),keyby = id3])
system.time(DT[, sum(v1),keyby = id3])

# pre-index id3
system.time(setkey(DT, id3))
system.time(DT[, sum(v1),id3])
system.time(DT[, sum(v1),id3])

library(data.table)
library(ggplot2)
a <- fread("C:/Users/dzj/.julia/v0.6/FastGroupBy/hehe6.csv")

ggplot(a) + geom_line(aes(x=log(n), y=log(x1), color=algo))
