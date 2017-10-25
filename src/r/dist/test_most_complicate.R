library(data.table)
source("src/r/1_generate_synthetic.r")
timings <- list()
timings$gen_syn <- system.time(DT <- gen_datatable_synthetic(2e9/8,100))[3]
timings$sum7_9_by_id6_1 <- system.time( DT[, lapply(.SD, sum), keyby=id6, .SDcols=7:9] )[3] # 35
timings$sum7_9_by_id6_2 <- system.time( DT[, lapply(.SD, sum), keyby=id6, .SDcols=7:9] )[3]

timings$sum7_9_by_id6_1a <- system.time( DT[, lapply(.SD, sum), keyby=id6, .SDcols=7:9] )[3]
timings$sum7_9_by_id6_2a <- system.time( DT[, lapply(.SD, sum), keyby=id6, .SDcols=7:9] )[3]

file1 = paste0("benchmark/results/",Sys.time(),".rds")
file1 = gsub(":","",file1)
saveRDS(timings,file=file1)
