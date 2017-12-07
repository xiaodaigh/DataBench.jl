source("src/R/0_setup.r")

library(pipeR)
library(data.table)
library(future)
plan(multiprocess)
pt <- proc.time()

aa1 <- dir(file.path(data_path,"InitialTrainingSet_rev1/"), full.names = T) %>>% 
  sapply(function(x) file.path(x,"ASDI","asdifpwaypoint.csv")) %>>% 
  future_lapply(fread) %>>% rbindlist
data.table::timetaken(pt)

system.time(fst::write.fst(aa1,file.path(data_path,"init_training_set.fst",100)))
system.time(feather::write_feather(aa1,file.path(data_path,"/init_training_set.feather")))

# pt <- proc.time()
# aa2 <- dir("D:/data/InitialTrainingSet_rev1/", full.names = T) %>>% 
#    sapply(function(x) file.path(x,"ASDI","asdifpwaypoint.csv")) %>>% 
#    lapply(fread)
# data.table::timetaken(pt) # 5:53

a = data.table::fread("D:/data/lending_club/csv/in one place/LoanStats_2017Q2.csv")
