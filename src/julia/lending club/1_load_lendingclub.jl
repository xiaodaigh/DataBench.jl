include("0_setup.jl");

R"""
library(data.table)
library(magrittr)

system.time(lc <- dir($lc_csv_path, full.names = T) %>%
  sapply(dir, full.names = T) %>%
  lapply(fread, na.strings = NULL) %>%
  rbindlist)

feather::write_feather(lc, $lc_feather_path)
"""
