library(fst)
library(arrow)
library(data.table)
library(magrittr)
library(ggplot2)


format_compare = function(input, title) {
  data.table_read = system.time(a <- fread(input))[3]
  data.table_write = system.time(fwrite(a, tempfile()))[3]
  
  tf1 = tempfile()
  fst_write = system.time(write_fst(a, tf1))[3]
  fst_read = system.time(read_fst(tf1))[3]
  
  tf2 = tempfile()
  parquet_write = system.time(write_parquet(a, tf2))[3]
  parquet_read = system.time(read_parquet(tf2))[3]
  
  b = data.table(
    `Package/format` = c("data.table/csv", "fst/fst", "arrow/parquet"),
    Legend = rep(c("read", "write"), each = 3),
    timings  = c(
      c(data.table_read, fst_read, parquet_read), 
      c(data.table_write, fst_write, parquet_write))
  )
  
  p = b %>% 
    ggplot + 
    geom_bar(aes(x = `Package/format`, weight = timings, fill=Legend), position="dodge") +
    ylab("Seconds") + 
    ggtitle(title)
  p
}

input = "c:/data/feature_matrix_cleaned.csv"
format_compare("c:/data/feature_matrix_cleaned.csv", "Read/Write performance: 5GB, 300k rows, and ~2000 columns" )


input = "c:/data/perf/Performance_2004Q3.txt"
a=format_compare(input, "Read/Write performance: 2GB, 27m rows, 31 columns")


