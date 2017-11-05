using Feather, DataFrames, StatsBase, BoskoDB
import DataFrames.DataFrame

using JuliaDB
addprocs()
@everywhere using JuliaDB, Dagger, BoskoDB

const lc_feather_path = "C:/Users/dzj/Documents/lc.feather"
const lc_csv_path = "C:/Users/dzj/Downloads/Lending Club/csv/"
