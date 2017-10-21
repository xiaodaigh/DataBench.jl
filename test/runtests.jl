using DataBench
using Base.Test

using DataFrames, CSV

# write your own tests here
@test 1 == 1

@time a = R_bench(;libpath = "C:/Users/dzj/Documents/R/win-library/3.4")
@time b = run_juliadb_bench()

# compute relativities to R's data.table
c = Dict(n => b[n]/a[n][1] for n in names(a))

@test length(c) == 11
