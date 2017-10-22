using DataBench
using Base.Test

#using DataFrames, CSV
#import Base.ht_keyindex
# write your own tests here
@time a = R_bench(;libpath = "C:/Users/dzj/Documents/R/win-library/3.4")
@time b = run_juliadb_bench()

# compute relativities to R's data.table
c = Dict(n => b[n]/a[n][1] for n in names(a))
@test length(c) == 11
