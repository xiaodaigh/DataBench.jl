module DataBench

export R_bench, createIndexedTable, run_juliadb_bench, run_juliadb_bench_pmap

include("julia/indexedtable.jl_bench.jl")
include("julia/rbench.jl")
#
# function saveJuliaResult(julres, outpath = "test/results/")
#   CSV.write(julres)
# end

# package code goes here

end # module
