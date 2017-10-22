__precompile__(true)
module DataBench

export R_bench, createIndexedTable, run_juliadb_bench, meanby

include("julia/indexedtable.jl_bench.jl")
include("julia/meanby.jl")
include("julia/rbench.jl")
#
# function saveJuliaResult(julres, outpath = "test/results/")
#   CSV.write(julres)
# end

# package code goes here

end # module
