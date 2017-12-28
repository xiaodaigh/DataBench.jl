__precompile__(true)
module DataBench
using IndexedTables, PooledArrays, NamedTuples, RCall, JuliaDB
import Base.ht_keyindex
using FastGroupBy, SplitApplyCombine

export R_bench, createIndexedTable, run_juliadb_bench, run_juliadb_bench_pmap
export gen_string_vec_fixed_len, gen_string_vec_var_len

include("julia/indexedtable.jl_bench.jl")
include("julia/rbench.jl")
include("julia/string_sort/string_sort.jl")
#
# function saveJuliaResult(julres, outpath = "test/results/")
#   CSV.write(julres)
# end

# package code goes here

end # module
