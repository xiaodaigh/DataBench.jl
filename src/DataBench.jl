__precompile__(true)
module DataBench
using IndexedTables, PooledArrays, NamedTuples, RCall, JuliaDB
import Base.ht_keyindex
using FastGroupBy

export R_bench, createIndexedTable, run_juliadb_bench, run_juliadb_bench_pmap
export gen_string_vec_fixed_len, gen_string_vec_var_len, gen_string_vec_id_fixed_len

include("julia/indexedtable.jl_bench.jl")
include("julia/rbench.jl")
include("julia/string_sort/string_sort.jl")


end # module
