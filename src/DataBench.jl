module DataBench

using PooledArrays, RCall
import Base.ht_keyindex
# using FastGroupBy

export R_bench, createIndexedTable, run_juliadb_bench, run_juliadb_bench_pmap
export gen_string_vec_fixed_len, gen_string_vec_var_len, gen_string_vec_id_fixed_len
export createSynDataFrame

# include("julia/blogpost/indexedtable.jl_bench.jl")
include("julia/r_data_table_group_by/rbench.jl")
include("julia/r_data_table_group_by/DataFrames.jl")

end # module
