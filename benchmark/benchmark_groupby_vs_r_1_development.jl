################################################################################
# setup
################################################################################

using DataBench, FastGroupBy, ShortStrings, CategoricalArrays, SortingLab,
    SortingAlgorithms, BenchmarkTools, DataFrames

using Statistics: mean

using Random
Random.seed!(1);
const N = 10_000_000; const K = 100

@time df = createSynDataFrame(N, K); #31 #40

# using JuliaDB, IterableTables
# @time dft = table(df)

# if isfile("df$(N÷1_000_000)m.feather")
#     @time df = Feather.read("df$(N÷1_000_000)m.feather")
# else
#     @time df = createSynDataFrame(N, K); #31 #40
#     @time Feather.write("df$(N÷1_000_000)m.feather", df) # save for future use
# end
# ZJ: It's actually slower to load from feather

################################################################################
# DT[, sum(v1), keyby=id1]
# short string & string
# Status: SLOWER but if converted to Categorical then it's faster
################################################################################
# test String15
# @time df[!, :id1_ss] = ShortString7.(df[!, :id1]);
# @time a = fastby(sum, df[!, :id1_ss], df[!, :v1]);
# @time a = fastby(sum, df[!,:id1_ss], df[!,:v1]);
#
# @time a = fastby(sum, df[!,:id1], df[!,:v1]);
# @time a = fastby(sum, df[!,:id1], df[!,:v1]);

################################################################################
# DT[, sum(v1), keyby=id1]
# test categorical
# Status: FASTER 9x
################################################################################
@time df[!,:id1_cate] = categorical(df[!,:id1]); #7
@time df[!,:id1_cate] = compress(df[!,:id1_cate]); # 0.5
# @time sumby(df[!,:id1_cate], df[!,:v1]);
@time fastby(sum, df[!,:id1_cate], df[!,:v1]);
@time fastby(sum, df[!,:id1_cate], df[!,:v1]);

# @time fgroupreduce(+, df[!,:id1_cate], df[!,:v1]);
# @time fgroupreduce(+, df[!,:id1_cate], df[!,:v1]);

################################################################################
# DT[, sum(v1), keyby="id1,id2"]
# test categorical
# Status: FASTER 9x
################################################################################
@time df[!,:id2_cate] = categorical(df[!,:id2]); #7
@time df[!,:id2_cate] = compress(df[!,:id2_cate]); # 0.5
# @time fgroupreduce(+, (df[!,:id1_cate], df[!,:id2_cate]), df[!,:v1]);
# @time fgroupreduce(+, (df[!,:id1_cate], df[!,:id2_cate]), df[!,:v1]);

################################################################################
# DT[, list(sum(v1),mean(v3)), keyby=id3]
# test categorical
# Status: SLOWER
# TODO: make a fast fsortandperm for integers
# TODO: make this faster than data.table
# TODO: look for mean and make it into a reduce
# TO BEAT: 0.6s for 10m; 14.11s for 100m;
#
# NOTES:
# The key issue here is that we need to sort two vectors in order to perform the
# groupby operation. R's seems to have a really fast way to doing that
# Potential make the group into radix sort instead of counting sort
#
# What about making an RLE vector type?
#
# I can radixsort the 10m refs in 0.08 seconds so still 0.52 seconds to distribute
#
# I can sortperm the 10m refs in 0.28 seconds so still
################################################################################
@time df[!,:id3_cate] = categorical(df[!,:id3]) |> compress;
# import FastGroupBy: BaseRadixSortSafeTypes, fastby

# @benchmark fastby((sum, mean), df[!,:id3_cate], (df[!,:v1], df[!,:v3]))
# BenchmarkTools.Trial:
#   memory estimate:  435.87 MiB
#   allocs estimate:  1000315
#   --------------
#   minimum time:     1.084 s (0.00% GC)
#   median time:      1.130 s (0.00% GC)  mean time:        1.133 s (0.00% GC)
#   maximum time:     1.170 s (0.00% GC)
#   --------------
#   samples:          5
#   evals/sample:     1

# BenchmarkTools.Trial:
#   memory estimate:  430.21 MiB
#   allocs estimate:  977648
#   --------------
#   minimum time:     801.169 ms (0.00% GC)
#   median time:      912.131 ms (0.00% GC)
#   mean time:        945.560 ms (0.00% GC)
#   maximum time:     1.200 s (0.00% GC)
#   --------------
#   samples:          6
#   evals/sample:     1

# alternatives
Base.zero(t::Type{T}) where T <: Tuple = ((zero(p) for p in t.parameters)...)

function fastby2(fns, byvec, valvec)
    xx = zip(valvec...) |> collect
    a = fgroupreduce((x,y)->(x[1]+y[1], x[2]+y[2], x[3] + 1), df[!,:id3_cate], xx, (0,0.0,0))
end

function fastby3(fns, byvec, valvec)
    (fgroupreduce(+, byvec, v) for (fn, v) in zip(fns, valvec)) |> collect
end

@time fastby3((sum, mean), df[!,:id3_cate], (df[!,:v1], df[!,:v3]));
# BenchmarkTools.Trial:
#   memory estimate:  885.54 MiB
#   allocs estimate:  10000828
#   --------------
#   minimum time:     25.588 s (48.27% GC)
#   median time:      26.755 s (50.79% GC)
#   mean time:        26.430 s (50.20% GC)
#   maximum time:     27.005 s (51.47% GC)
#   --------------
#   samples:          5
#   evals/sample:     1
@time fastby2((sum, mean), df[!,:id3_cate], (df[!,:v1], df[!,:v3]));
# BenchmarkTools.Trial:
#   memory estimate:  1.95 GiB  allocs estimate:  5000410  --------------
#   minimum time:     14.963 s (37.27% GC)
#   median time:      18.198 s (48.77% GC)
#   mean time:        18.539 s (48.99% GC)
#   maximum time:     21.212 s (53.51% GC)
#   --------------
#   samples:          5
#   evals/sample:     1

@benchmark fastby3((sum, mean), df[!,:id3_cate], (df[!,:v1], df[!,:v3])) samples=5 evals=1 seconds=120
# BenchmarkTools.Trial:
#   memory estimate:  100.84 MiB
#   allocs estimate:  1000671
#   --------------
#   minimum time:     640.946 ms (0.00% GC)
#   median time:      661.054 ms (0.00% GC)
#   mean time:        691.247 ms (0.00% GC)
#   maximum time:     762.335 ms (0.00% GC)
#   --------------
#   samples:          5
#   evals/sample:     1

@benchmark fastby2((sum, mean), df[!,:id3_cate], (df[!,:v1], df[!,:v3])) samples=5 evals=1 seconds=120
# BenchmarkTools.Trial:
#   memory estimate:  206.06 MiB
#   allocs estimate:  500331
#   --------------
#   minimum time:     439.494 ms (0.00% GC)
#   median time:      472.370 ms (0.00% GC)
#   mean time:        479.215 ms (0.00% GC)
#   maximum time:     558.852 ms (0.00% GC)
#   --------------
#   samples:          5
#   evals/sample:     1

################################################################################
# DT[, lapply(.SD, mean), keyby=id4, .SDcols=7:9]
# TO BEAT: 0.6s for 10m; 2.76s for 100m; 6.45s for 250m
# TODO: make CountingSort
# Status: MUCH SLOWER if the number of elements is large and saturating memory
################################################################################
df[!,:id4_32] = Int32.(df[!,:id4])
df[!,:id4_8] = Int8.(df[!,:id4])

@time a = fastby((mean, mean, mean), df[!,:id4], (df[!,:v1], df[!,:v2], df[!,:v3]));
@time a = fastby((mean, mean, mean), df[!,:id4], (df[!,:v1], df[!,:v2], df[!,:v3]));

@time a = fastby((mean, mean, mean), df[!,:id4_32], (df[!,:v1], df[!,:v2], df[!,:v3]));
@time a = fastby((mean, mean, mean), df[!,:id4_32], (df[!,:v1], df[!,:v2], df[!,:v3]));

@time a = fastby((mean, mean, mean), df[!,:id4_8], (df[!,:v1], df[!,:v2], df[!,:v3]));
@time a = fastby((mean, mean, mean), df[!,:id4_8], (df[!,:v1], df[!,:v2], df[!,:v3]));

################################################################################
# DT[, lapply(.SD, sum), keyby=id6, .SDcols=7:9]
# TO BEAT: 0.4s for 10m; 11.33s for 100m; 31.31 for 250m
################################################################################
df[!,:id6] = Int32.(df[!,:id6]); # this is needed not because it was `Int64` but `Float64`
@time a = fastby((sum, sum, sum), df[!,:id6], (df[!,:v1], df[!,:v2], df[!,:v3]));
@time a = fastby((sum, sum, sum), df[!,:id6], (df[!,:v1], df[!,:v2], df[!,:v3]));

# @time a = FastGroupBy.fastby4((sum, sum, sum), df[!,:id6], (df[!,:v1], df[!,:v2], df[!,:v3]));
# fns, byvec, valvec = (sum, sum, sum), df[!,:id6], (df[!,:v1], df[!,:v2], df[!,:v3])

using Base.Threads
function fastby2(fns, byvec, valvec)
    l = length(fns)
    for i=1:l
        fastby(fns[i], byvec, valvec[i])
    end
end

function fastby3(fns, byvec, valvec)
    l = length(fns)
    @threads for i=1:l
        fastby(fns[i], byvec, valvec[i])
    end
end

@time a = fastby2((sum, sum, sum), df[!,:id6], (df[!,:v1], df[!,:v2], df[!,:v3]));
@time a = fastby3((sum, sum, sum), df[!,:id6], (df[!,:v1], df[!,:v2], df[!,:v3]));
@time a = fastby.((sum, sum, sum), (df[!,:id6], df[!,:id6], df[!,:id6]), (df[!,:v1], df[!,:v2], df[!,:v3]));

@time a = fgroupreduce(+, df[!,:id6], df[!,:v1])

@benchmark fastby(sum, df[!,:id6], df[!,:v1]) samples=5 evals=1 seconds=60

@time aggregate(df[[:id4, :v1]], :id4, sum)

using BenchmarkTools
@benchmark fastby2((sum, sum, sum), df[!,:id6], (df[!,:v1], df[!,:v2], df[!,:v3]))
# BenchmarkTools.Trial:
#   memory estimate:  370.06 MiB
#   allocs estimate:  300124
#   --------------
#   minimum time:     246.012 ms (0.00% GC)
#   median time:      258.472 ms (0.00% GC)
#   mean time:        318.356 ms (18.04% GC)
#   maximum time:     1.192 s (77.09% GC)
#   --------------
#   samples:          16
#   evals/sample:     1
@benchmark fastby3((sum, sum, sum), df[!,:id6], (df[!,:v1], df[!,:v2], df[!,:v3]))
# BenchmarkTools.Trial:
#   memory estimate:  291.33 MiB
#   allocs estimate:  254586
#   --------------
#   minimum time:     120.978 ms (0.00% GC)
#   median time:      136.074 ms (0.00% GC)
#   mean time:        222.204 ms (37.22% GC)
#   maximum time:     1.235 s (88.69% GC)
#   --------------
#   samples:          25
#   evals/sample:     1

@time a = fastby(sum, df[!,:id6], df[!,:v1])
@which fastby(sum, df[!,:id6], df[!,:v1])

################################################################################
# plotting of results
################################################################################
