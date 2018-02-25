################################################################################
# setup
################################################################################
using Revise
using DataBench, FastGroupBy, ShortStrings, CategoricalArrays, SortingLab,
    SortingAlgorithms, Feather, BenchmarkTools, DataFrames, DataFramesMeta,
    Lazy, Query

srand(1);
const N = 10_000_000; const K = 100
@time df = createSynDataFrame(N, K); #31 #40

################################################################################
# DT[, sum(v1), keyby=id1]
# short string & string
# Status: SLOWER but if converted to Categorical then it's faster
################################################################################
# test String15
@time df[:id1_ss] = ShortString7.(df[:id1]);
# @time a = fastby(sum, df[:id1_ss], df[:v1]);
# @time a = fastby(sum, df[:id1_ss], df[:v1]);

# @time a = fastby(sum, df[:id1], df[:v1]);
# @time a = fastby(sum, df[:id1], df[:v1]);

sumv1id1_ss = @benchmark fastby(sum, df[:id1_ss], df[:v1])
sumv1id1 = @benchmark fastby(sum, df[:id1], df[:v1])

sumv1id1_dfm_fn(df) = @> begin
    df
    @by(:id1, sumx=sum(:v1))
end

sumv1id1_dfm = @benchmark sumv1id1_dfm_fn(df)

sumv1id1_query_fn(df) = @from i in df begin
    @group i by i.id1 into g
    @select {r=sum(g..v1)}
    @collect DataFrame
end
sumv1id1_query = @benchmark sumv1id1_query_fn(df)

# sumv1id1_df_fn(df) = aggregate(df[[:id1,:v1]], :id1, sum)
# sumv1id1_df = @benchmark sumv1id1_df_fn(df)

################################################################################
# DT[, sum(v1), keyby=id1]
# test categorical
# Status: FASTER 9x
################################################################################
@time df[:id1_cate] = categorical(df[:id1]); #7
@time df[:id1_cate] = compress(df[:id1_cate]); # 0.5
# @time sumby(df[:id1_cate], df[:v1]);
# @time fastby(sum, df[:id1_cate], df[:v1]);
# @time fastby(sum, df[:id1_cate], df[:v1]);

# @time fgroupreduce(+, df[:id1_cate], df[:v1]);
# @time fgroupreduce(+, df[:id1_cate], df[:v1]);

sumv1id1_cate = @benchmark fastby(sum, df[:id1_cate], df[:v1])
sumv1id1_cate_groupreduce = @benchmark fgroupreduce(+, df[:id1_cate], df[:v1])

sumv1id1_cate_dfm_fn(df) = @> begin
    df
    @by(:id1_cate, sumx=sum(:v1))
end

sumv1id1_cate_dfm = @benchmark sumv1id1_cate_dfm_fn(df)

umv1id1_cate_query_fn(df) = @from i in df begin
    @group i by i.id1_cate into g
    @select {r=sum(g..v1)}
    @collect DataFrame
end

umv1id1_cate_query = @benchmark umv1id1_cate_query_fn(df)

################################################################################
# DT[, sum(v1), keyby="id1,id2"]
# test categorical
# Status: FASTER 9x
################################################################################
@time df[:id2_cate] = categorical(df[:id2]); #7
@time df[:id2_cate] = compress(df[:id2_cate]); # 0.5
# @time fgroupreduce(+, (df[:id1_cate], df[:id2_cate]), df[:v1]);
# @time fgroupreduce(+, (df[:id1_cate], df[:id2_cate]), df[:v1]);

sumv1id1id2_groupreduce = @benchmark fgroupreduce(+, (df[:id1_cate], df[:id2_cate]), df[:v1])

sumv1id1id2_dfm_fn(df) = @> begin
    df
    @by([:id1_cate,:id2_cate],r = sum(:v1))
end

@benchmark sumv1id1id2_dfm_fn(df)

sumv1id1id2_query_fn(df) =  
@from i in df begin
    @group i by (i.id1,i.id2) into g
    @select {r=sum(g..v1)}
    @collect DataFrame
end

sumv1id1id2_query = @benchmark sumv1id1id2_query_fn(df)

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
@time df[:id3_cate] = categorical(df[:id3]) |> compress;
# import FastGroupBy: BaseRadixSortSafeTypes, fastby

sumv1meanv3id3_cate = @benchmark fastby((sum, mean), df[:id3_cate], (df[:v1], df[:v3]))
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

sumv1meanv3id3_cate_dfm_fn(df) = @> begin
    df
    @by([:id3], sumv1 = sum(:v1), meanv2=mean(:v2))
end

sumv1meanv3id3_cate_dfm = @benchmark sumv1meanv3id3_cate_dfm(df)
# BenchmarkTools.Trial:  memory estimate:  1022.60 MiB
#   allocs estimate:  7395590  --------------  
#   minimum time:     7.985 s (0.00% GC)  
#   median time:      7.985 s (0.00% GC)
#   mean time:        7.985 s (0.00% GC)
#   maximum time:     7.985 s (0.00% GC)
#   --------------
#   samples:          1
#   evals/sample:     1

sumv1meanv3id3_cate_ql_fn(df) = @from i in df begin
    @group i by i.id3 into g
    @select {s=sum(g..v1),m=mean(g..v3)}
    @collect DataFrame
end

sumv1meanv3id3_cate_ql = @benchmark sumv1meanv3id3_cate_ql_fn(df)


################################################################################
# DT[, lapply(.SD, mean), keyby=id4, .SDcols=7:9]
# TO BEAT: 0.6s for 10m; 2.76s for 100m; 6.45s for 250m
# TODO: make CountingSort
# Status: MUCH SLOWER if the number of elements is large and saturating memory
################################################################################
df[:id4_32] = Int32.(df[:id4])
df[:id4_8] = Int8.(df[:id4])

# @time a = fastby((mean, mean, mean), df[:id4], (df[:v1], df[:v2], df[:v3]));

# @time a = fastby((mean, mean, mean), df[:id4_32], (df[:v1], df[:v2], df[:v3]));
# @time a = fastby((mean, mean, mean), df[:id4_32], (df[:v1], df[:v2], df[:v3]));

# @time a = fastby((mean, mean, mean), df[:id4_8], (df[:v1], df[:v2], df[:v3]));
# @time a = fastby((mean, mean, mean), df[:id4_8], (df[:v1], df[:v2], df[:v3]));

mean79id4 = @benchmark fastby((mean, mean, mean), df[:id4], (df[:v1], df[:v2], df[:v3]))
# BenchmarkTools.Trial:
#   memory estimate:  540.57 MiB
#   allocs estimate:  2470
#   --------------
#   minimum time:     456.440 ms (0.00% GC)
#   median time:      494.681 ms (0.00% GC)
#   mean time:        587.366 ms (16.06% GC)
#   maximum time:     1.310 s (64.82% GC)
#   --------------
#   samples:          9
#   evals/sample:     1

mean79id4_dfm_fn(df) = @> df begin
    @by(:id4, mean1 = mean(:v1), mean2 = mean(:v2), mean3 = mean(:v3))
end

mean79id4_dfm = @benchmark mean79id4_dfm_fn(df)

mean79id4_ql_fn(df) = @from i in df begin
    @group i by i.id4 into g
    @select {m7=mean(g..v1),m8=mean(g..v2),m9=mean(g..v3)}
    @collect DataFrame
end

mean79id4_ql = @benchmark mean79id4_ql_fn(df)

################################################################################
# DT[, lapply(.SD, sum), keyby=id6, .SDcols=7:9]
# TO BEAT: 0.4s for 10m; 11.33s for 100m; 31.31 for 250m
################################################################################
df[:id6] = Int64.(df[:id6]); # this is needed not because it was `Int64` but `Float64`
# @time a = fastby((sum, sum, sum), df[:id6], (df[:v1], df[:v2], df[:v3]));
# @time a = fastby((sum, sum, sum), df[:id6], (df[:v1], df[:v2], df[:v3]));

sum79id6 = @benchmark fastby((sum, sum, sum), df[:id6], (df[:v1], df[:v2], df[:v3]))
# BenchmarkTools.Trial:
#   memory estimate:  598.48 MiB
#   allocs estimate:  1330706
#   --------------
#   minimum time:     950.109 ms (0.00% GC)
#   median time:      986.441 ms (0.00% GC)
#   mean time:        997.222 ms (0.00% GC)
#   maximum time:     1.095 s (0.00% GC)
#   --------------
#   samples:          6
#   evals/sample:     1

sum79id6_dfm_fn(df) = @> df begin
    @by(:id6, sum1 = sum(:v1), sum2 = sum(:v2), sum3 = sum(:v3))
end

sum79id6_dfm = @benchmark sum79id6_dfm_fn(df)

sum79id6_ql_fn(df) = @from i in df begin
    @group i by i.id6 into g
    @select {m7=sum(g..v1),m8=sum(g..v2),m9=sum(g..v3)}
    @collect DataFrame
end

sum79id6_ql = @benchmark sum79id6_ql_fn(df)

################################################################################
# generate r data
################################################################################
using RCall

R"""
memory.llimit()
library(data.table)
N=$N; K=$K
set.seed(1)
DT <- data.table(
  id1 = sample(sprintf("id%03d",1:K), N, TRUE),      # large groups (char)
  id2 = sample(sprintf("id%03d",1:K), N, TRUE),      # large groups (char)
  id3 = sample(sprintf("id%010d",1:(N/K)), N, TRUE), # small groups (char)
  id4 = sample(K, N, TRUE),                          # large groups (int)
  id5 = sample(K, N, TRUE),                          # large groups (int)
  id6 = sample(N/K, N, TRUE),                        # small groups (int)
  v1 =  sample(5, N, TRUE),                          # int in range [1,5]
  v2 =  sample(5, N, TRUE),                          # int in range [1,5]
  v3 =  sample(round(runif(100,max=100),4), N, TRUE) # numeric e.g. 23.5749
)
cat("GB =", round(sum(gc()[,2])/1024, 3), "\n")
replicate(5, list(
    system.time( DT[, sum(v1), keyby=id1] ),
    system.time( DT[, sum(v1), keyby=id1] ),
    system.time( DT[, sum(v1), keyby="id1,id2"] ),
    system.time( DT[, sum(v1), keyby="id1,id2"] ),
    system.time( DT[, list(sum(v1),mean(v3)), keyby=id3] ),
    system.time( DT[, list(sum(v1),mean(v3)), keyby=id3] ),
    system.time( DT[, lapply(.SD, mean), keyby=id4, .SDcols=7:9] ),
    system.time( DT[, lapply(.SD, mean), keyby=id4, .SDcols=7:9] ),
    system.time( DT[, lapply(.SD, sum), keyby=id6, .SDcols=7:9] ),
    system.time( DT[, lapply(.SD, sum), keyby=id6, .SDcols=7:9] )
))
"""

################################################################################
# Convert to IndexedTables.jl
################################################################################
using IndexedTables, JuliaDB
@time dfit = table(df) # 11s
@time dfit2 = reindex(dfit, (:id1,:id2,:id3,:id4,:id5,:id6)) #26

# see https://github.com/JuliaComputing/JuliaDB.jl/issues/139
@benchmark groupreduce(+, dfit2, :id6, select = :v1)

################################################################################
# plotting of results
################################################################################
