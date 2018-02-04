using DataBench, FastGroupBy, ShortStrings, CategoricalArrays

srand(1);
const N = 100_000_000; const K = 100
@time df = createSynDataFrame(N, K)
df[:id1_cate] = categorical(df[:id1])
df[:id1_cate] = compress(df[:id1_cate])
@time sumby(df[:id1_cate], df[:v1])
@time sumby(df[:id1_cate], df[:v1])


@time fastby(sum, df[:id1], df[:v1]);
@time fastby(sum, df[:id1], df[:v1]);


df[:id1_ss] = ShorterString.(df[:id1])

df[:id1_cate] = categorical(df[:id1])
df[:id1_cate] = compress(df[:id1_cate])

@time fastby(sum, df[:id_ss], df[:v1])

@time fastby(sum, df[:id1_cate], df[:v1])
@time fastby(sum, df[:id1_cate], df[:v1])



@time fgroupreduce(+, (df[:id1_cate], df[:id1_cate]), df[:v1])
@time fgroupreduce(+, df[:id1_cate], df[:v1])

#DT[, sum(v1), keyby=id1]



# system.time( DT[, sum(v1), keyby="id1,id2"] )

# system.time( DT[, list(sum(v1),mean(v3)), keyby=id3] )

# system.time( DT[, lapply(.SD, mean), keyby=id4, .SDcols=7:9] )

# system.time( DT[, lapply(.SD, sum), keyby=id6, .SDcols=7:9] )

