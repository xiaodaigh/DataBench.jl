using CSV, DataFrames, DataFramesMeta
respath = "benchmark/bench_string_results/"
paths = readdir(respath)
@time df = reduce(vcat, CSV.read.(respath.*paths))

# create some features
df[:logn] = log(10,df[:n])
df[:slogn] = df[:strlen].*log(10,df[:n])

only_radix_sort(test, strlen) = test == "julia_lsd_radixsort_elapsed" ? strlen : 0
df[:strlen_radix] = only_radix_sort.(df[:test],df[:strlen])
df[:loge] = log(df[:elapsed])

using GLM
lm(@formula(loge ~ test*logn + test*strlen - 1), df)


df_unique 
df[:predicted] = exp.(predict(m, df))

df[[:test, :elapsed, :predicted, :strlen, :n]]

