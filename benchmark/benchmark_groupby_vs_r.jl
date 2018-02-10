using Revise
using DataBench, FastGroupBy, ShortStrings, CategoricalArrays, uCSV,
    SortingAlgorithms, Feather, BenchmarkTools, CSV, TextParse, FileIO
using IterableTables, JuliaDB, IndexedTables

srand(1);
const N = 1_000_000; const K = 100
@time df = createSynDataFrame(N, K); #31 #40


@time df = 
# test String15
@time df[:id1_ss] = ShortString7.(df[:id1]);
@time fastby(sum, df[:id1_ss], df[:v1]);
@time fastby(sum, df[:id1], df[:v1]);

# test categorical
@time df[:id1_cate] = categorical(df[:id1]); #7
@time df[:id1_cate] = compress(df[:id1_cate]); # 0.5
@time sumby(df[:id1_cate], df[:v1]);
@time fastby(sum, df[:id1_cate], df[:v1]);

@time fgroupreduce(+, (df[:id1_cate], df[:id1_cate]), df[:v1]);
@time fgroupreduce(+, df[:id1_cate], df[:v1]);

@time df[:id3_cate] = categorical(df[:id3]) |> compress

@time fastby(sum, df[:id3_cate], df[:v1]);

@benchmark fgroupreduce(+, $df[:id3_cate], $df[:v1]) evals=1 samples=5 seconds=120

@time Feather.write("df.feather", df)
@time df = Feather.read("df.feather")

@time save(File(format"JLD","df.jld"), "df", df)))


@time dfit = table(df)

@time JuliaDB.save(dfit,"d:/dataabc")

using CSV

refs = df[:id3_cate].refs

extrema(refs)
#DT[, sum(v1), keyby=id1]
# system.time( DT[, sum(v1), keyby="id1,id2"] )

# system.time( DT[, list(sum(v1),mean(v3)), keyby=id3] )

# system.time( DT[, lapply(.SD, mean), keyby=id4, .SDcols=7:9] )

# system.time( DT[, lapply(.SD, sum), keyby=id6, .SDcols=7:9] )

using Distributions, StatPlots, Plots

plot(
    Truncated(
        Normal(0,1),
    -1.75, 1.8)
    , xlim = (-2,2)
    , ylim = (0, 0.5)
    , fill = (0, 0.5, :red)
    , xticks =([ -1.75, -1, 0, 1, 1.8], string.([ -1.75, -1, 0, 1, 1.8]))
    , xlabel = "Z"
    , label = "Truncated Normal pdf"
    , title = "Truncated normal"
)
savefig("1.png")
plot(
    Truncated(
        Normal(0,1),
    -1.75, Inf)
    , xlim = (-3,3)
    , ylim = (0, 0.5)
    , fill = (0, 0.5, :red)
    , xticks =([-1.75, -1, 0, 1, 2], string.([-1.75,-1, 0, 1, 2]))
    , label = "Truncated Normal pdf - with conservatism"
    , xlabel = "Z"
    , title = "Truncated normal pdf - with conservatism"
)
savefig("2.png")

plot(
    Truncated(
        Normal(-2.6,0.1^2),
    -Inf, -2.58)
    , xlim = (-2.65,-2.55)
    # , ylim = (0, 0.5)
    , fill = (0, 0.5, :green)
    , xticks =(reverse(-2.6 .+ -0.1.*[-1.75, -1, 0, 1, 2]), string.(reverse(-2.6 .+ -0.1.*[-1.75, -1, 0, 1, 2])))
    , label = "Truncated Normal pdf - with conservatism"
    , xlabel = "\Phi^-1 ODR = \mu - \sigma Z"
    , title = "Truncated normal pdf - with conservatism"
)
savefig("3.png")

plot(Vector[rand(10),[0.1,0.9],[0.4,0.5]], linetypes=[:line,:vline,:hline], widths=[2,5,10])
plot([-1.75, 1.8], linetypes=[:vline] widths=[1])

vline(-1.75)

reverse(-2.6 .+ -0.1.*[-1.75, -1, 0, 1, 2])

x = [randstring(rand(4:16)) for i = 1:1_000_000]

using SortingLab
radixsort(x[1:1]) # compile
sort(x[1:1])
@time radixsort(x)
@time sort(x)
@time sort(x, alg=QuickSort)


using ShortStrings

x = [randstring(rand(4:15)) for i = 1:1_000_000]
sx =  ShortString15.(x)
ShortStrings.fsort(sx[1:1]) # compile
sort(x[1:1])
@time ShortStrings.fsort(sx) #0.23s
@time sort(sx) #1.27s

x = [randstring(16) for i = 1:1_000_000]

using SortingLab
radixsort(x[1:1]) # compile
sort(x[1:1])
@time radixsort(x)
@time sort(x)
@time sort(x, alg=QuickSort)

using Parquet

