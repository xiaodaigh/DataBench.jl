################################################################################
# setup
################################################################################
using Revise
using DataBench, FastGroupBy, ShortStrings, CategoricalArrays, uCSV,
    SortingAlgorithms, Feather, BenchmarkTools, CSV, TextParse, FileIO, 
    IterableTables, JuliaDB, IndexedTables, Feather, DataFrames

srand(1);
const N = 100_000_000; const K = 100

df = DataFrame()
if isfile("df$(N÷1_000_000)m.feather") 
    @time df = Feather.read("df$(N÷1_000_000)m.feather")
else
    @time df = createSynDataFrame(N, K); #31 #40
    @time Feather.write("df$(N÷1_000_000)m.feather", df) # save for future use
end

################################################################################
# DT[, sum(v1), keyby=id1]
# short string & string
################################################################################
# test String15
@time df[:id1_ss] = ShortString7.(df[:id1]);
@time fastby(sum, df[:id1_ss], df[:v1]);
@time fastby(sum, df[:id1], df[:v1]);


################################################################################
# DT[, sum(v1), keyby=id1]
# test categorical
################################################################################
@time df[:id1_cate] = categorical(df[:id1]); #7
@time df[:id1_cate] = compress(df[:id1_cate]); # 0.5
@time sumby(df[:id1_cate], df[:v1]);
@time fastby(sum, df[:id1_cate], df[:v1]);
@time fgroupreduce(+, df[:id1_cate], df[:v1]);


################################################################################
# DT[, sum(v1), keyby="id1,id2"]
# test categorical
################################################################################
@time df[:id2_cate] = categorical(df[:id2]); #7
@time df[:id2_cate] = compress(df[:id2_cate]); # 0.5
@time fgroupreduce(+, (df[:id1_cate], df[:id2_cate]), df[:v1]);

################################################################################
# DT[, list(sum(v1),mean(v3)), keyby=id3]
# test categorical
################################################################################
@time df[:id3_cate] = categorical(df[:id3]) |> compress;
# @time fastby(sum, df[:id3_cate], df[:v1]);
# @time fastby(sum, df[:id3_cate], df[:v1]);
@time a = fastby([sum, mean], df[:id3_cate], (df[:v1], df[:v3]));

# using RCall

# R"""
# memory.limit(2^31-1)
# library(data.table)
# df = feather::read_feather("df100m.feather")
# setDT(df)
# names(df)
# # system.time(df[, list(sum(v1),mean(v3)), keyby=id3])
# """

# R"""
# names(df)
# """


if false
    import FastGroupBy: fastby
    byvec =  df[:id3_cate]
    refs = byvec.refs
    s = SortingLab.fsortperm(byvec.refs)

    val = df[s,:v1]
    val2 = df[s,:v3]
    byvec = refs[s]
    valvec = tuple(val, val2)
    fns = [sum, mean]

    valvec = val
    fn = sum
end

using JLD
JLD.save("df100m.jld","df", df) #ERROR: StackOverflowError:

################################################################################
# DT[, lapply(.SD, mean), keyby=id4, .SDcols=7:9]
################################################################################
import FastGroupBy: BaseRadixSortSafeTypes, fastby
using Base.Threads
df[:id6] = Int32.( df[:id6])


@time a = fastby(sum, df[:id4], (df[:v1], df[:v2], df[:v3]));
@time a = fastby(sum, df[:id6], (df[:v1], df[:v2], df[:v3]));


function fastby2(fn::Function, byvec::AbstractVector{T}, valvec::NTuple{3, AbstractVector}) where T <: BaseRadixSortSafeTypes
    vs1 = valvec[1]
    FastGroupBy.grouptwo!(byvec, vs1)
    FastGroupBy._contiguousby_vec(sum, byvec, vs1)
end

@time fastby2(sum, df[:id4], (df[:v1], df[:v2], df[:v3]))
@time fastby2(sum, df[:id6], (df[:v1], df[:v2], df[:v3]))

using SortingAlgorithms
df1=df[[:id4,:v1,:v2,:v3]]
@time sort!(df1, cols=:id4, alg=RadixSort)

if false
    df[:id6] = Int32.(df[:id6])
    byvec = df[:id6]
    valvec = df[:v2]

    @time s = SortingLab.fsortperm(byvec);
    @time valvec[s];
    @time sort(byvec)

    valvec = (df[:v1], df[:v2], df[:v3])
end

################################################################################
# plotting of results
################################################################################
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

