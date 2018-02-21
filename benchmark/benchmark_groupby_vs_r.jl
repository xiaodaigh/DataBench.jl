################################################################################
# setup
################################################################################
using Revise
using DataBench, FastGroupBy, ShortStrings, CategoricalArrays, SortingLab,
    SortingAlgorithms, Feather, BenchmarkTools, DataFrames

srand(1);
const N = 100_000_000; const K = 100

@time df = createSynDataFrame(N, K); #31 #40
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
@time df[:id1_ss] = ShortString7.(df[:id1]);
@time fastby(sum, df[:id1_ss], df[:v1]);
@time fastby(sum, df[:id1], df[:v1]);


################################################################################
# DT[, sum(v1), keyby=id1]
# test categorical
# Status: FASTER 9x
################################################################################
@time df[:id1_cate] = categorical(df[:id1]); #7
@time df[:id1_cate] = compress(df[:id1_cate]); # 0.5
# @time sumby(df[:id1_cate], df[:v1]);
# @time fastby(sum, df[:id1_cate], df[:v1]);
@time fgroupreduce(+, df[:id1_cate], df[:v1]);
@time fgroupreduce(+, df[:id1_cate], df[:v1]);


################################################################################
# DT[, sum(v1), keyby="id1,id2"]
# test categorical
# Status: FASTER 9x
################################################################################
@time df[:id2_cate] = categorical(df[:id2]); #7
@time df[:id2_cate] = compress(df[:id2_cate]); # 0.5
@time fgroupreduce(+, (df[:id1_cate], df[:id2_cate]), df[:v1]);
@time fgroupreduce(+, (df[:id1_cate], df[:id2_cate]), df[:v1]);

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
# @time fastby(sum, df[:id3_cate], df[:v1]);
# @time fastby(sum, df[:id3_cate], df[:v1]);
@time a = fastby([sum, mean], df[:id3_cate], (df[:v1], df[:v3]));
@time a = fastby([sum, mean], df[:id3_cate], (df[:v1], df[:v3]));
@benchmark fastby([sum, mean], $df[:id3_cate], ($df[:v1], $df[:v3])) samples=5 evals=1 seconds=600

if false
    import FastGroupBy: fastby
    byvec1 =  df[:id3_cate]
    refs = byvec1.refs
    byvec = copy(byvec1.refs)
    x = collect(1:length(byvec))
    
    @time orderx = [b.first for b in ab]

    @time val = df[:v1];
    @time valv = @view(val[orderx]);
    @time val2 = df[:v3];
    @time val2v = @view(val2[orderx]);

    valvec = (df[:v1], df[:v3])
    fn = [sum, mean]

    @time sortperm(refs); # 0.289167 seconds (8 allocations: 77.057 MiB, 7.54% gc time)
    @time SortingLab.fsortperm(refs); # 0.267444 seconds (20 allocations: 228.906 MiB, 8.29% gc time)
    @benchmark ab = SortingLab.fsortandperm_int_range_lsd(refs, N÷K, 1)
    # 10m
    # BenchmarkTools.Trial:
    # memory estimate:  152.64 MiB
    # allocs estimate:  11
    # --------------
    # minimum time:     183.423 ms (0.00% GC)
    # median time:      196.381 ms (0.00% GC)
    # mean time:        199.993 ms (0.00% GC)
    # maximum time:     244.906 ms (0.00% GC)
    # --------------
    # samples:          26
    # evals/sample:     1

    # 100m
    # BenchmarkTools.Trial:
    # memory estimate:  1.49 GiB
    # allocs estimate:  12
    # --------------
    # minimum time:     2.187 s (0.05% GC)
    # median time:      2.377 s (6.52% GC)
    # mean time:        2.455 s (10.56% GC)
    # maximum time:     2.801 s (22.20% GC)
    # --------------
    # samples:          3
    # evals/sample:     1

    

    if false
        @time res = fastby([sum, mean], byvec, (val, val2)) # 13.65
        @time res = fastby([sum, mean], byvec, (val, val2)) # 11.11
    end

    @time fastby([sum, mean], byvec, (val, val2))




    s = SortingLab.fsortperm(byvec.refs)

    val = df[s,:v1]
    val2 = df[s,:v3]
    byvec = refs[s]
    valvec = tuple(val, val2)
    fns = [sum, mean]

    valvec = val
    fn = sum
end

################################################################################
# DT[, lapply(.SD, mean), keyby=id4, .SDcols=7:9]
################################################################################
import FastGroupBy: BaseRadixSortSafeTypes, fastby
using Base.Threads
df[:id6] = Int32.( df[:id6])
df[:id4_cate] = categorical(df[:id4]) |> compress


@time a = FastGroupBy.fgroupreduce(+, df[:id4], df[:v1]);
@time a = FastGroupBy.fgroupreduce(+, df[:id4], df[:v2]);
@time a = FastGroupBy.fgroupreduce(+, df[:id4], df[:v3]);

@time a = FastGroupBy.fgroupreduce(+, df[:id4_cate], df[:v1]);
@time a = FastGroupBy.fgroupreduce(+, df[:id4_cate], df[:v2]);
@time a = FastGroupBy.fgroupreduce(+, df[:id4_cate], df[:v3]);


@time a = FastGroupBy.fastby(sum, df[:id4], (df[:v1], df[:v2], df[:v3]));
@time a = FastGroupBy.fastby(sum, df[:id6], (df[:v1], df[:v2], df[:v3]));


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


    byvec = df[:id4]
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



using Distributions, Optim

# hard coded data\observations
odr=[0.10,0.20,0.15,0.22,0.15,0.10,0.08,0.09,0.12]
Q_t = quantile.(Normal(0,1), odr)

# return a function that accepts `[mu, sigma]` as parameter
function neglik_tn(Q_t)
    maxx = maximum(Q_t)
    f(μσ) = -sum(logpdf.(Truncated(Normal(μσ[1],μσ[2]), -Inf, maxx), Q_t))
    f
end

neglikfn = neglik_tn(Q_t)

# optimize!
# start searching
@time res = optimize(neglikfn, [mean(Q_t), std(Q_t)]) # 17 seconds
@time res = optimize(neglikfn, [mean(Q_t), std(Q_t)]) # 0.000137 seconds

# use `fieldnames(res)` to see the list of field names that can be referenced via . (dot)
# the \mu and \sigma estimates
res.minimizer # [-1.0733250637041452,0.2537450497038758]# 0.00000 seconds

neglikfn = neglik_tn(Q_t.*2)


Results of Optimization Algorithm
 * Algorithm: Nelder-Mead
 * Starting Point: [-1.1300664159893685,0.22269345618402703]
 * Minimizer: [-1.0733250637041452,0.2537450497038758]
 * Minimum: -1.893080e+00
 * Iterations: 28
 * Convergence: true
   *  √(Σ(yᵢ-ȳ)²)/n < 1.0e-08: true
   * Reached Maximum Number of Iterations: false
 * Objective Calls: 59

using Plots
@manipulate for μ in 0:0.1:1, σ in 0:0.1:1
    x = rand(10)
    plot(x, x.*μ)
end

Results of Optimization Algorithm
 * Algorithm: Nelder-Mead
 * Starting Point: [-1.1300664159893685,0.22269345618402703]
 * Minimizer: [-1.4295521090506047,0.3555910189025838]
 * Minimum: -3.020876e+00
 * Iterations: 30
 * Convergence: true
   *  √(Σ(yᵢ-ȳ)²)/n < 1.0e-08: true
   * Reached Maximum Number of Iterations: false
 * Objective Calls: 63
