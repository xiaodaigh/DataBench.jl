using SortingAlgorithms
using RCall, PyCall, DataFrames, BenchmarkTools, IterTools, CSV
using Plots
gr()

function string_sort_timing_cmp(N, K, idnumlen = 10)
    srand(1);
    svec1 = rand(["id"*dec(i,idnumlen) for i in 1:N÷K], N);
    julia_timing = @elapsed sort(svec1, alg = StringRadixSort); 
    println(julia_timing)

    julia_timing_base = @elapsed sort(svec1); 
    println(julia_timing_base)

    R"""
    # increase memory limit; only works on Windows
    if (Sys.info()["sysname"] == "Windows") memory.limit(32653)
    id3 = sample(sprintf("id%010d",1:($N/$K)), $N, TRUE) # small groups (char)
    pt = proc.time()
    sort(id3, method = "radix")
    r_timing = proc.time() - pt
    rm(id3)
    gc()
    """
    @rget r_timing

    println(r_timing[3])

    py"""
    import numpy as np
    import timeit

    # randChar is workaround for MemoryError in mtrand.RandomState.choice
    # http://stackoverflow.com/questions/25627161/how-to-solve-memory-error-in-mtrand-randomstate-choice
    def randChar(f, numGrp, N) :
        things = [f%x for x in range(numGrp)]
        return [things[x] for x in np.random.choice(numGrp, N)]

    id3 = randChar("id%010d", $N//$K, $N)   # small groups (char)
    tt = timeit.Timer("id3.sort()" ,"from __main__ import id3").timeit(1) # 6.8 seconds
    """

    python_timing = py"tt"

    DataFrame(  lang = ["Julia", "Julia", "R", "Python"], 
                alg  = ["radix", "base", "radix", "base"],
                timings = [julia_timing, julia_timing_base, r_timing[3], python_timing], 
                N = repeat([N], inner = 4),
                K = repeat([K], inner = 4),
                idnumlen = repeat([idnumlen], inner = 4),
                grps=[N÷K for i=1:4])
end

# sorting a vector with no duplicates
res10m = string_sort_timing_cmp(10_000_000, 1, 10);
CSV.write("res10m.csv", res10m);
res10m = CSV.read("res10m.csv");

gb = bar(res10m[:lang].*"-".*res10m[:alg], res10m[:timings], label="Seconds")
title!(gb,"String sort performance: 10m unique id strings")
ylabel!(gb, "Seconds")
savefig(gb, "sort_perf_10m_u.png")

# sorting a vector with lots of duplicates
res10m100 = string_sort_timing_cmp(10_000_000, 100, 10);
CSV.write("res10m100.csv", res10m100);

res10m100 = CSV.read("res10m100.csv");
gb = bar(res10m100[:lang].*"-".*res10m100[:alg], res10m100[:timings], label="Seconds")
title!(gb,"String sort performance: 10m strings 100k unique values")
ylabel!(gb, "Seconds")
savefig(gb, "sort_perf_10m100_u.png")



string_sort_timing_cmp(1_000_000, 100, 3)

res = [string_sort_timing_cmp(n,k,idnumlen) for (n,k,idnumlen) = product(10.^(6:7),[1,100],[3,10])]
res = reduce(vcat,res)



sort!(res,cols = [:K,:N,:idnumlen,:timings])

res1m = string_sort_timing_cmp(1_000_000, 100,  3)
res10m = string_sort_timing_cmp(10_000_000, 100, 3)
res100m = string_sort_timing_cmp(100_000_000, 100, 3)
# res200m = string_sort_timing_cmp(200_000_000, 100, 3)
# res400m = string_sort_timing_cmp(400_000_000, 100, 3)
res = reduce(vcat,[res1m, res10m, res100m, res200m, res400m])

res1m = string_sort_timing_cmp(1_000_000, 100, 3)
res10m = string_sort_timing_cmp(10_000_000, 100,3)
res100m = string_sort_timing_cmp(100_000_000, 100,3)
# res200m = string_sort_timing_cmp(200_000_000, 100,3)
# res400m = string_sort_timing_cmp(400_000_000, 100,3)
res = reduce(vcat,[res1m, res10m, res100m, res200m, res400m])

res1m10 = string_sort_timing_cmp(1_000_000, 100)
res10m10 = string_sort_timing_cmp(10_000_000, 100)
res100m10 = string_sort_timing_cmp(100_000_000, 100)
# res200m10 = string_sort_timing_cmp(200_000_000, 100)
# res400m10 = string_sort_timing_cmp(400_000_000, 100)
res = reduce(vcat,[res1m, res10m, res100m, res200m, res400m])

N = 10_000_000
K = 100_000
x = rand(["id"*dec(i,3) for i in 1:N÷K], N);

srand(1);
N = 10_000_000
K = 1
svec1 = rand(["id"*dec(i,10) for i in 1:N÷K], N);
julia_timing = @elapsed sort(svec1, alg = StringRadixSort); 
println(julia_timing)