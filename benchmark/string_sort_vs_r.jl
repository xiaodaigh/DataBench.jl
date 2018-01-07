using SortingAlgorithms
using RCall, PyCall, DataFrames, BenchmarkTools

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

    DataFrame(  lang = ["julia", "julia", "r", "python"], 
                timings = [julia_timing, julia_timing_base, r_timing[3], python_timing], 
                N = [N for i=1:4], 
                grps=[N÷K for i=1:4])
end

# warmup
string_sort_timing_cmp(1_000_000, 100, 3)

res1m = string_sort_timing_cmp(1_000_000, 100, 3)
res10m = string_sort_timing_cmp(10_000_000, 100,3)
res100m = string_sort_timing_cmp(100_000_000, 100,3)
res200m = string_sort_timing_cmp(200_000_000, 100,3)
res400m = string_sort_timing_cmp(400_000_000, 100,3)
res = reduce(vcat,[res1m, res10m, res100m, res200m, res400m])

res1m10 = string_sort_timing_cmp(1_000_000, 100)
res10m10 = string_sort_timing_cmp(10_000_000, 100)
res100m10 = string_sort_timing_cmp(100_000_000, 100)
res200m10 = string_sort_timing_cmp(200_000_000, 100)
res400m10 = string_sort_timing_cmp(400_000_000, 100)
res = reduce(vcat,[res1m, res10m, res100m, res200m, res400m])

N = 10_000_000
K = 100_000
x = rand(["id"*dec(i,3) for i in 1:N÷K], N);

using FastGroupBy

@time FastGroupBy.sort(x, alg = StringRadixSort);
@time SortingAlgorithms.sort(x, alg = StringRadixSort);
@time FastGroupBy.radixsort!(x);


srand(1);
N = 10_000_000
K = 1
svec1 = rand(["id"*dec(i,10) for i in 1:N÷K], N);
julia_timing = @elapsed sort(svec1, alg = StringRadixSort); 
println(julia_timing)

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
r_timing[3]