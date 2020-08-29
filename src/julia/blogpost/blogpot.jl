"""
This section is no longer relevant as indexed table has died
"""

# only need to be run once to install packages
#Pkg.clone("https://github.com/JuliaData/SplitApplyCombine.jl.git")
#Pkg.clone("https://github.com/xiaodaigh/FastGroupBy.jl.git")

using FastGroupBy, PooledArrays, Compat, BenchmarkTools

const N = Int(2e9/8);
const K = UInt(100);

const id3 = rand(Int32(1):Int32(K), N);
const id6 = rand(Int32(1):Int32(round(N/K)), N);
const v1 =  rand(Int32(1):Int32(5), N);

@elapsed sumby(id6[1:2],v1[1:2])

function b()
    id6c = copy(id6)
    v1c = copy(v1)
    @elapsed sumby(id6c,v1c)
end

res = [b() for i = 1:5]
mean(res)

function a()
    id3c = copy(id3)
    v1c = copy(v1)
    @elapsed sumby(id3c,v1c)
end

res = [a() for i = 1:10]
mean(res)

# generate string ids
function randstrarray1(pool, N)
    K = length(pool)
    PooledArray(PooledArrays.RefArray(rand(1:K, N)), pool)
enduisng

pool1 = [@sprintf "id%010d" k for k in 1:(N/K)]
id3 = randstrarray1(pool1, N)

# treat it as Pooledarray
@time sumby(id3, v1)

# treat by as strings and use dictionary method
id3_str = rand(pool1, N)
@time sumby_dict(id3_str, v1)

# parallelized sum
@time addprocs() # create Julia workers
@time using FastGroupBy
@everywhere using FastGroupBy
@everywhere using SplitApplyCombine
@time psumby(id6,v1) # 35 seconds
