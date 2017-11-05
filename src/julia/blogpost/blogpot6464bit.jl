
# only need to be run once to install packages
#Pkg.clone("https://github.com/JuliaData/SplitApplyCombine.jl.git")
#Pkg.clone("https://github.com/xiaodaigh/FastGroupBy.jl.git")

using FastGroupBy, PooledArrays, Compat, BenchmarkTools

const N = Int(2e9/8)
const K = UInt(100)

const id3 = rand(Int32(1):Int32(round(N/K)), N)
const id6 = rand(Int64(1):Int64(round(N/K)), N)
const v1 =  rand(Int32(1):Int32(5), N)


@belapsed sumby(id6[1:2],v1[1:2])
@btime sumby(id6,v1)

# generate string ids
function randstrarray1(pool, N)
    K = length(pool)
    PooledArray(PooledArrays.RefArray(rand(1:K, N)), pool)
end

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
