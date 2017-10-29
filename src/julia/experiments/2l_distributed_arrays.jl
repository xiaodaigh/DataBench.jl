
using DistributedArrays
@time addprocs()
@everywhere using DistributedArrays

@time pool1 = [@sprintf "id%010d" k for k in 1:(N/K)]
@time id3 = rand(pool1, N)

@time did3 = distribute(id3)
@time hash.(did3)
