
using Revise
using FastGroupBy, BenchmarkTools

#const N = Int64(2e9/8)
#const N = 100_000_000
#const N = UInt32(2^31)
const N = UInt32(2^30)

#const N = Int(2^31-1)
const K = UInt32(100)
#const id4 = rand(1:K, N)
const id6 = rand(Int32(1):Int32(round(N/K)), N)
const v1 =  rand(Int32(1):Int32(5), N)
#const v1 = similar(id6)
#const v3 =  rand(round.(rand(100)*100,4), N)

#@time sumby(id6, v1)

# 526 seconds for 2billion
#using SortingAlgorithms
#@time sumby(id6, v1)


# @belapsed sumby(id4,v1) # 4.8
# @belapsed sumby(id4,v3) #4.7
#@time psumby(id4,v1); # 7.8
#@time psumby(id4,v3); #7.7

#
# @belapsed sumby(id6,v1) #64.5
# @belapsed sumby(id6,v3) #68.3
#@belapsed psumby(id6,v1) #28.3
#@belapsed psumby(id6,v3) #37.2
