using FastGroupBy, BenchmarkTools

const N = Int64(2e9/8)
#const N = 100_000_000

#const N = Int(2^31-1)
const K = 100
id4 = rand(1:K, N)
id6 = rand(1:Int64(round(N/K)), N)
v1 =  rand(1:5, N)
v3 =  rand(round.(rand(100)*100,4), N)



# @belapsed sumby(id4,v1) # 4.8
# @belapsed sumby(id4,v3) #4.7
#@time psumby(id4,v1); # 7.8
#@time psumby(id4,v3); #7.7

#
# @belapsed sumby(id6,v1) #64.5
# @belapsed sumby(id6,v3) #68.3
#@belapsed psumby(id6,v1) #28.3
#@belapsed psumby(id6,v3) #37.2
