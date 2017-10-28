# it's too risky
using Iterators
import Iterators.filter

# for number of groups << 2^16
function sumby_fewgroups{T,S}(by::AbstractArray{T,1}, val::AbstractArray{S,1}; powerreduction = 48, seed = UInt(0))
  by_index = (hash.(by, seed) .>>> powerreduction) .+ UInt(1)
  res = zeros(S, Int64(2)^(64-powerreduction))
  # resize the Dict to a larger size
  for (byi, vali) in zip(by_index, val)
      @inbounds res[byi] += vali
  end
  collect(Iterators.filter(x->x[2] != 0, zip(by, res)))
end

@time sumby_fewgroups(id4, v1) # 3.4



@time sumby_fewgroups(id4, v1, seed = UInt(3)) # 2.7
@time sumby_fewgroups(id4, v1) # 3.4
@time sumby_fewgroups(id4, v1) # 2.7
using BenchmarkTools
@benchmark sumby_fewgroups(id4, v1) # 2.7


# complete failure for id6
@time sumby_fewgroups(id6, v1, 32)
@time sumby_fewgroups(id6, v1, 37, UInt(1))

using StatsBase
@time countmap(id4, weights(v1))
@time countmap(id6, weights(v1))
@time sumby(id6, v1)
addprocs()
@everywhere using FastGroupBy
@time psumby(id6, v1)

unique(sample(id6, 1_000_000, replace=false))
unique(sample(id6, 2_000_000, replace=false))

unique(rand(id6,1_000_000))
unique(rand(id6,2_000_000))
unique(rand(id6,3_000_000))
unique(rand(id6,4_000_000))
unique(rand(id6,5_000_000))
unique(rand(id6,6_000_000))

unique(rand(id4,1_000_000))
unique(rand(id4,2_000_000))
unique(rand(id4,3_000_000))
unique(rand(id4,4_000_000))
unique(rand(id4,5_000_000))
unique(rand(id4,6_000_000))
