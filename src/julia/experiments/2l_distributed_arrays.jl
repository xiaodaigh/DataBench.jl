# ZJ: currenlty there is no support for distributed array and given the speed below it's not ready
using DistributedArrays
@time addprocs()
@everywhere using DistributedArrays
@everywhere using FastGroupBy

@time did6 = distribute(id6)
@time dv1 = distribute(v1)

function sumby_da(by, val)
  T = eltype(by)
  S = eltype(val)
  res = [@spawnat k sumby(localpart(by), localpart(val)) for k = 2:nprocs()]
  # algorithms to collate all dicts
  fnl_res = fetch(res[1])
  szero = zero(S)
  for i = 2:length(res)
    next_res = fetch(res[i])
    for k = keys(next_res)
      fnl_res[k] = get(fnl_res, k, szero) + next_res[k]
    end
  end
  fnl_res
end

@time sumby_da(did6, dv1)

@time pool1 = [@sprintf "id%010d" k for k in 1:(N/K)]
@time id3 = rand(pool1, N)

@time did3 = distribute(id3)

@time res = [@spawnat k sumby(hash.(localpart(did3)), localpart(dv1)) for k = 2:nprocs()]
@time res = [@spawnat k sumby_dict(localpart(did3), localpart(dv1)) for k = 2:nprocs()]
@time fetch.(res)

using CategoricalArrays
import CategoricalArrays.CategoricalArray
import CategoricalArrays.CategoricalPool
@time a = CategoricalArray(id3, ordered=false)

@time a = CategoricalArray(rand(UInt32(1):UInt32(N/K),N), CategoricalPool(pool1,false))
