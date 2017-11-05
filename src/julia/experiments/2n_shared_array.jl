# ZJ: currenlty there is no support for distributed array and given the speed below it's not ready
using StatsBase

@time addprocs()
@everywhere using FastGroupBy

@time did6 = SharedArray(id6)
@time dv1 = SharedArray(v1)

function sumby_sa(by, val)
  T = eltype(by)
  S = eltype(val)

  l = length(by)
  a = sort(collect(Set([1:Int(round((l/nworkers()))):l...,l])))

  if length(a) > nworkers() + 1
    a_mid = a[2:(end-1)]
    a = vcat(1, sort(setdiff(a_mid, sample(a_mid, length(a) - nwokers() - 1, repalce = false))), l)
  end
  b = a[2:end]

  res = [@spawnat k sumby(by[a[k]:b[k]],val[a[k]:b[k]]) for k = 1:nworkers()]

  @time res1 = fetch.(res)
  return res1

  # algorithms to collate all dicts
  fnl_res = fetch(res[1])
  szero = zero(S)
  @time for i = 2:length(res)
    next_res = fetch(res[i])
    for k = keys(next_res)
      fnl_res[k] = get(fnl_res, k, szero) + next_res[k]
    end
  end
  fnl_res
end

@time sumby_sa(did6,dv1)
