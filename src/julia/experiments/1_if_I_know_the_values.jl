import Base.ht_keyindex
import StatsBase.countmap

# knowing the exact unique values of id4
function meany3{T, S}(id4::Vector{T}, v1::Vector{S})
  szero = zero(S)
  res = [szero for i in 1:100]
  wt = [0 for i in 1:100]
  for (i,id) in enumerate(id4)
    @inbounds res[id] = res[id]+v1[i]
    @inbounds wt[id] = wt[id] + 1
  end
  return res ./ wt
end

@time res = meany3(rand(1:100,100), rand(1:100,100));
timings[:oracle_array] = @elapsed res = meany3(id4, v1)
timings[:oracle_array1] = @elapsed res = meany3(id4, v3)

# if I do not know the numebr before hand but I know it's a small integer
function meany4{T,S}(id4::Vector{T}, v1::Vector{S})
  wt = countmap(id4)
  szero = zero(S)
  res = [szero for i in 1:max(keys(wt)...)]
  length(id4) == length(v1) || throw(DimensionMismatch())
  for (i,id) in enumerate(id4)
    @inbounds vi = v1[i]
    res[id] += vi
  end
  return Dict(k => res[k]/wt[k] for k in keys(wt))
end

@time meany4(rand(1:100,100), rand(1:100,100));
timings[:count_then_array] = @elapsed meany4(id4, v1)
timings[:count_then_array1] = @elapsed meany4(id4, v1)
