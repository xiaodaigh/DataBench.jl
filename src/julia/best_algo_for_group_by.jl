import Base.ht_keyindex
import StatsBase.countmap
const N = Int64(2e9/8);
const K = 100;
srand(1);
@time const id4 = rand(1:K, N);# large groups (int)
@time const v1 =  rand(1:5, N); # int in range [1,5]
#@time const v2 =  rand(1:5, N);# int in range [1,5]
@time const v3 =  rand(round.(rand(100)*100,4), N); # numeric e.g. 23.5749

timings = Dict();
function meany_best{S,T}(id4::Vector{T}, v1::Vector{S})::Dict{T,Real}
  res = Dict{T, Tuple{S, Int64}}()
  szero = zero(S)
  for (id, val) in zip(id4,v1)
    index = ht_keyindex(res, id)
    if index > 0
      @inbounds vw = res.vals[index]
      new_vw = (vw[1] + val, vw[2] + 1)
      @inbounds res.vals[index] = new_vw
    else
      @inbounds res[id] = (val, 1)
    end

  end
  return Dict(k => res[k][1]/res[k][2] for k in keys(res))
end

@time res = meany_best(rand(1:100,2), rand(1:100,2))
timings[:zip_dict] = @elapsed res = meany_best(id4, v1)
timings[:zip_dict1] = @elapsed res = meany_best(id4, v3)



function meany2{S,T}(by::Vector{T}, val::Vector{S})
  res = Dict{T, Tuple{S, Int64}}()
  szero = zero(S)
  Threads.@threads for (i, byi) in enumerate(by)
    index = ht_keyindex(res, byi)
    if index > 0
      @inbounds vw = res.vals[index]
      new_vw = (vw[1] + val[i], vw[2] + 1)
      @inbounds res.vals[index] = new_vw
    else
      @inbounds res[byi] = (val[i], 1)
    end

  end
  return Dict(k => res[k][1]/res[k][2] for k in keys(res))
end

@time meany2(rand(1:100,2), rand(1:100,2));
timings[:enum_dict] = @elapsed meany2(id4, v1)
timings[:enum_dict1] = @elapsed meany2(id4, v3)

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

# if I do not know the numebr before hand
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

timings

using DataBench, IndexedTables
@elapsed DT = createIndexedTable(Int64(2e9/8), 100)
@elapsed res1 = meany_best(column(DT, :id4), column(DT, :v1))
@elapsed res2 = meany_best(column(DT, :id4), column(DT, :v3))
