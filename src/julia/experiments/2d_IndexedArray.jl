# create an index for id6
# building an index is too time consuming
import Base.ht_keyindex
import Base.ht_keyindex2
using SortingAlgorithms

function buildindex{T}(val::Vector{T})
  index = Dict{T,Vector{Int64}}()
  for (i,v) in enumerate(val)
    dindex = ht_keyindex(index, v)
    if dindex > 0
      push!(index.vals[dindex], i)
    else
      index[v] = [i]
    end
  end
  return index
end

function buildindex2{T}(val::Vector{T})
  index = Dict{T,Vector{Int64}}()
  for (i,v) in enumerate(val)
    dindex = ht_keyindex2(index, v)
    if dindex > 0
      push!(index.vals[dindex], i)
    else
      Base._setindex!(index, [i], v, -dindex)
    end
  end
  return index
end

function sumby_index{T,S}(index::Dict{T, Vector{Int64}}, val::Vector{S})
  return Dict{T,S}(kv[1] => sum(val[kv[2]]) for kv in index)
end

@time iid4 = buildindex(id4); #9.96
@time sumby_index(iid4, v1); #8.88
@time sumby_index(iid4, v1); #8.63

@time iid6 = buildindex(id6);  #259,290
@time sumby_index(iid6, v1); # 30.93
@time sumby_index(iid6, v3); #26.7

@time iid42 = buildindex2(id4); #10.94
