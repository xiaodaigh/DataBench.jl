# create an index for id6
import Base.ht_keyindex
import Base.ht_keyindex2

function buildindex_iter{T}(val::Vector{T})
  index = zeros(Int64, length(val))

  for (i,v) in enumerate(val)
    dindex = ht_keyindex2(index, v)

  end
  return index
end

function sumby_index{T,S}(index::Dict{T, Vector{Int64}}, val::Vector{S})
  return Dict{T,S}(k => sum(val[index[k]]) for k in keys(index))
end

@time iid4_iter = buildindex_iter(id4); #9
@time sumby_index(iid4_iter, v1); #8
@time sumby_index(iid4_iter, v1);  #8

@time iid6_iter = buildindex_iter(id6);  #259
@time sumby_index(iid6, v1); # 30.93
@time sumby_index(iid6, v3); #26.7
