import Base.ht_keyindex

# was thinking maybe if i dont try to hash everytime then it will be faster
# but it's now

function sumby_contig{S,T}(by::Vector{T}, val::Vector{S})
  l = length(by)
  l == length(val) || throw(DimensionMismatch())
  res = Dict{T, S}()

  @inbounds tmp_val = val[1]
  @inbounds last_id = by[1]

  for i in 2:l
    @inbounds id = by[i]
    if id == last_id # keep adding if the value is the same
      @inbounds tmp_val += val[i]
    else
      index = ht_keyindex(res, last_id)
      if index > 0
        @inbounds res.vals[index] += tmp_val
      else
        @inbounds res[last_id] = tmp_val
      end
      @inbounds tmp_val = val[i]
      last_id = id
    end
  end

  @inbounds id = by[l]
  index = ht_keyindex(res, id)
  if index > 0
    @inbounds res.vals[index] += tmp_val
  else
    @inbounds res[id] = val[l]
  end

  return Dict(k => res[k] for k in keys(res))
end

two_groups = rand(1:2,N)
@time sumby_contig(two_groups, v1); # 36 seconds so only marginally better
@time sumby_contig(two_groups, v1) #
@time sumby(two_groups,v1)
@time sumby_contig(id6, v1);
