function meanby{S,T}(id4::Vector{T}, v1::Vector{S})::Dict{T,Real}
  l = length(id4)
  l == length(v1) || throw(DimensionMismatch())
  res = Dict{T, Tuple{S, Int64}}()
  szero = zero(S)

  tmp_val = v1[1]
  tmp_count = 1

  for i in 2:l
    last_id = id4[i-1]
    id, val = id4[i], v1[i]
    if id == last_id
      tmp_val += v1[i]
      tmp_count += 1
    else
      index = ht_keyindex(res, last_id)
      if index > 0
        @inbounds vw = res.vals[index]
        new_vw = (vw[1] + tmp_val, vw[2] + tmp_count)
        @inbounds res.vals[index] = new_vw
      else
        @inbounds res[last_id] = (tmp_val, tmp_count)
      end
      @inbounds tmp_val = v1[i]
      tmp_count = 1
    end
  end

  id = id4[l]
  index = ht_keyindex(res, id)
  if index > 0
    @inbounds vw = res.vals[index]
    new_vw = (vw[1] + tmp_val, vw[2] + 1)
    @inbounds res.vals[index] = new_vw
  else
    @inbounds res[id] = (v1[l], 1)
  end

  return Dict(k => res[k][1]/res[k][2] for k in keys(res))
end
