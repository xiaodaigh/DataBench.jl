function sumby_oracle_id6{T, S}(by::Vector{T}, val::Vector{S})
  szero = zero(S)
  res = [szero for i in 1:Int64(round(N/K))]

  for (byi,vali) in zip(by, val)
    @inbounds res[byi] += vali
  end
  return res
end
@elapsed sumby_oracle_id6(id6, v1) # 7.06
