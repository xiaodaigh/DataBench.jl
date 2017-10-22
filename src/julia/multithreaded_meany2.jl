function meany_m{S,T}(id4::Vector{T}, v1::Vector{S})
  res = Array{Dict{T, Tuple{S, Int64}}}(Threads.nthreads())
  for j in 1:Threads.nthreads()
    res[j] = Dict{T, Tuple{S, Int64}}()
  end
  szero = zero(S)
  Threads.@threads for i = 1:length(id4)
    @inbounds id = id4[i]
    @inbounds val = v1[i]
    ti = Threads.threadid()
    index = ht_keyindex(res[ti], id)
    if index > 0
      @inbounds vw = res[ti].vals[index]
      new_vw = (vw[1] + val, vw[2] + 1)
      @inbounds res[ti].vals[index] = new_vw
    else
      @inbounds res[ti][id] = (val, 1)
    end
  end
  #return Dict(k => res[k][1]/res[k][2] for k in keys(res))
  return res
end

@time res = meany_bestm(rand(1:100,2), rand(1:100,2))
timings[:zip_dict] = @elapsed res = meany_m(id4, v1)
timings[:zip_dict1] = @elapsed res = meany_m(id4, v3)s
