import Base.ht_keyindex
function msumby{T,S}(by::Vector{T}, val::Vector{S})::Array{Dict{T, S}}
  res = Array{Dict{T, S}}(Threads.nthreads())
  for j in 1:Threads.nthreads()
    res[j] = Dict{T, S}()
  end
  Threads.@threads for i = 1:length(by)
    # ti = Threads.threadid()
    # @inbounds id = by[i]
    # @inbounds val = val[i]
    # @inbounds dict =  res[ti]
    # index = ht_keyindex(dict, id)
    # if index > 0
    #   @inbounds vw = dict.vals[index]
    #   new_vw = vw + val
    #   @inbounds dict.vals[index] = new_vw
    # else
    #   @inbounds dict[id] = val
    # end
  end
  return res
end


@time res = msumby(id4, v1);

function msumby_test{T,S}(by::Vector{T}, val::Vector{S})
  res = Array{Dict{T, S}}(Threads.nthreads())
  for j in 1:Threads.nthreads()
    res[j] = Dict{T, S}()
  end
  for i = 1:length(by)
    # ti = Threads.threadid()
    # @inbounds id = by[i]
    # @inbounds val = val[i]
    # @inbounds dict =  res[ti]
    # index = ht_keyindex(dict, id)
    # if index > 0
    #   @inbounds vw = dict.vals[index]
    #   new_vw = vw + val
    #   @inbounds dict.vals[index] = new_vw
    # else
    #   @inbounds dict[id] = val
    # end
  end
  return res
end

@time msumby_test(id4, v1)

function nothread_test(v)
  for i = 1:length(v)
    @inbounds v[i] = 0.5
  end
  sum(v)
end

function thread_test(v)
  Threads.@threads for i = 1:length(v)
      @inbounds v[i] = 0.5
  end
  sum(v)
end


v = zeros(Float64, 250_000_000)
@benchmark nothread_test(v)
v = zeros(Float64, 250_000_000)
@benchmark thread_test(v)
