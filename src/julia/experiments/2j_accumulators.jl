using DataStructures

function sumby_accumulator{T,S}(by::AbstractVector{T}, val::AbstractVector{S})
  a = Accumulator(T,S)
  for (byi, vali) = zip(by, val)
    push!(a, byi, vali)
  end
  return a
end

@time sumby_accumulator(id6, v1) # 100 seconds
#@time sumby_dict(id6, v1)
