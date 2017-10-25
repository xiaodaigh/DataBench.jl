#addprocs()

# too slow

function sumby_pmap{T,S}(by::Vector{T}, val::Vector{S})
  if nprocs() == 1
    throw(ErrorException("only one proc"))
  end
  l = length(by)
  bys = SharedArray{T}(by)
  vals = SharedArray{S}(val)
  sss = Int64(round(l/nprocs()))
  index = sort(collect(Set([0:sss:l...,l])))

  index2 = [(index[i-1]+1):index[i] for i in 2:length(index)]

  res = pmap(index2) do ii
    sumby(bys[ii], vals[ii])
  end
  return res
end

@everywhere using FastGroupBy
@time sumby_pmap(id6, v1) #36
@time sumby_pmap(id6, v1) #32
