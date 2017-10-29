# slow as hell
using DataStructures

function sumby_trie{T<:AbstractString,S<:Number}(by::AbstractVector{T}, val::AbstractVector{S})
  a = Trie{S}()
  szero = zero(S)
  for (byi, vali) = zip(by, val)
    a[byi] = get(a, byi, szero) + vali
  end
  return a
end

@time sumby_trie(id3, v1) # 100 seconds
#@time sumby_dict(id6, v1)
