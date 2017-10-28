# rehashing stuff doesn't help
import FastGroupBy.sumby
import Base.ht_keyindex
import Base.rehash!
function sumby{T,S}(by::AbstractArray{T,1}, val::AbstractArray{S,1}, sizehint::Unsigned)
  res = Dict{T, S}()
  rehash!(res, sizehint)
  for (byi, vali) in zip(by, val)
    index = ht_keyindex(res, byi)
    if index > 0
      @inbounds  res.vals[index] += vali
    else
      @inbounds res[byi] = vali
    end
  end
  return res
end

@time sumby(id6, v1, UInt(2_500_000))
