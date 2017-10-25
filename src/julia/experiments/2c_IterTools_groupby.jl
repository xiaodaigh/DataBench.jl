using IterTools
const gb = IterTools.groupby

function sumby_itgb{T,S}(by::AbstractArray{T}, v1::AbstractArray{S})
  res = Dict{T, S}()
  szero = zero(S)
  for i in IterTools.groupby(x -> x[1], zip(by,v1))
    byi = i[1][1]
    vali = mapreduce(x->x[2],+,i)
    index = ht_keyindex(res, byi)
    if index > 0
      @inbounds vw = res.vals[index]
      new_vw = vw + vali
      @inbounds res.vals[index] = new_vw
    else
      @inbounds res[byi] = vali
    end

  end
  return res
end

@time sumby_itgb(id4, v1)
@time sumby_itgb(id4, v1)
