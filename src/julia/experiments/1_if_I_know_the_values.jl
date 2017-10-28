import IterTools.filter

function sumby_intrange{T<:Integer, S}(by::Vector{T}, val::Vector{S}, bylow::T, byhigh::T)
  lby = byhigh - bylow + 1
  res = zeros(T, lby)

  for (byi,vali) in zip(by, val)
    @inbounds res[(byi-bylow) + 1] += vali
  end
  IterTools.filter(x -> x[2] != 0, zip(bylow:byhigh,res))
end
@time res = sumby_intrange(id6, v1, 1, 2_500_000); #7 seconds
