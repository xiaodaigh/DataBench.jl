# Map a bits-type to an unsigned int, maintaining sort order
using SortingAlgorithms
using  Base.Order, Compat
uint_mapping(::ForwardOrdering, x::Unsigned) = x
for (signedty, unsignedty) in ((Int8, UInt8), (Int16, UInt16), (Int32, UInt32), (Int64, UInt64), (Int128, UInt128))
    # In Julia 0.4 we can just use unsigned() here
    @eval uint_mapping(::ForwardOrdering, x::$signedty) = reinterpret($unsignedty, xor(x, typemin(typeof(x))))
end
uint_mapping(::ForwardOrdering, x::Float32)  = (y = reinterpret(Int32, x); reinterpret(UInt32, ifelse(y < 0, ~y, xor(y, typemin(Int32)))))
uint_mapping(::ForwardOrdering, x::Float64)  = (y = reinterpret(Int64, x); reinterpret(UInt64, ifelse(y < 0, ~y, xor(y, typemin(Int64)))))

uint_mapping{Fwd}(rev::ReverseOrdering{Fwd}, x) = ~uint_mapping(rev.fwd, x)
uint_mapping{T<:Real}(::ReverseOrdering{ForwardOrdering}, x::T) = ~uint_mapping(Forward, x) # maybe unnecessary; needs benchmark

uint_mapping(o::By,   x     ) = uint_mapping(Forward, o.by(x))
uint_mapping(o::Perm, i::Int) = uint_mapping(o.order, o.data[i])
uint_mapping(o::Lt,   x     ) = error("uint_mapping does not work with general Lt Orderings")

const RADIX_SIZE = 11
const RADIX_MASK = 0x7FF



# vs = rand(Int8, 10)
# ts = similar(vs)
# ts = zeros(eltype(vs), length(vs))
# us = vs
# us1 = zeros(eltype(us), length(us))#
#
# import DataFrames.DataFrame

function sort_piracy{T,S}(vs::AbstractVector{T},  us::AbstractVector{S})
  ts=similar(vs)
  us1=similar(us)
  o = Forward
  lo = 1
  hi = length(vs)

  if lo >= hi;  return vs;  end

  # Make sure we're sorting a bits type
  TT = Base.Order.ordtype(o, vs)
  if !isbits(TT)
      error("Radix sort only sorts bits types (got $TT)")
  end

  # Init
  iters = ceil(Integer, sizeof(T)*8/RADIX_SIZE)
  bin = zeros(UInt32, 2^RADIX_SIZE, iters)
  if lo > 1;  bin[1,:] = lo-1;  end

  # Histogram for each element, radix
  for i = lo:hi
      v = uint_mapping(o, vs[i])
      for j = 1:iters
          idx = @compat(Int((v >> (j-1)*RADIX_SIZE) & RADIX_MASK)) + 1
          @inbounds bin[idx,j] += 1
      end
  end

  # Sort!
  swaps = 0
  len = hi-lo+1
  for j = 1:iters
  # Unroll first data iteration, check for degenerate case
    v = uint_mapping(o, vs[hi])
    idx = @compat(Int((v >> (j-1)*RADIX_SIZE) & RADIX_MASK)) + 1

    # are all values the same at this radix?
    if bin[idx,j] == len;  continue;  end

    cbin = cumsum(bin[:,j])
    ci = cbin[idx]
    ts[ci] = vs[hi]
    us1[ci] = us[hi]

    cbin[idx] -= 1

    # Finish the loop...
    @inbounds for i in hi-1:-1:lo
        v = uint_mapping(o, vs[i])
        idx = @compat(Int((v >> (j-1)*RADIX_SIZE) & RADIX_MASK)) + 1
        ci = cbin[idx]
        ts[ci] = vs[i]
        us1[ci] = us[i]
        cbin[idx] -= 1
    end
    vs,ts = ts,vs
    us,us1 = us1,us
    swaps += 1
  end

  @inbounds if isodd(swaps)
    vs,ts = ts,vs
    us,us1 = us1,us
    for i = lo:hi
        vs[i] = ts[i]
        us[i] = us1[i]
    end
  end

  sumby_sorted(vs, us)
end

@time a = sort_piracy(id6, v1);

@time sumby(id6,v1) #20

pool1 = [@sprintf "id%010d" k for k in 1:(N/K)]
function randstrarray1(pool, N)
    K = length(pool)
    PooledArray(PooledArrays.RefArray(rand(1:K, N)), pool)
end
using
const id3 = pool1[rand(1:2_500_000,N)]


@time a = sort_piracy(hash.(id3), v1);
@time a = sumby(id3, v1);

@time hid3 = hash.(id3)
@time hid3 = hash.(id3)
@time a = sort_piracy(hid3, v1);

addprocs()
using DistributedArrays
did3 = distribute(id3)
@time hdid3 = hash.(did3)

methods(hdid3)

@time a = sort_piracy(hdid3, v1);
