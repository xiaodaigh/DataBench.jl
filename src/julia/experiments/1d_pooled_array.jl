using PooledArrays
import PooledArrays.PooledArray
pool = [1:Int64(round(N/K))...]

@time id6_pooled = PooledArray(PooledArrays.RefArray(rand(Int32(1):Int32(round(N/K)), N)), pool)
@time sumby(id6_pooled, v1)
