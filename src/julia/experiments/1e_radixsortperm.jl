using FastGroupBy,BenchmarkTools

@time id6c = copy(id6)
@time res = sumby(id6c, v1) #17,20
@time sumby_sorted(id6c, v1)
@time sumby_sorted(id6c, v1)
@time sumby_sorted2(id6c, v1)
@time sumby_sorted2(id6c, v1)

@time id6c = copy(id6)
@time res = sumby(id6c, v1) #17,11
@time res1 = sumby_sorted(id6c, v1)
@time res1 = sumby_sorted(id6c, v1)

pool1 = [@sprintf "id%010d" k for k in 1:(N/K)]
id3 = rand(pool1, N)

function randstrarray1(pool, N)
    K = length(pool)
    PooledArray(PooledArrays.RefArray(rand(1:K, N)), pool)
end

@time id3 = rand(pool1, N)

@time a = sumby(hash.(id3), v1);
@time a = sumby(hash.(id3), v1);

@time hid3 = hash.(id3)
@time hid3 = hash.(id3)

@time a = sumby(hid3, v1);

@time a = sumby_dict(id3, v1)
