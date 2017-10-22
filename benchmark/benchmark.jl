timings = Dict();
@time res = meanby(rand(1:100,2), rand(1:100,2))

import Base.ht_keyindex
import StatsBase.countmap
const N = Int64(2e9/8);
const K = 100;
srand(1);
@time const id4 = rand(1:K, N);# large groups (int)
@time const v1 =  rand(1:5, N); # int in range [1,5]
timings[:zip_dict] = @elapsed res = meanby(id4, v1)
@time const v3 =  rand(round.(rand(100)*100,4), N);
timings[:zip_dict1] = @elapsed res = meanby(id4, v3)
