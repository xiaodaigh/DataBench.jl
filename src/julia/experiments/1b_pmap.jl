#addprocs()
using FastGroupBy
@everywhere using FastGroupBy
@everywhere using SplitApplyCombine
@time as = SharedArray(id6);
@time bs = SharedArray(v1);

@time pgroupreduce(x -> x[1], x->x[2], (x,y)-> x+y, dict_add_reduce, as, bs); #29
@time psumby(id6, v1); #26


@time res = pgroupreduce(x -> x[1], x->(x[2],1), (x,y)-> (x[1]+y[1], x[2]+y[2]), vcat, as, bs); #29



@time res = pgroupreduce(x -> x[1], x->(x[2],1), (x,y)-> (x[1]+y[1], x[2]+y[2]), dict_mean_reduce, as, bs); #29
