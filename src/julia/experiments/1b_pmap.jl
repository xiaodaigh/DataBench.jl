@time using FastGroupBy
@time sumby(id6,v1)
@time addprocs()

@everywhere using FastGroupBy
@everywhere using SplitApplyCombine
@time psumby(id6,v1)


@time as = SharedArray(id6);
@time bs = SharedArray(v1);

@time psumby(as,bs)

@time pgroupreduce(x -> x[1], x->x[2], (x,y)-> x+y, dict_add_reduce, as, bs); #29
@time pgroupreduce(x -> x[1], x->x[2], (x,y)-> x+y, dict_add_reduce, as, bs)
@time psumby(id6, v1); #26

@time res = pgroupreduce(x -> x[1], x->(x[2],1), (x,y)-> (x[1]+y[1], x[2]+y[2]), dict_mean_reduce, as, bs); #29


@time countmap(id6,weights(v1))
@time sumby(id6,v1)
@time groupreduce(x->x[1], x->x[2], (x,y)->x+y, zip(id6,v1))
