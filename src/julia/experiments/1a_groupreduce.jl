using SplitApplyCombine

@time groupreduce(x->x[1],x->x[2],(x,y) -> x+y, zip(id6,v1)); #51 seconds
@time sumby(id6,v1); # 48 seconds
