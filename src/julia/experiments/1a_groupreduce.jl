using SplitApplyCombine

function gr(id6,v1)
  groupreduce(x->x[1],x->x[2],(x,y) -> x+y, zip(id6,v1))
end

@time gr(id6,v1) #47
@time gr(id6,v1) #47

@time groupreduce(x->x[1],x->x[2],(x,y) -> x+y, zip(id6,v1))


@time sumby(id6,v1);
@time sumby(id6,v1);
