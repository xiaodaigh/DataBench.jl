#also too slow
using CategoricalArrays
import CategoricalArrays.CategoricalVector

function sumby_cate(by, val)
 byc = CategoricalVector(by)
 sumby(byc, val)
end

@time id6c = CategoricalVector(id6)
@which sumby(id6c, v1)
@time sumby_cate(id6, v1);
@time sumby(id6, v1);
