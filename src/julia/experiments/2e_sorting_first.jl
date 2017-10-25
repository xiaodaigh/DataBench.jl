
function sumby_sort(id6,v1)
  a = DataFrames.DataFrame(id6=id6,v1=v1)
  sort!(a, cols=:id6; alg=RadixSort)
  a
end

@time sumby_sort(id6,v1) #

@time sumby(id6,v1)
