using CSV

@time a = CSV.read(
           "C:/data/Performance_All/Performance_2010Q3.txt",
               delim = '|',
           header = false,
           typemap = Dict(
               Int => Int32,
               Float64 => Float32,
               Union{Missing, Int} => Union{Missing, Int32},
               Union{Missing, Float64} => Union{Missing, Float32}
               ),
           copycols=true
       );
