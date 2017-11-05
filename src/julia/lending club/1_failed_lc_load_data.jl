
# using CSV, DataFrames, RCall, StatsBase
#
# csvpath = "C:/Users/dzj/Downloads/Lending Club/csv/"
# dirs = readdir(csvpath)
#
# files = map(dirs) do dir1
#     res = readdir(csvpath * dir1)
#     joinpath(csvpath, dir1, res[1])
# end
#
#
# function fread(path1)
#     R"library(data.table);a = fread($path1 , na.strings = NULL)"
#     @rget a
#     a
# end
#
# data = mapreduce(vcat, files) do file
#     println(file)
#     fread(file)
# end

# CSV.read("C:/Users/dzj/Downloads/Lending Club/csv/LoanStats3b.csv/LoanStats3b.csv"; datarow = 2)
# path1 = "C:/Users/dzj/Downloads/Lending Club/csv/LoanStats3a.csv/LoanStats3a.csv"
# df = fread("C:/Users/dzj/Downloads/Lending Club/csv/LoanStats3a.csv/LoanStats3a.csv")

# function c()
#     local i = 1
#     colwise(df) do col
#         if typeof(col) == DataArrays.DataArray{String, 1}
#             print(i)
#         end
#         i = i + 1
#     end
# end
#
# c()

# function c1()
#     local i = 1
#     map(df[22]) do df1
#         i
#         if typeof(df1) != String
#             print(df1)
#         end
#     end
# end
