using DataFrames, CSV, Missings

dirpath = "d:/data/fannie_mae/"
dirpaths = readdir(dirpath)
filepath = joinpath(dirpath, "Performance_2016Q3.txt")

const types = [
    String,                     Union{String, Missing},     Union{String, Missing},     Union{Float64, Missing},    Union{Float64, Missing}, 
    Union{Float64, Missing},    Union{Float64, Missing},    Union{Float64, Missing},    Union{String, Missing},     Union{String, Missing},
    Union{String, Missing},     Union{String, Missing},     Union{String, Missing},     Union{String, Missing},     Union{String, Missing}, 
    Union{String, Missing},     Union{String, Missing},     Union{Float64, Missing},    Union{Float64, Missing},    Union{Float64, Missing}, 
    Union{Float64, Missing},    Union{Float64, Missing},    Union{Float64, Missing},    Union{Float64, Missing},    Union{Float64, Missing}, 
    Union{Float64, Missing},    Union{Float64, Missing},    Union{Float64, Missing},    Union{String, Missing},     Union{Float64, Missing}, 
    Union{String, Missing}]

@time perf = CSV.read(filepath, delim='|', header = false, types = types) #45;



using RCall
using Feather
function rfread(path)
    R"""
    feather::write_feather(data.table::fread($path), 'tmp.feather')
    gc()
    """
    Feather.read("tmp.feather")
end

@time perf = rfread(filepath) # 50 seconds

@time Feather.read("tmp.feather") # 9 seconds only

using FileIO, CSVFiles

df = DataFrame(load(File(format"CSV", "d:/data/fannie_mae/Performance_2000Q1.txt"), "|"))

using JuliaDB
loadtable("d:/data/fannie_mae/Performance_2000Q1.txt",
     delim='|',
     nastrings=vcat(TextParse.NA_STRINGS, "X"),
     header_exists=false);