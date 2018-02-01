using DataFrames, CSV, Missings

dirpath = "d:/data/fannie_mae/"
dirpaths = readdir(dirpath)
filepath = joinpath(dirpath, "Performance_2000Q1.txt")


const types = [
    String,                     Union{String, Missing},     Union{String, Missing},     Union{Float64, Missing},    Union{Float64, Missing}, 
    Union{Float64, Missing},    Union{Float64, Missing},    Union{Float64, Missing},    Union{String, Missing},     Union{String, Missing},
    Union{String, Missing},     Union{String, Missing},     Union{String, Missing},     Union{String, Missing},     Union{String, Missing}, 
    Union{String, Missing},     Union{String, Missing},     Union{Float64, Missing},    Union{Float64, Missing},    Union{Float64, Missing}, 
    Union{Float64, Missing},    Union{Float64, Missing},    Union{Float64, Missing},    Union{Float64, Missing},    Union{Float64, Missing}, 
    Union{Float64, Missing},    Union{Float64, Missing},    Union{Float64, Missing},    Union{String, Missing},     Union{Float64, Missing}, 
    Union{String, Missing}]

@time perf = CSV.read(filepath, delim='|', header = false, types = types, weakrefstrings=false) #45;

@time Feather.write("julia_feather.feather", perf)
using JLD2, FileIO, DataFrames
df = DataFrame(a=[1])

# @time @save "df.jld2" perf # too slow

const ufm = Union{Float64, Missing}
# parse Fannie Mae performance data
function parsefm(T, s)
    if T == ufm
        if s  == ""
            return missing
        end
        return parse(Float64, s)
    end
    String(s)
end

strings_to_parse = ["100007365142|11/01/2001||8|73753.13|22|338|337|01/2030|00000|0|N|||||||||||||||||||",
"100007365142|12/01/2001|JPMORGAN CHASE BANK, NA|8|73694.5|23|337|336|01/2030|00000|0|N|||||||||||||||||||"]

df = DataFrame(a = strings_to_parse)

CSV.write("mwe.txt", df, header = false)

df1 = CSV.read("mwe.txt", header=false)
# 2×1 DataFrames.DataFrame
# │ Row │ Column1                                                                             │
# ├─────┼─────────────────────────────────────────────────────────────────────────────────────┤
# │ 1   │ 100007365142|11/01/2001||8|73753.13|22|338|337|01/2030|00000|0|N||||||||||||||||||| │
# │ 2   │ 100007365142|12/01/2001|JPMORGAN CHASE BANK 

df2 = CSV.read("mwe.txt", header=false, delim = '|')

split.(strings_to_parse, '|')

function convert2tuple(r)
    ([parsefm(T, s) for (T,s) in zip(types, split(r, '|'))]...)
end

@time hehe = convert2tuple.(perfraw[:Column1]);


for i =1:size(perfraw,1)
    println(i)
    convert2tuple(perfraw[:Column1][i])
end

@time abc(perfraw)


    pv = ["LOAN_ID", "Monthly.Rpt.Prd", "Servicer.Name", "LAST_RT", "LAST_UPB", "Loan.Age", "Months.To.Legal.Mat" , "Adj.Month.To.Mat", 
"Maturity.Date", "MSA", "Delq.Status", "MOD_FLAG", "Zero.Bal.Code", "ZB_DTE", "LPI_DTE", "FCC_DTE","DISP_DT", "FCC_COST", "PP_COST", "AR_COST", 
"IE_COST", "TAX_COST", "NS_PROCS", "CE_PROCS", "RMW_PROCS", "O_PROCS", "NON_INT_UPB", "PRIN_FORG_UPB_FHFA", "REPCH_FLAG", "PRIN_FORG_UPB_OTH", 
"TRANSFER_FLAG"]

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

@time perf = rfread(filepath) # 45

@time Feather.read("tmp.feather")

abctp = ([abc(T,s) for (T,s) in zip(types, split(perf[1,1],'|'))]...)

DataFrame(abctp)

function()
    df = abc(perf[1,1])
for r in eachrow(perf)
    print(([abc(T,s) for (T,s) in zip(types, split(r[1],'|'))]...))
    break
end

using DataFrames
const N = 100_000_000; const K = 100
srand(1)
@time df = DataFrame(idstr = rand("id".*dec.(1:N÷K,10), N)
        , id = rand(1:K, N)
        , val = rand(1:5,N))
using CSV
@time CSV.write("df.csv", df);
    



for i in 1:ncol(perf)
    println(pv[i],":", sum(ismissing.(perf[i])))
end

@time perf = CSV.read(filepath, header=false)

@time perf1 = readtable(filepath, separator = '|', header=false) # 221 # 261


using Feather
@time af  = Feather.read("d:/p.feather")

using TextParse
@time iperf = TextParse.csvread(filepath, '|', type_detect_rows = 11857)

using uCSV

uCSV.read(filepath, delim = '|', typedetectrows = 11857)


using FileIO, CSVFiles, DataFrames, FeatherFiles
df = DataFrame(load(File(format"CSV", filepath), '|'))


using Feather

using RCall
R"""
library(feather)
system.time(read_feather("d:/p.feather"))
"""

df = Feather.read("d:/p.feather")

R"""
memory.limit(2^31-1)
library(data.table)
library(feather)
list(
system.time(a <- fread($filepath)),
system.time(write_feather(a, "d:/p.feather")))
"""