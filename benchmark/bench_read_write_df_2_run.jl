################################################################################
# setup
################################################################################
# using Revise
using RCall, Plots, StatPlots
# using DataBenchs
using uCSV, Feather, BenchmarkTools, CSV
using TextParse, FileIO, JLD, IterableTables
using JuliaDB, IndexedTables, uCSV, JLD2, DataFrames

using BenchmarkTools
using GR
using StatPlots
using FileIO
using Plots
using GR
gr()

outpath = "d:/tmp/"

srand(1);
N = 1_000_000;  K = 100;
# @time df = DataBench.createSynDataFrame(N, K); #31 #40
pool = "id".*dec.(1:K,3);
pool1 = "id".*dec.(1:N÷K,10);
nums = round.(rand(100).*100, 4);

############################ python package test
using PyCall
using Conda
# Conda.add("pandas") # need to run if runs into error
# Conda.add_channel("conda-forge")
# Conda.add("feather-format")
@pyimport pandas as pd
# @pyimport feather

include("benchmark/bench_read_write_df_1_code.jl")

################################################################################
# benchmark write
# ZJ: from my own testing, writing of 1m rows is sufficient to assess the
# relativities
################################################################################
println("starting testing")
if true
    julres = bench_df_write_read(1_000_000, 100, outpath, true )
    rres=rreadwrite(outpath)
    res1m = (julres, rres)
    FileIO.save(outpath*"df_write_bench_1m.jld", "res1m", res1m)

    # show read and write
    data = FileIO.load(outpath*"/df_write_bench_1m.jld")
    res1m = data["res1m"]
    (julres, rres) = res1m
    plot_bench_df_read_write(julres, rres, 1_000_000, true)
    Plots.savefig("benchmark/results/1m.png")


    wtimes = vcat([mean(a.times)/1e9 for a in julres[1]], rres[1:2:5])
    rtimes = vcat([mean(a.times)/1e9 for a in julres[2]], rres[2:2:6])

    df = DataFrame(pkg = ["Feather.jl","CSV.jl","TextParse.jl","JLD.jl","JLD2.jl", "Pandas","fst","data.table","R feather"],
    wtimes = wtimes,
    rtimes = rtimes)

    sort!(df, cols = [:rtimes])

    julres = bench_df_write_read(10_000_000, 100, outpath )
    rres=rreadwrite(outpath)
    res1m = (julres, rres)
    FileIO.save(outpath*"df_write_bench_10m.jld", "res1m", res1m)

    # show read and write
    data = FileIO.load(outpath*"/df_write_bench_10m.jld")
    res1m = data["res1m"]
    (julres, rres) = res1m
    plot_bench_df_read_write(julres, rres, 10_000_000)
    Plots.savefig("benchmark/results/10m.png")

   
    


    julres = bench_df_write_read(100_000_000, 100, outpath )
        rres=rreadwrite(outpath)
        res1m = (julres, rres)
        FileIO.save(outpath*"df_write_bench_100m.jld", "res1m", res1m)

    # show read and write
    data = FileIO.load(outpath*"/df_write_bench_100m.jld")
    res1m = data["res1m"]
    (julres, rres) = res1m
    plot_bench_df_read_write(julres, rres, 100_000_000)

    # #  show writing test
    # data = FileIO.load("e:/df_write_bench_1m_more.jld")
    # res1m = data["res1m"]
    # (julres, rres) = res1m

    # plot_bench_df_read_write(julres, rres, 1_000_000, false)

end

# if true
#     julres = bench_df_write_read(1_000_000, 100, outpath, false )
#     rres=rreadwrite(outpath)
#     res1m = (julres, rres)
#     FileIO.save("d:/tmp/df_write_bench_1m_more.jld", "res1m", res1m)
# end

