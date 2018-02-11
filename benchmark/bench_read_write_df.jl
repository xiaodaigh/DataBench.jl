################################################################################
# setup
################################################################################
using Revise, RCall, Plots, StatPlots
# using DataBenchs
using uCSV, Feather, BenchmarkTools, CSV, TextParse, FileIO, JLD, IterableTables, JuliaDB, IndexedTables, uCSV, JLD2, DataFrames
outpath = "d:/tmp/"

srand(1);
N = 100;  K = 100;
# @time df = DataBench.createSynDataFrame(N, K); #31 #40
pool = "id".*dec.(1:K,3);
pool1 = "id".*dec.(1:N÷K,10);
nums = round.(rand(100).*100, 4);

function bench_df_write_read(N,K, outpath, exclslow = true)
    df = DataFrame(
        id1 = rand(pool,N),
        id2 = rand(pool,N),
        id3 = rand(pool1,N),
        id4 = rand(1:K,N),
        id5 = rand(1:K,N),
        id6 = rand(1:(N/K),N),
        v1 = rand(1:5,N),
        v2 = rand(1:5,N),
        v3 = rand(nums,N));

    if exclslow
        writeres = [
            @benchmark(Feather.write(outpath*"df.feather", $df)), # 138
            # @benchmark(CSV.write(outpath*"df.csv", $df)), #569.749011
            @benchmark(FileIO.save(outpath*"df_fileio.csv", $df)), # 209.438193 seconds (1.20 G allocations: 47.704 GiB, 5.91% gc time)
            # @benchmark(uCSV.write(outpath*"df_u.csv", $df)), #528.785193 seconds (3.60 G allocations: 157.952 GiB, 8.43% gc time)
            @benchmark(FileIO.save(outpath*"df.jld","df", $df)) #215.839709 seconds (1.16 k allocations: 6.706 GiB, 2.50% gc time)
            # @benchmark(JLD2.@save(outpath*"df.jld2", $df)), #765.809597 seconds (2.70 G allocations: 58.094 GiB, 19.22% gc time)
            #,@benchmark(JuliaDB.save($dfit,outpath*randstring(8)))
            ]
        readres = [
            @benchmark(Feather.read(outpath*"df.feather"))
            ,@benchmark(DataFrame(FileIO.load(outpath*"df_fileio.csv")))
            ,@benchmark(FileIO.load(outpath*"df.jld"))
        ]
        return (writeres, readres)
    else
        dfit = table(df)
        @time return [
            @benchmark(Feather.write(outpath*"df.feather", $df)) , # 138
            @benchmark(CSV.write(outpath*"df.csv", $df)), #569.749011
            @benchmark(FileIO.save(outpath*"df_fileio.csv", $df)), # 209.438193 seconds (1.20 G allocations: 47.704 GiB, 5.91% gc time)
            @benchmark(uCSV.write(outpath*"df_u.csv", $df)), #528.785193 seconds (3.60 G allocations: 157.952 GiB, 8.43% gc time)
            @benchmark(FileIO.save(outpath*"df.jld","df", $df)), #215.839709 seconds (1.16 k allocations: 6.706 GiB, 2.50% gc time)
            @benchmark(JLD2.@save(outpath*"df.jld2", $df)), #765.809597 seconds (2.70 G allocations: 58.094 GiB, 19.22% gc time)
            @benchmark(JuliaDB.save($dfit,outpath*randstring(8)))]
    end
end

function rreadwrite(outpath)
    r = R"""
    memory.limit(2^31-1)
    library(fst)
    library(feather)
    library(data.table)
    df = feather::read_feather(file.path($outpath,"df.feather"))

    pt = proc.time()
    system.time(write_fst(df,file.path($outpath,"df_default.fst")))[3]
    fstt = proc.time() - pt

    #system.time(write_fst(df,file.path($outpath,"df_0.fst"), 0))[3]
    #system.time(write_fst(df,file.path($outpath,"df_100.fst"), 100))[3]

    pt = proc.time()
    system.time(read_fst(file.path($outpath,"df_default.fst")))[3]
    fsttr= proc.time() - pt

    # system.time(read_fst(file.path($outpath,"df_0.fst")))[3],
    # system.time(read_fst(file.path($outpath,"df_100.fst")))[3],

    # multi threaded read write
    pt = proc.time()
    fwrite(df, file.path($outpath, "df_fwrite.csv"))
    fwritet = proc.time() - pt

    pt = proc.time()
    system.time(fread("df_fwrite.csv"))[3]
    dtr = proc.time() - pt

    list(
        fstt[3],
        fsttr[3],
        fwritet[3],
        dtr[3]
    )
    """
    [Float64(r[i]) for i=1:length(r)]
end

function plot_bench_df_read_write(julres, rres, N, exclslow=true)
    if exclslow
        x = ["Feather.jl",          "TextParse.jl\n FileIO.jl",         "JLD.jl\n FileIO.jl"]
        rx = ["R\n fst (default)","R\n data.table"]

        x = vcat(repeat(x, outer=2), repeat(rx, outer=2))
        rw = ["write","read"]

        group = vcat(repeat(rw, inner=3), repeat(rw, inner=2))
        
        julwrite = (x->(x.times ./ 1e9) |> mean).(julres[1])
        julread = (x->(x.times ./ 1e9) |> mean).(julres[2])
        y = vcat(julwrite, julread, rres)

        groupedbar(
        x
        , y
        , group = group
        , bar_position = :dodge
        , title = "DataFrame read/write to disk performance ($(Int(N/1_000_000))m)"
        , ylabel = "seconds")
        savefig("read_write_df_bench $(Int(N/1_000_000))m.png")
    else
        x = ["Feather.jl","CSV.jl", "TextParse.jl\n FileIO.jl","uCSV.jl","JLD.jl\n FileIO.jl","JLD2.jl","IndexedTables.jl", "R\n fst (default)","R\n fwrite"]
        y = [(x->(x.times ./ 1e9) |> mean).(res[1:7])]
        groupedbar(
            x
            , y
            , group = group
            , bar_position = :dodge
            , title = "DataFrame write to disk performance ($(Int(N/1_000_000))m)"
            , ylabel = "seconds")
        savefig("read_write_df_bench $(Int(N/1_000_000))m.png")
    end
end

################################################################################
# benchmark write
# ZJ: from my own testing testing writing of 1m rows is sufficient to assess the
# relativities
################################################################################
julres = bench_df_write_read(1_000_000, 100, outpath)
rres=rreadwrite(outpath)
res1m = (julres, rres)
FileIO.save("df_write_bench_1m.jld", "res1m", res1m)
# a = FileIO.load("df_write_bench_1m.jld")
# res1m = a["res1m"]
plot_bench_df_read_write(julres, rres, 1_000_000)

# res100m = bench_df_write(100_000_000, 100, outpath)
# res100m = vcat(res100m, rwrite(outpath))
# FileIO.save("df_write_bench_100m.jld", "res100m", res100m)
# plot_bench_df_write(res100m, 100_000_000)

# res1000m = bench_df_write(1_000_000_000, 100, outpath)
# res1000m = vcat(res1000m, rwrite(outpath))
# FileIO.save("df_write_bench_1000m.jld", "res1000m", res1000m)
# plot_bench_df_write(res1000m, 1_000_000_000)

# res10m = bench_df_write(10_000_000, 100, outpath)
# res10m = vcat(res10m, rwrite(outpath))
# FileIO.save("df_write_bench_10m.jld", "res10m", res10m)
# plot_bench_df_write(res10m, 10_000_000)

################################################################################
# benchmark reads
################################################################################


################################################################################
# run once writes
################################################################################
@time Feather.write("df.feather",df[1,:]);
@time Feather.write("df.feather",df); # 138
@time CSV.write("df.csv",df[1,:]);
@time CSV.write("df.csv",df); #569.749011
@time save("df_fileio.csv", df[1,:]);
@time save("df_fileio.csv", df); # 209.438193 seconds (1.20 G allocations: 47.704 GiB, 5.91% gc time)
@time uCSV.write("df_u.csv",df[1,:]);
@time uCSV.write("df_u.csv",df); #528.785193 seconds (3.60 G allocations: 157.952 GiB, 8.43% gc time)
@time FileIO.save("df.jld","df",df[1,:]);
@time FileIO.save("df.jld","df",df) #215.839709 seconds (1.16 k allocations: 6.706 GiB, 2.50% gc time)
@time Jl@save "d:/df.jld2" df[1,:]
@time @save "d:/df.jld2" df #765.809597 seconds (2.70 G allocations: 58.094 GiB, 19.22% gc time)

using FstFileFormat
FstFileFormat.write(df,"df_fstfileformat.fst");

using BenchmarkTools

@benchmark Feather.write("df.feather", $df) # 138
@benchmark save("df_fileio.csv", $df)

using FstFileFormat

@time FstFileFormat.write( df, "test.df");


@time FstFileFormat.read("d:/df_julia.fst"); #414.801426 seconds (1.80 G allocations: 127.457 GiB, 59.45% gc time)

    
function cleansave(data, fname)
    b = IOBuffer();
    writedlm(b, data); ##write to buffer first / while processing
    fid = open(fname, "w");
    seek(b, 0);
    write(fid, read(b)); ## then save
    close(fid);
end;
cleansave(df[1,:], "dfcleansave.csv")

@time cleansave(df, "dfcleansave.csv")




using RCall

R"""
memory.limit(2^31-1)
"""

@time R"""
memory.limit(2^31-1)
fst::write_fst($df, "d:/df_julia.fst")
NULL
"""

R"""
library(fst)
system.time(write_fst(a, "a30.fst", 30)) # 24
system.time(write_fst(a, "a70.fst", 70)) # 34
system.time(write_fst(a, "a100.fst", 100)) # 114
system.time(write_fst(a, "a0.fst", 0)) # 20

system.time(read_fst("a30.fst")) # 84
system.time(read_fst("a70.fst")) # 70
system.time(read_fst("a100.fst")) # 73.73
system.time(read_fst("a0.fst")) # 72

library(feather)
system.time(write_feather(a,"a.feather")) # 38.44
system.time(read_feather("a.feather")) #65.75
"""