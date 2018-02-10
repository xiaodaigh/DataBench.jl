using Revise
using DataBench
using uCSV, Feather, BenchmarkTools, CSV, TextParse, FileIO, JLD, IterableTables, JuliaDB, IndexedTables, uCSV, JLD2
srand(1);
const N = 1_000_000; const K = 100
@time df = DataBench.createSynDataFrame(N, K); #31 #40

@benchmark Feather.write("df.feather",df); # 138
@benchmark CSV.write("df.csv",df); #569.749011
@benchmark FileIO.save("df_fileio.csv", df); # 209.438193 seconds (1.20 G allocations: 47.704 GiB, 5.91% gc time)
@benchmark uCSV.write("df_u.csv",df); #528.785193 seconds (3.60 G allocations: 157.952 GiB, 8.43% gc time)
@benchmark FileIO.save("df.jld","df",df) #215.839709 seconds (1.16 k allocations: 6.706 GiB, 2.50% gc time)
@benchmark @save "d:/df.jld2" df #765.809597 seconds (2.70 G allocations: 58.094 GiB, 19.22% gc time)


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
@time @save "d:/df.jld2" df[1,:]
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