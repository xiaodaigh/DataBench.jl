using DataBench, FastGroupBy, IndexedTables, IterableTables, SplitApplyCombine
import DataFrames.DataFrame
@time addprocs()
@time @everywhere using FastGroupBy
#using DataFrames, CSV
#import Base.ht_keyindex
# write your own tests here

@time b = run_juliadb_bench(1_000_000, 100)
@time b1 = run_juliadb_bench_pmap(1_000_000, 100)
# @time a = R_bench(1_000_000, 100; libpath = "C:/Users/dzj/Documents/R/win-library/3.4")
# c = Dict(n => b[n]/a[n][1] for n in names(a))
#
# @test length(c) == 11

@time b1 = run_juliadb_bench_pmap()
gc()
file1 = replace("test/results/julia_pmap $(now()).csv",":","")
using CSV
CSV.write(file1, DataFrame(;b1...));

@time b = run_juliadb_bench()
gc()
file1 = replace("test/results/julia $(now()).csv",":","")
using CSV
CSV.write(file1, DataFrame(;b...));

rd = readdir("test/results/")
substr(rd,1,1)

@time a = R_bench(;libpath = "C:/Users/dzj/Documents/R/win-library/3.4")
file1 = replace("test/results/r $(now()).csv",":","")
using CSV
CSV.write(file1, a);

# compute relativities to R's data.table
c = Dict(n => b[n]/a[n][1] for n in names(a))
@test length(c) == 11

# collate the results
abc = vcat(a, DataFrame(;b...), DataFrame(;c...))
abc[:role] = ["R","Julia","Ratio"];


# test sumby
srand(1);

#
:sum7_9_by_id6_1 => (@elapsed groupreduce(
    x->x[1],
    x->(x[2],x[3],x[4]),
    (x,y)->(x[1]+y[1],x[2]+y[2],x[3]+y[3]),
    zip(column(DT,:id6),column(DT,:v1),column(DT,:v2),column(DT,:v3)))),


#@time DT = createIndexedTable(Int64(2e9/8), 100);
N = 100_000_000
K = 100
NK = Int64(round(N/K))

@time id6 = rand(1:NK,N)
@time v1 = rand(1:5,N)

# normal sum by
@time sumby(id6, v1);
@time sumby(id6, v1);

# groupreduce
@time groupreduce(x->x[1], x->x[2],(x,y) -> x+y, zip(id6,v1));
@time groupreduce(x->x[1], x->x[2],(x,y) -> x+y, zip(id6,v1));

# sumby with processes
addprocs(4)
@everywhere using FastGroupBy
@time sid6 = SharedArray(id6);
@time sv1 = SharedArray(v1);

@time res = psumby(sid6, sv1);
@time res = psumby(sid6, sv1);

@time res = psumby(id6, v1);
@time res = psumby(id6, v1);

# data benchmarks
@time dtdf = DataFrames.DataFrame(DT);
@time meanby(dtdf, :id4, :v1)


timings = Dict();
@time res = meanby(rand(1:100,2), rand(1:100,2))

@time res1_1 = meanby(DT, :id4, :v1);
@time res1_2 = meanby(DT, :id4, :v1);
@time res2_1 = meanby(DT, :id4, :v3);
@time res2_2 = meanby(DT, :id4, :v3);

using DataFramesMeta
@time @by(dtdf, :id4, d = mean(:v1)); # about 10 times slower

#meanby(dtdf, :id4, :v1)
@time dtdt = IndexedTables.IndexedTable(dtdf)
@time meanby(dtdt, :id4, :v1)
