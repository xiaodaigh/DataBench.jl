using DataBench, FastGroupBy

srand(1);
@time DT = createIndexedTable(10_000_000, 100);

using IterableTables
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
