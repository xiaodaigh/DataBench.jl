using StatsBase, RCall, Query, DataFrames, DataFramesMeta, Distributions, PooledArrays, IndexedTables

function query_benches(df::DataFrame)

    # timings
    ti = Dict()

    ti[:sum1] = @elapsed @from i in df begin
                             @group i by i.id1 into g
                             @select {r=sum(g..v1)}
                             @collect DataFrame
                         end
    ti[:sum2] = @elapsed @from i in df begin
                             @group i by i.id1 into g
                             @select {r=sum(g..v1)}
                             @collect DataFrame
                         end
    ti[:sum3] = @elapsed @from i in df begin
                             @group i by (i.id1,i.id2) into g
                             @select {r=sum(g..v1)}
                             @collect DataFrame
                         end
    ti[:sum4] = @elapsed @from i in df begin
                             @group i by (i.id1,i.id2) into g
                             @select {r=sum(g..v1)}
                             @collect DataFrame
                         end
    ti[:sum_mean1] = @elapsed @from i in df begin
                             @group i by i.id3 into g
                             @select {s=sum(g..v1),m=mean(g..v3)}
                             @collect DataFrame
                         end
    ti[:sum_mean2] = @elapsed @from i in df begin
                             @group i by i.id3 into g
                             @select {s=sum(g..v1),m=mean(g..v3)}
                             @collect DataFrame
                         end
    ti[:mean7_9_by_id4_1] = @elapsed @from i in df begin
                             @group i by i.id4 into g
                             @select {m7=mean(g..v1),m8=mean(g..v2),m9=mean(g..v3)}
                             @collect DataFrame
                         end
    ti[:mean7_9_by_id4_2] = @elapsed @from i in df begin
                             @group i by i.id4 into g
                             @select {m7=mean(g..v1),m8=mean(g..v2),m9=mean(g..v3)}
                             @collect DataFrame
                         end
    ti[:sum7_9_by_id6_1] = @elapsed @from i in df begin
                             @group i by i.id6 into g
                             @select {m7=mean(g..v1),m8=mean(g..v2),m9=mean(g..v3)}
                             @collect DataFrame
                         end
    ti[:sum7_9_by_id6_2] = @elapsed @from i in df begin
                             @group i by i.id6 into g
                             @select {m7=mean(g..v1),m8=mean(g..v2),m9=mean(g..v3)}
                             @collect DataFrame
                         end
    return ti
end

function query_benches(df::IndexedTable)

    # timings
    ti = Dict()

    ti[:sum1] = @elapsed @from i in df begin
                             @group i by i.id1 into g
                             @select {r=sum(g..v1)}
                             @collect DataFrame
                         end
    ti[:sum2] = @elapsed @from i in df begin
                             @group i by i.id1 into g
                             @select {r=sum(g..v1)}
                             @collect DataFrame
                         end
    ti[:sum3] = @elapsed @from i in df begin
                             @group i by (i.id1,i.id2) into g
                             @select {r=sum(g..v1)}
                             @collect DataFrame
                         end
    ti[:sum4] = @elapsed @from i in df begin
                             @group i by (i.id1,i.id2) into g
                             @select {r=sum(g..v1)}
                             @collect DataFrame
                         end
    ti[:sum_mean1] = @elapsed @from i in df begin
                             @group i by i.id3 into g
                             @select {s=sum(g..v1),m=mean(g..v3)}
                             @collect DataFrame
                         end
    ti[:sum_mean2] = @elapsed @from i in df begin
                             @group i by i.id3 into g
                             @select {s=sum(g..v1),m=mean(g..v3)}
                             @collect DataFrame
                         end
    ti[:mean7_9_by_id4_1] = @elapsed @from i in df begin
                             @group i by i.id4 into g
                             @select {m7=mean(g..v1),m8=mean(g..v2),m9=mean(g..v3)}
                             @collect DataFrame
                         end
    ti[:mean7_9_by_id4_2] = @elapsed @from i in df begin
                             @group i by i.id4 into g
                             @select {m7=mean(g..v1),m8=mean(g..v2),m9=mean(g..v3)}
                             @collect DataFrame
                         end
    ti[:sum7_9_by_id6_1] = @elapsed @from i in df begin
                             @group i by i.id6 into g
                             @select {m7=mean(g..v1),m8=mean(g..v2),m9=mean(g..v3)}
                             @collect DataFrame
                         end
    ti[:sum7_9_by_id6_2] = @elapsed @from i in df begin
                             @group i by i.id6 into g
                             @select {m7=mean(g..v1),m8=mean(g..v2),m9=mean(g..v3)}
                             @collect DataFrame
                         end
    return ti
end


function run_query_benches(N=1_000_000;K=100)
    # get small data for JIT warmup
    d_ = createSynDataFrame(10,3)
    # dt_ = createDataTable(10,3)
    

    # warm up julia benchmarks
    query_benches(d_);
    # benches(dt_);
    benches(di_);
    DfMeta_benches(d_);

    # get real data
    d = createDataFrame(N,K)
    # measure DataFrames
    query_df = benches(d)
    meta = DfMeta_benches(d)
    d = 0
    gc()
    # measure DataTables
    # dt = createDataTable(N,K)
    # query_dt = benches(dt)
    # dt = 0
    # gc()
    # measure IndexedTables
    di = createIndexedTable(N,K)
    query_di = benches(di)
    di = 0
    gc()


    # get R time
    R = R_bench(N,K)

    # get
    k = collect(keys(query_df))
    #out = DataFrame(bench = k,Query_DF = [query_df[kk] for kk in k],Query_DT= [query_dt[kk] for kk in k],Query_idxT= [query_di[kk] for kk in k],DataFramesMeta=[meta[kk] for kk in k], Rdatatable=[R[kk] for kk in k])
    out = DataFrame(bench = k,Query_DF = [query_df[kk] for kk in k] ,Query_idxT= [query_di[kk] for kk in k],DataFramesMeta=[meta[kk] for kk in k], Rdatatable=[R[kk] for kk in k])
    sort!(out,cols=:bench)
    rel = deepcopy(out)
    #rel = @transform(rel,Query_DF = :Query_DF./:Rdatatable, Query_DT = :Query_DT./:Rdatatable, Query_idxT = :Query_idxT./:Rdatatable, DataFramesMeta = :DataFramesMeta ./ :Rdatatable, Rdatatable = 1.0)
    rel = @transform(rel,Query_DF = :Query_DF./:Rdatatable, Query_idxT = :Query_idxT./:Rdatatable, DataFramesMeta = :DataFramesMeta ./ :Rdatatable, Rdatatable = 1.0)
    return (out,rel)
end

function run_all()
    d=Dict()
    for n in [1_000_000, 2_000_000]
        d[n] = run_benches(n)
        gc()
        info("results with $n rows:")
        info("-- absolute time in seconds:")
        println(d[n][1])
        info("-- time relative to R data.table:")
        println(d[n][2])
        println()
        println()
    end
    return d
end
