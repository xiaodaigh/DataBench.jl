using CSV, DataFrames, DataFramesMeta
respath = "benchmark/bench_string_results/"
paths = readdir(respath)
@time df = reduce(vcat, CSV.read.(respath.*paths))

df1 = by(df, [:test,:n,:strlen, :strlentype]) do df1
    mean(df1[:elapsed])
end
rename!(df1,:x1=>:elapsed)

df_var = df1[df1[:strlentype] .== "variable",:]
df_fix = df1[df1[:strlentype] .== "fixed",:]
unstack(df_var, [:n, :strlen],:test,:elapsed)
unstack(df_fix, [:n, :strlen],:test,:elapsed)


# find the winnder of fixed length string
function findwinnerstrsort(df1)
    df_winner = by(df1,[:n,:strlen]) do subdf
        me = minimum(subdf[:elapsed])
        subdf[subdf[:elapsed] .== me,[:test,:elapsed]]
    end

    df_no_r = df1[df1[:test] .!= "r_radixsort_elapsed",:]
    df_winner_no_r = by(df_no_r,[:n,:strlen]) do subdf
        me = minimum(subdf[:elapsed])
        subdf[subdf[:elapsed] .== me,[:test,:elapsed]]
    end

    rename!(df_winner_no_r,:test=>:test2)
    rename!(df_winner_no_r,:elapsed=>:elapsed2)

    dfres = join(df_winner, df_winner_no_r, on=[:n,:strlen])
    dfres[:speed_ratio] = dfres[:elapsed2]./dfres[:elapsed]
    dfres
end

findwinnerstrsort(df_var)
findwinnerstrsort(df_fix)
