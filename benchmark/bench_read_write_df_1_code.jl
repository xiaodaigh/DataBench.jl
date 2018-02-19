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

    FileIO.save(outpath*"df_fileio.csv", df)
    frm = pd.read_csv(outpath*"df_fileio.csv")  # will be used for testting Pandas
    # frm[:to_feather]("d:/tmp/p.feather")
    # frm[:to_parquet]("d:/tmp/p.feather")

    if exclslow
        writeres = [
            @benchmark(Feather.write(outpath*"df.feather", $df)), # 138
            # @benchmark(CSV.write(outpath*"df.csv", $df)), #569.749011
            @benchmark(FileIO.save(outpath*"df_fileio.csv", $df)), # 209.438193 seconds (1.20 G allocations: 47.704 GiB, 5.91% gc time)
            # @benchmark(uCSV.write(outpath*"df_u.csv", $df)), #528.785193 seconds (3.60 G allocations: 157.952 GiB, 8.43% gc time)
            @benchmark(FileIO.save(outpath*"df.jld","df", $df)), #215.839709 seconds (1.16 k allocations: 6.706 GiB, 2.50% gc time)
            # @benchmark(JLD2.@save(outpath*"df.jld2", $df)), #765.809597 seconds (2.70 G allocations: 58.094 GiB, 19.22% gc time)
            #,@benchmark(JuliaDB.save($dfit,outpath*randstring(8)))
            @benchmark($frm[:to_csv](joinpath(outpath,"df_pandas.csv")))  # pandas writing
            # ,@benchmark($frm[:to_feather](joinpath(outpath,"df_pandas.feather")))
            ]
        readres = [
            @benchmark(Feather.read(outpath*"df.feather"))
            ,@benchmark(DataFrame(FileIO.load(outpath*"df_fileio.csv")))
            ,@benchmark(FileIO.load(outpath*"df.jld"))
            ,@benchmark(pd.read_csv(outpath*"df_pandas.csv"))
            # ,@benchmark(pd.read_feather(outpath*"df_pandas.feather"))
        ]
        return (writeres, readres)
    else
        dfit = table(df)

        writeres = [
            @benchmark(Feather.write(outpath*"df.feather", $df)) , # 138
            @benchmark(CSV.write(outpath*"df.csv", $df)), #569.749011
            @benchmark(FileIO.save(outpath*"df_fileio.csv", $df)), # 209.438193 seconds (1.20 G allocations: 47.704 GiB, 5.91% gc time)
            @benchmark(uCSV.write(outpath*"df_u.csv", $df)), #528.785193 seconds (3.60 G allocations: 157.952 GiB, 8.43% gc time)
            @benchmark(FileIO.save(outpath*"df.jld","df", $df)), #215.839709 seconds (1.16 k allocations: 6.706 GiB, 2.50% gc time)
            @benchmark(JLD2.@save(outpath*"df.jld2", $df)), #765.809597 seconds (2.70 G allocations: 58.094 GiB, 19.22% gc time)
            # @benchmark(JuliaDB.save($dfit,outpath*randstring(8))),
            # @benchmark(JuliaDB.save($dfit,"df.juliadb")),
            @benchmark($frm[:to_csv](joinpath(outpath,"df_pandas.csv")))  # pandas writing
            ]

        jldpath = outpath*"df.jld2"
        readres = [
            @benchmark(Feather.read(outpath*"df.feather"))
            ,@benchmark(CSV.read(outpath*"df.csv"))
            ,@benchmark(DataFrame(FileIO.load(outpath*"df_fileio.csv")))
            ,@benchmark(uCSV.read(outpath*"df_u.csv"))
            ,@benchmark(FileIO.load(outpath*"df.jld"))
            ,@benchmark(JLD2.@load("d:/tmp/df.jld2"))
            ,@benchmark(pd.read_csv(outpath*"df_pandas.csv"))
        ]
        return (writeres, readres)
    end
end

function rreadwrite(outpath)
    r = R"""
    memory.limit(2^31-1) # windows only; to get rid of memory limit
    library(fst)
    library(feather)
    library(data.table)

    pt = proc.time()
    df <- feather::read_feather(file.path($outpath,"df.feather"))
    featherr = proc.time() - pt

    pt = proc.time()
    feather::write_feather(df, file.path($outpath,"df_r.feather"))
    featherw = proc.time() - pt

    pt = proc.time()
    system.time(write_fst(df,file.path($outpath,"df_default.fst")))[3]
    fstw = proc.time() - pt

    #system.time(write_fst(df,file.path($outpath,"df_0.fst"), 0))[3]
    #system.time(write_fst(df,file.path($outpath,"df_100.fst"), 100))[3]

    pt = proc.time()
    system.time(read_fst(file.path($outpath,"df_default.fst")))[3]
    fstr= proc.time() - pt

    # system.time(read_fst(file.path($outpath,"df_0.fst")))[3],
    # system.time(read_fst(file.path($outpath,"df_100.fst")))[3],

    # multi threaded read write
    pt = proc.time()
    fwrite(df, file.path($outpath, "df_fwrite.csv"))
    fwritet = proc.time() - pt

    pt = proc.time()
    #system.time(fread("df_fwrite.csv"))[3]
    system.time(fread(file.path($outpath, "df_fwrite.csv")))[3]
    freadr = proc.time() - pt

    list(
        fstw[3],
        fstr[3],
        fwritet[3],
        freadr[3],
        featherw[3],
        featherr[3]
    )
    """
    [Float64(r[i]) for i=1:length(r)]
end

function plot_bench_df_read_write(julres, rres, N, exclslow=true)
    if exclslow
        x = ["Feather.jl",          "TextParse.jl\n FileIO.jl",         "JLD.jl\n FileIO.jl", "Python\n Pandas"]
        rx = ["R\n fst (default)","R\n data.table", "R\n feather"]

        x = vcat(repeat(x, outer=2), repeat(rx, inner=2))
        rw = ["write","read"]

        group = vcat(repeat(rw, inner=4), repeat(rw, outer=3))

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
        #GR.savefig("e:/read_write_df_bench $(Int(N/1_000_000))m.png")
    else
        x = ["Feather.jl","CSV.jl", "TextParse.jl\n FileIO.jl","uCSV.jl","JLD.jl\n FileIO.jl","JLD2.jl", "Python\n Pandas"]
        rx = ["R\n fst (default)","R\n data.table", "R\n feather"]

        x = vcat(repeat(x, outer=2), repeat(rx, inner=2))
        rw = ["write","read"]

        group = vcat(repeat(rw, inner=7), repeat(rw, outer=3))

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
    end
end


