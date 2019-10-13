using CSV, Feather
using JDF, FileIO, Blosc, StatsPlots, RCall
using DataFrames, WeakRefStrings # required for JLD2, JDF
using Fread:fread
Blosc.set_num_threads(6)

gen_benchmark(dirpath, largest_file, outpath, data_label; delim = ',', header=true) = begin
    if !isdir(outpath)
        mkpath(outpath)
    end

    println("CSV.read")
    csv_read1 = @elapsed df = CSV.read(joinpath(dirpath, largest_file), delim = delim, header = header, threaded=false);
    csv_read2 = @elapsed df = CSV.read(joinpath(dirpath, largest_file), delim = delim, header = header, threaded=false);

    csv_write1 = 0
    csv_write2 = 0
    try
        csv_write1 = @elapsed CSV.write(joinpath(outpath, largest_file*".csv"), df);
        csv_write2 = @elapsed CSV.write(joinpath(outpath, largest_file*".csv"), df);
    catch err
    end

    R"""
    library(data.table)
    library(fst)
    # memory.limit(Inf)
    data_table_read1 = system.time(a <- data.table::fread($(joinpath(dirpath, largest_file))))[3]
    data_table_read2 = system.time(data.table::fread($(joinpath(dirpath, largest_file))))[3]
    data_table_write1 = system.time(data.table::fwrite(a, $(joinpath(outpath, largest_file*".data.table.csv"))))[3]
    data_table_write2 = system.time(data.table::fwrite(a, $(joinpath(outpath, largest_file*".data.table.csv"))))[3]
    fst_write1 = system.time(fst::write_fst(a, $(joinpath(outpath, largest_file*".fst"))))[3]
    fst_write2 = system.time(fst::write_fst(a, $(joinpath(outpath, largest_file*".fst"))))[3]
    fst_read1 = system.time(fst::read_fst($(joinpath(outpath, largest_file*".fst"))))[3]
    fst_read2 = system.time(fst::read_fst($(joinpath(outpath, largest_file*".fst"))))[3]
    parquet_r_write1 = system.time(arrow::write_parquet(a, $(joinpath(outpath, largest_file*".parquet"))))[3]
    parquet_r_write2 = system.time(arrow::write_parquet(a, $(joinpath(outpath, largest_file*".parquet"))))[3]
    parquet_r_read1 = system.time(arrow::read_parquet($(joinpath(outpath, largest_file*".parquet"))))[3]
    parquet_r_read2 = system.time(arrow::read_parquet($(joinpath(outpath, largest_file*".parquet"))))[3]
    rm(a)
    gc()
    """

    @rget data_table_read1
    @rget data_table_read2
    @rget data_table_write1
    @rget data_table_write2
    @rget fst_write1
    @rget fst_write2
    @rget fst_read1
    @rget fst_read2
    @rget parquet_r_read1
    @rget parquet_r_read2
    @rget parquet_r_write1
    @rget parquet_r_write2

    fread_read1 = 0
    fread_read2 = 0
    fread_read1 = @elapsed fread(joinpath(dirpath, largest_file))
    fread_read2 = @elapsed fread(joinpath(dirpath, largest_file))


    # jlso_write1 = 0
    # jlso_write2 = 0
    # try
    #     jlso_write1 = @elapsed JLSO.save(joinpath(outpath, largest_file*".jlso"), df);
    #     jlso_write2 = @elapsed JLSO.save(joinpath(outpath, largest_file*".jlso"), df);
    # catch err
    # end

    # jld2_write1 = 0
    # jld2_write2 = 0
    # try
    #     jld2_write1 = @elapsed save(joinpath(outpath, largest_file*".jld2"), Dict("df" => df));
    #     jld2_write2 = @elapsed save(joinpath(outpath, largest_file*".jld2"), Dict("df" => df));
    # catch err
    # end

    # jld_write1 = 0
    # jld_write2 = 0
    # try
    #     jld_write1 = @elapsed save(joinpath(outpath, largest_file*".jld"), Dict("df" => df));
    #     jld_write2 = @elapsed save(joinpath(outpath, largest_file*".jld"), Dict("df" => df));
    # catch err
    # end

    jdf_write1 = 0
    jdf_write2 = 0
    try
        jdf_write1 = @elapsed savejdf(joinpath(outpath, largest_file*".jdf"), df);
        jdf_write2 = @elapsed savejdf(joinpath(outpath, largest_file*".jdf"), df);
    catch err

    end

    # # feather can't handle all missing
    for n in names(df)
        if eltype(df[!,n]) == Missing
            println("Removed $n for Feather.jl")
            select!(df, Not(n))
            df[!,n] = Vector{Union{Missing, Bool}}(missing, size(df, 1))
        end
    end

    feather_write1 = 0
    feather_write2 = 0
    try
        feather_write1 = @elapsed Feather.write(joinpath(outpath, largest_file*".feather"), df);
        feather_write2 = @elapsed Feather.write(joinpath(outpath, largest_file*".feather"), df);
    catch err
    end


    #########################################  loading
    feather_read1 = 0
    feather_read2 = 0
    try
        feather_read1 = @elapsed Feather.read(joinpath(outpath, largest_file*".feather"));
        feather_read2 = @elapsed Feather.read(joinpath(outpath, largest_file*".feather"));
    catch err
    end

    # jld_read1 = 0
    # jld_read2 = 0
    # try
    #     jld_read1 = @elapsed load(joinpath(outpath, largest_file*".jld"))["df"];
    #     jld_read2 = @elapsed load(joinpath(outpath, largest_file*".jld"))["df"];
    # catch err
    # end

    # jld2_read1 = 0
    # jld2_read2 = 0
    # try
    #     jld2_read1 = @elapsed load(joinpath(outpath, largest_file*".jld2"))["df"];
    #     jld2_read2 = @elapsed load(joinpath(outpath, largest_file*".jld2"))["df"];
    # catch err
    # end
    #
    # jlso_read1 = 0
    # jlso_read2 = 0
    # try
    #     jlso_read1 = @elapsed JLSO.load(joinpath(outpath, largest_file*".jlso"))["data"];
    #     jlso_read2 = @elapsed JLSO.load(joinpath(outpath, largest_file*".jlso"))["data"];
    # catch err
    # end

    jdf_read1 = 0
    jdf_read2 = 0
    try
        jdf_read1 = @elapsed loadjdf(joinpath(outpath, largest_file*".jdf"));
        jdf_read2 = @elapsed loadjdf(joinpath(outpath, largest_file*".jdf"));
    catch err
    end

    # write_perf = [jdf_write1, jdf_write2, csv_write1, csv_write2, feather_write1, feather_write2, jld2_write1, jld2_write2, jlso_write1, jlso_write2]
    # read_perf = [jdf_read1, jdf_read2, csv_read1, csv_read2, feather_read1, feather_read2, jld2_read1, jld2_read2, jlso_read1, jlso_read2]

    write_perf = [jdf_write1, jdf_write2, csv_write1, csv_write2, feather_write1, feather_write2, data_table_write1, data_table_write2, fst_write1, fst_write2, parquet_r_write1, parquet_r_write2]
    read_perf  = [jdf_read1,  jdf_read2,  csv_read1, csv_read2, feather_read1, feather_read2, fread_read1, fread_read2, data_table_read1, data_table_read2, fst_read1, fst_read2, parquet_r_read1, parquet_r_read2]

    pkgs = repeat(["JDF.jl", "CSV.jl", "Feather.jl", "data.table", "fst", "parquet R"], inner = 2)
    run_group = repeat(["1st", "2nd"], outer = 6)

    plot_write = groupedbar(
        pkgs,
        write_perf,
        group = run_group,
        ylab = "Seconds",
        title = "Disk-format Write performance comparison \n Data: $data_label data \n Size: $(size(df)) filesize:$(round(filesize(joinpath(dirpath, largest_file))/1024^3, digits=1))GB \n Julia $(VERSION)"
    )
    savefig(plot_write, joinpath(outpath, largest_file*"plot_write.png"))

    # plot_write_wo_jlso = groupedbar(
    #     repeat(["JDF.jl", "CSV.jl", "Feather.jl", "JLD2.jl"], inner = 2),
    #     [jdf_write1, jdf_write2, csv_write1, csv_write2, feather_write1, feather_write2, jld2_write1, jld2_write2],
    #     group = repeat(["1st", "2nd"], outer = 4),
    #     ylab = "Seconds",
    #     title = "Disk-format Write performance comparison \n Data: Undefined control sequence \n(size(df)) filesize:Undefined control sequence \n(VERSION)"
    # )
    # savefig(plot_write_wo_jlso, joinpath(outpath, largest_file*"plot_write_wo_jlso.png"))

    read_pkgs = repeat(["JDF.jl", "CSV.jl", "Feather.jl", "Fread.jl", "data.table", "fst", "parquet R"], inner = 2)
    read_run_group = repeat(["1st", "2nd"], outer = 7)

    plot_read = groupedbar(
        read_pkgs,
        read_perf,
        group = read_run_group,
        ylab = "Seconds",
        title = "Disk-format Read performance comparison \n Data: $data_label data \n Size: $(size(df)) filesize:$(round(filesize(joinpath(dirpath, largest_file))/1024^3, digits=1))GB \n Julia $(VERSION)"
    )
    savefig(plot_read, joinpath(outpath, largest_file*"plot_read.png"))

    # plot_read_wo_csv_jlso = groupedbar(
    #     repeat(["JDF.jl", "Feather.jl", "JLD2.jl"], inner = 2),
    #     [jdf_read1, jdf_read2, feather_read1, feather_read2, jld2_read1, jld2_read2],
    #     group = repeat(["1st", "2nd"], outer = 3),
    #     ylab = "Seconds",
    #     title = "Disk-format Read performance comparison \n Data: Undefined control sequence \n(size(df)) filesize:Undefined control sequence \n(VERSION)"
    # )
    # savefig(plot_read_wo_csv_jlso, joinpath(outpath, largest_file*"plot_read_wo_csv_jlso.png"))
    #
    # plot_read_wo_jlso = groupedbar(
    #     repeat(["JDF.jl", "CSV.jl", "Feather.jl", "JLD2.jl"], inner = 2),
    #     [jdf_read1, jdf_read2, csv_read1, csv_read2, feather_read1, feather_read2, jld2_read1, jld2_read2],
    #     group = repeat(["1st", "2nd"], outer = 4),
    #     ylab = "Seconds",
    #     title = "Disk-format Read performance comparison \n Data: Undefined control sequence \n(size(df)) filesize:Undefined control sequence \n(VERSION)"
    # )
    # savefig(plot_read_wo_jlso, joinpath(outpath, largest_file*"plot_read_wo_jlso.png"))

    (write_perf, read_perf, dirpath, outpath, largest_file, df)
end


sum_file_size(dir) = begin
    res = Int[]
    for f in joinpath.(dir, readdir(dir))
        if isfile(f)
            push!(res, filesize(f))
        elseif isdir(f)
            println(f)
            push!(res, sum(filesize.(joinpath.(f,readdir(f)))))
        end
    end
    df = DataFrame(file = readdir(dir), fs = res)

    tmpfn = (x->x[end-3:end])
    df[!,:ext] = tmpfn.(df[!, :file])
    filter!(r -> r.ext != ".png", df)
    df = sort!(df, :ext)[[1:2;4], :]
    df[:pkg] = ["CSV.jl", "JDF.jl", "Feather.jl"]
    sort!(df, :pkg)
    df
end


# AirTime
@time res = gen_benchmark("c:/data/AirOnTimeCSV/", "airOT199302.csv", "c:/data/jdf-bench/airOT199302.csv", "Air On Time 199302")


# Fannie Mae

# download("http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000-2007.tgz", "c:/data/mortgage_2000-2007.tgz")

# un-tar and uncompress the file
# ;tar zxvg mortgage_2000-2007.tgz

# uncomment for debugging
dirpath = "C:/data/perf/"
largest_file = "Performance_2004Q3.txt"
outpath = "c:/data/jdf-bench/Performance_2004Q3.txt"
data_label =  "Fannie Mae Performance 2004Q3"
#
@time res = gen_benchmark(dirpath, largest_file, outpath, data_label, delim = '|', header = false);



dirpath = "C:/data/"
largest_file = "feature_matrix_cleaned.csv"
outpath = "c:/data/jdf-bench/feature_matrix_cleaned.csv"
data_label =  "Mortgage Risk with featuretools"

@time res = gen_benchmark(dirpath, largest_file, outpath, data_label);

# write_perf = res[1]
# read_perf = res[2]
# pkgs = repeat(["JDF.jl", "CSV.jl", "Feather.jl", "data.table", "fst", "parquet R"], inner = 2)
# run_group = repeat(["1st", "2nd"], outer = 6)
#
# write_perf = write_perf[pkgs .!= "Feather.jl"]
# run_group = run_group[pkgs .!= "Feather.jl"]
# pkgs1 = pkgs[pkgs .!= "Feather.jl"]
#
# plot_write = groupedbar(
#     pkgs1,
#     write_perf,
#     group = run_group,
#     ylab = "Seconds",
#     title = "Disk-format Write performance comparison \n Julia $(VERSION)"
# )
# savefig(plot_write, joinpath(outpath, largest_file*"plot_write_less.png"))
#
# pkgs = repeat(["JDF.jl", "CSV.jl", "Feather.jl", "data.table", "fst", "parquet R"], inner = 2)
# run_group = repeat(["1st", "2nd"], outer = 6)
# read_perf = read_perf[.!in.(pkgs, Ref(["CSV.jl", "parqeut R"]))]
# run_group = run_group[.!in.(pkgs, Ref(["CSV.jl", "parqeut R"]))]
# pkgs2 = pkgs[.!in.(pkgs, Ref(["CSV.jl", "parqeut R"]))]
#
# plot_read = groupedbar(
#     pkgs2,
#     read_perf,
#     group = run_group,
#     ylab = "Seconds",
#     title = "Disk-format Read performance comparison \n Julia $(VERSION)"
# )
# savefig(plot_read, joinpath(outpath, largest_file*"plot_read_less.png"))
#
#
# sizedf = sum_file_size(outpath)
#
# using StatsPlots
#
# p = plot(
#     sizedf.pkg,
#     sizedf.fs/1024^3,
#     linetype = :bar,
#     ylab = "Size (GB)",
#     legend = false,
#     title = "On-disk file Size for various formats\n $data_label data")
# savefig(p, joinpath(outpath, largest_file*"_filesize.png"))
