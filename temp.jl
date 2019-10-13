using CSV, Feather
using JDF, FileIO, Blosc, StatsPlots, RCall
using DataFrames, WeakRefStrings # required for JLD2, JDF
using Fread:fread
using PyCall
Blosc.set_num_threads(6)

dirpath = "C:/data/"
largest_file = "feature_matrix_cleaned.csv"
outpath = "c:/data/jdf-bench/feature_matrix_cleaned.csv"
data_label =  "Mortgage Risk with featuretools"
delim = ','
header = true


gen_benchmark3(dirpath, largest_file, outpath, data_label; delim = ',', header=true) = begin
    if !isdir(outpath)
        mkpath(outpath)
    end

    println("CSV.read")
    csv_read1 = @elapsed df = CSV.read(joinpath(dirpath, largest_file), delim = delim, header = header);
    csv_read2 = @elapsed df = CSV.read(joinpath(dirpath, largest_file), delim = delim, header = header);

    csv_write1 = 0
    csv_write2 = 0
    try
        csv_write1 = @elapsed CSV.write(joinpath(outpath, largest_file*".csv"), df);
        csv_write2 = @elapsed CSV.write(joinpath(outpath, largest_file*".csv"), df);
    catch err
    end

    delims = string(delim)
    @rput delims
    @rput header
    R"""
    library(data.table)
    library(fst)
    # memory.limit(Inf)
    readr_read1 = system.time(a <- readr::read_delim($(joinpath(dirpath, largest_file)), delim = delims, progress=FALSE, col_names=header))[3]
    # readr_read2 = system.time(a <- readr::read_delim($(joinpath(dirpath, largest_file)), delim = delims, progress=FALSE, col_names=header))[3]
    # rbase_read1 = system.time(a <- readr::read_csv($(joinpath(dirpath, largest_file)), progress=FALSE))[3]
    # rbase_read2 = system.time(a <- readr::read_csv($(joinpath(dirpath, largest_file)), progress=FALSE))[3]
    data_table_read1 = system.time(a <- data.table::fread($(joinpath(dirpath, largest_file))))[3]
    # data_table_read2 = system.time(a <- data.table::fread($(joinpath(dirpath, largest_file))))[3]
    rm(a)
    gc()
    """

    py"""
    2+2
    """

    @rget data_table_read1
    @rget data_table_read2
    # @rget rbase_read1
    # @rget rbase_read2
    @rget readr_read1
    @rget readr_read2

    fread_read1 = 0
    fread_read2 = 0
    fread_read1 = @elapsed fread(joinpath(dirpath, largest_file))
    fread_read2 = @elapsed fread(joinpath(dirpath, largest_file))

    #read_perf  = [ csv_read1, csv_read2, fread_read1, fread_read2, data_table_read1, data_table_read2, readr_read1, readr_read2, rbase_read1, rbase_read2]
    read_perf  = [ csv_read1, csv_read2, fread_read1, fread_read2, data_table_read1, data_table_read2, readr_read1, readr_read2]

    read_pkgs = repeat(["CSV.jl", "Fread.jl", "data.table", "readr"], inner = 2)
    read_run_group = repeat(["1st", "2nd"], outer = 4)

    plot_read = groupedbar(
        read_pkgs,
        read_perf,
        group = read_run_group,
        ylab = "Seconds",
        title = "Disk-format Read performance comparison \n Data: $data_label data \n Size: $(size(df)) filesize:$(round(filesize(joinpath(dirpath, largest_file))/1024^3, digits=1))GB \n Julia $(VERSION)"
    )
    savefig(plot_read, joinpath(outpath, largest_file*"plot3_read.png"))

    (read_perf, dirpath, outpath, largest_file, df)
end

#@time res = gen_benchmark3("c:/data/AirOnTimeCSV/", "airOT199302.csv", "c:/data/jdf-bench/airOT199302.csv", "Air On Time 199302")


dirpath = "C:/data/"
largest_file = "feature_matrix_cleaned.csv"
outpath = "c:/data/jdf-bench/feature_matrix_cleaned.csv"
data_label =  "Mortgage Risk with featuretools"

@time res = gen_benchmark3(dirpath, largest_file, outpath, data_label);
#
# dirpath = "C:/data/perf/"
# largest_file = "Performance_2004Q3.txt"
# outpath = "c:/data/jdf-bench/Performance_2004Q3.txt"
# data_label =  "Fannie Mae Performance 2004Q3"
# #
# @time res = gen_benchmark3(dirpath, largest_file, outpath, data_label, delim = '|', header = false);
