#https://www.kaggle.com/c/flight
@time using JuliaDB, IndexedTables #8.5 seconds
function test_InitialTrainingSet_rev1(path = "D:/data/InitialTrainingSet_rev1/", outpath = "d:/gcflights")
  files = path .* readdir(path) .* "/ASDI/asdifpwaypoint.csv"
  #@time @everywhere using Dagger, IndexedTables, JuliaDB # 0.5 second
  @time df = JuliaDB.loadfiles(files, indexcols = ["asdiflightplanid", "ordinal"]);
  #@time df = JuliaDB.loadfiles(files);
  @time JuliaDB.save(df,outpath);
end

function test_load_juliadb(path="d:/gcflights")
  @time df = JuliaDB.load(path);
end

# @time test_InitialTrainingSet_rev1()

path = "D:/data/InitialTrainingSet_rev1/"
outpath = "d:/gcflights"
files = path .* readdir(path) .* "/ASDI/asdifpwaypoint.csv"
#@time @everywhere using Dagger, IndexedTables, JuliaDB # 0.5 second
# addprocs()
@time df = JuliaDB.loadtable(files, indexcols = ["asdiflightplanid", "ordinal"])
colnames(df)
#@time df = JuliaDB.loadfiles(files);
@time JuliaDB.save(df,outpath)

select(df, :ordinal)
