using IndexedTables, PooledArrays, NamedTuples, FastGroupBy, SAC
import Base.ht_keyindex
# Pkg.checkout("JuliaDB")
# Pkg.rm("TextParse")
# Pkg.checkout("JuliaDB")
# Pkg.add("JuliaDB")

function randstrarray(pool, N, K)
    PooledArray(PooledArrays.RefArray(rand(UInt8(1):UInt8(K), N)), pool)
end

function createIndexedTable(N::Int,K::Int)
  pool = [@sprintf "id%03d" k for k in 1:K]
  pool1 = [@sprintf "id%010d" k for k in 1:(N/K)]

  DT = IndexedTable(
    IndexedTables.Columns(
      row_id = [1:N;]
      ),
    IndexedTables.Columns(
      id1 = randstrarray(pool, N, K),
      id2 = randstrarray(pool, N, K),
      id3 = randstrarray(pool1, N, K),
      id4 = rand(1:K, N),                          # large groups (int)
      id5 = rand(1:K, N),                          # large groups (int)
      id6 = rand(1:(N/K), N),                        # small groups (int)
      v1 =  rand(1:5, N),                          # int in range [1,5]
      v2 =  rand(1:5, N),                          # int in range [1,5]
      v3 =  rand(round.(rand(100)*100,4), N) # numeric e.g. 23.5749
      ));
  return DT
end

function run_juliadb_bench(N::Int = Int64(2e9/8), K::Int = 100)
  # warm up
  createIndexedTable(1,1)

  # create true dataset
  #julia_timings = Dict{Symbol, Float64}()
  timegen = @elapsed DT = createIndexedTable(N, K)
  julia_timings = Dict(
    :gen_syn => timegen,
    :sum1_1 => (@elapsed sumby(DT, :id1, :v1)),
    :sum1_2 => (@elapsed sumby(DT, :id1, :v1)),
    :sum2_1 => (@elapsed aggregate(+, DT, by=(:id1,:id2), with=:v1)),
    :sum2_2 => (@elapsed aggregate(+, DT, by=(:id1,:id2), with=:v1)),
    :sum_mean1 => (@elapsed [sumby(DT, :id3, :v1), meanby(DT,:id3, :v3)]),
    :sum_mean2 => (@elapsed [sumby(DT, :id3, :v1), meanby(DT,:id3, :v3)]),
    :mean7_9_by_id4_1 => (@elapsed [meanby(DT, :id4, :v1), meanby(DT, :id4, :v2), meanby(DT, :id4, :v3)]),
    :mean7_9_by_id4_2 => (@elapsed [meanby(DT, :id4, :v1), meanby(DT, :id4, :v2), meanby(DT, :id4, :v3)]),
    :sum7_9_by_id6_1 => (@elapsed groupreduce(
        x->x[1],
        x->(x[2],x[3],x[4]),
        (x,y)->(x[1]+y[1],x[2]+y[2],x[3]+y[3]),
        zip(column(DT,:id6),column(DT,:v1),column(DT,:v2),column(DT,:v3)))),
    :sum7_9_by_id6_2 => (@elapsed groupreduce(
        x->x[1],
        x->(x[2],x[3],x[4]),
        (x,y)->(x[1]+y[1],x[2]+y[2],x[3]+y[3]),
        zip(column(DT,:id6),column(DT,:v1),column(DT,:v2),column(DT,:v3))))
  )

  return julia_timings
end

# function run_sac_bench(N::Int = Int64(2e9/8), K::Int = 100)
#   # create true dataset
#   #julia_timings = Dict{Symbol, Float64}()
#   timegen = @elapsed DT = createIndexedTable(N, K)
#   sac_timings = Dict(
#     :gen_syn => timegen,
#     :sum1_1 => (@elapsed groupreduce(x->x[1], x->x[2], (x,y) -> x+y, zip(column(DT,:id1),column(DT,:v1)))),
#     :sum1_2 => (@elapsed groupreduce(x->x[1], x->x[2], (x,y) -> x+y, zip(column(DT,:id1),column(DT,:v1)))),
#     :sum2_1 => (@elapsed groupreduce(x->(x[1], x[2]), x -> x[3], (x, y) -> x+y, zip(column(DT,:id1),column(DT,:id2),column(DT,:v1)))),
#     :sum2_2 => (@elapsed groupreduce(x->(x[1], x[2]), x -> x[3], (x, y) -> x+y, zip(column(DT,:id1),column(DT,:id2),column(DT,:v1)))),
#     :sum_mean1 => (@elapsed groupreduce(x->x[1], x -> (x[2],x[3],1), (x, y) -> (x[1]+y[1],x[2]+y[2],x[3]+y3]), zip(column(DT,:id3),column(DT,:v1),column(DT,:v3)))),
#     :sum_mean2 => (@elapsed groupreduce(x->x[1], x -> (x[2],x[3],1), (x, y) -> (x[1]+y[1],x[2]+y[2],x[3]+y3]), zip(column(DT,:id3),column(DT,:v1),column(DT,:v3)))),
#     :mean7_9_by_id4_1 => (@elapsed [meanby(DT, :id4, :v1), meanby(DT, :id4, :v2), meanby(DT, :id4, :v3)]),
#     :mean7_9_by_id4_2 => (@elapsed [meanby(DT, :id4, :v1), meanby(DT, :id4, :v2), meanby(DT, :id4, :v3)]),
#     :sum7_9_by_id6_1 => (@elapsed [sumby(DT, :id6, :v1), sumby(DT, :id6, :v2), sumby(DT, :id6, :v3)]),
#     :sum7_9_by_id6_2 => (@elapsed [sumby(DT, :id6, :v1), sumby(DT, :id6, :v2), sumby(DT, :id6, :v3)])
#   )
#
#   return sac_timings
# end
