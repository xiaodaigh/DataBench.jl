using IndexedTables, PooledArrays, NamedTuples, FastGroupBy
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
    :sum7_9_by_id6_1 => (@elapsed [sumby(DT, :id6, :v1), sumby(DT, :id6, :v2), sumby(DT, :id6, :v3)]),
    :sum7_9_by_id6_2 => (@elapsed [sumby(DT, :id6, :v1), sumby(DT, :id6, :v2), sumby(DT, :id6, :v3)])
  )

  return julia_timings
end
