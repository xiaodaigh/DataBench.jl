using IndexedTables, PooledArrays, NamedTuples

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
    Columns(
      row_id = [1:N;]
      ),
    Columns(
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
  timegen = @elapsed DT = createIndexedTable(N, K)

  julia_timings = Dict(
    :gen_syn => timegen,
    :sum1_1 => (@elapsed aggregate(+, DT, by=(:id1,), with=:v1)),
    :sum1_2 => (@elapsed aggregate(+, DT, by=(:id1,), with=:v1)),
    :sum2_1 => (@elapsed aggregate(+, DT, by=(:id1,:id2), with=:v1)),
    :sum2_2 => (@elapsed aggregate(+, DT, by=(:id1,:id2), with=:v1)),
    :sum_mean1 => (@elapsed aggregate_vec(v -> @NT(sum = sum(column(v, :v1)), mean = mean(column(v, :v3))), DT, by =(:id3,), with = (:v1, :v3))),
    :sum_mean2 => (@elapsed aggregate_vec(v -> @NT(sum = sum(column(v, :v1)), mean = mean(column(v, :v3))), DT, by =(:id3,), with = (:v1, :v3))),
    :mean7_9_by_id4_1 => (@elapsed [meanby(DT, :id, :v1), meanby(DT, :id, :v2), meanby(DT, :id, :v3)]),
    :mean7_9_by_id4_1 => (@elapsed [meanby(DT, :id, :v1), meanby(DT, :id, :v2), meanby(DT, :id, :v3)]),
    #res <- c(res, list(system.time( DT[, lapply(.SD, sum), keyby=id6, .SDcols=7:9] )))
    :sum7_9_by_id6_1 => (@elapsed aggregate_vec(
      v -> @NT(
        sum1 = sum(column(v, :v1)),
        sum2 = sum(column(v, :v2)),
        sum3 = sum(column(v, :v3))
        ), DT, by =(:id6,), with = (:v1, :v2, :v3))),
    :sum7_9_by_id6_2 => (@elapsed aggregate_vec(
      v -> @NT(
        sum1 = sum(column(v, :v1)),
        sum2 = sum(column(v, :v2)),
        sum3 = sum(column(v, :v3))
        ), DT, by =(:id6,), with = (:v1, :v2, :v3)))
  )

  return julia_timings
end
