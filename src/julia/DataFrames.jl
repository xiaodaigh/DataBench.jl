using DataFrames

function createSynDataFrame(N::Int,K::Int)
    pool = "id".*dec.(1:K,3)
    pool1 = "id".*dec.(1:N÷K,10)
    nums = round.(rand(100).*100, 4)

    df = DataFrame(
        id1 = rand(pool,N),
        id2 = rand(pool,N),
        id3 = rand(pool1,N),
        id4 = rand(1:K,N),
        id5 = rand(1:K,N),
        id6 = rand(1:(N/K),N),
        v1 = rand(1:5,N),
        v2 = rand(1:5,N),
        v3 = rand(nums,N))
    return df
end


function createSynDataFrame_categoricalarray(N::Int,K::Int)
    pool = "id".*dec.(1:K,3)
    pool1 = "id".*dec.(1:N÷K,10)
    nums = round.(rand(100)*100, 4)

    df = DataFrame(
        id1 = rand(pool,N),
        id2 = rand(pool,N),
        id3 = rand(pool1,N),
        id4 = rand(1:K,N),
        id5 = rand(1:K,N),
        id6 = rand(1:(N/K),N),
        v1 = rand(1:5,N),
        v2 = rand(1:5,N),
        v3 = rand(nums, N))
    return df
end