using DataFrames, Distributions

function createSynDataFrame(N::Int,K::Int)
    pool = "id".*dec.(1:K,3)
    pool1 = "id".*dec.(1:N÷K,10)

    df = DataFrame(
        id1 = sample(pool,N),
        id2 = sample(pool,N),
        id3 = sample(pool1,N),
        id4 = sample(1:K,N),
        id5 = sample(1:K,N),
        id6 = sample(1:(N/K),N),
        v1 = sample(1:5,N),
        v2 = sample(1:5,N),
        v3 = sample(rand(round.(rand(Uniform(0,100),100),4), N)))
    return df
end


function createSynDataFrame_categoricalarray(N::Int,K::Int)
    pool = "id".*dec.(1:K,3)
    pool1 = "id".*dec.(1:N÷K,10)

    df = DataFrame(
        id1 = sample(pool,N),
        id2 = sample(pool,N),
        id3 = sample(pool1,N),
        id4 = sample(1:K,N),
        id5 = sample(1:K,N),
        id6 = sample(1:(N/K),N),
        v1 = sample(1:5,N),
        v2 = sample(1:5,N),
        v3 = sample(rand(round.(rand(Uniform(0,100),100),4), N)))
    return df
end