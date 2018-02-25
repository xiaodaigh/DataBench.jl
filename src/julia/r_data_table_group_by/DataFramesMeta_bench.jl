
function DfMeta_benches(df::DataFrame)

    # timings
    ti = Dict()

    ti[:sum1] = @elapsed @linq df |>
                    @by(:id1,r = sum(:v1))

    ti[:sum2] = @elapsed @linq df |>
                    @by(:id1,r = sum(:v1))

    ti[:sum3] = @elapsed @linq df |>
                    @by([:id1,:id2],r = sum(:v1))

    ti[:sum4] = @elapsed @linq df |>
                    @by([:id1,:id2],r = sum(:v1))

    ti[:sum_mean1] = @elapsed @linq df |>
                    @by(:id3,s = sum(:v1),m=mean(:v1))

    ti[:sum_mean2] = @elapsed @linq df |>
                    @by(:id3,s = sum(:v1),m=mean(:v1))

    ti[:mean7_9_by_id4_1] = @elapsed @linq df |>
                        @by(:id4,m7=mean(:v1),m8=mean(:v2),m9=mean(:v3))

    ti[:mean7_9_by_id4_2] = @elapsed @linq df |>
                        @by(:id4,m7=mean(:v1),m8=mean(:v2),m9=mean(:v3))

    ti[:sum7_9_by_id6_1] = @elapsed @linq df |>
                        @by(:id6,m7=mean(:v1),m8=mean(:v2),m9=mean(:v3))

    ti[:sum7_9_by_id6_2] = @elapsed @linq df |>
                        @by(:id6,m7=mean(:v1),m8=mean(:v2),m9=mean(:v3))

    return ti
end