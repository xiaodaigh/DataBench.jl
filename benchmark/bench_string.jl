using Revise
using DataBench, RCall
using FastGroupBy
using IterableTables
using DataFrames
using CSV


function bench_gen_string_vec_var_len(n, strlen, skipbase = true)
    x = DataBench.gen_string_vec_var_len(n, strlen);

    if skipbase
        copyx = copy(x);
        julia_sort_elapsed = @elapsed sort!(copyx);
        @assert issorted(copyx)
    end

    copyx = copy(x);
    julia_lsd_radixsort_elapsed = @elapsed radixsort_lsd!(copyx);
    @assert issorted(copyx)


    copyx = copy(x);
    R"""
    memory.limit(2^32-1)
    rcopyx = $copyx
    summary(rcopyx)
    pt = proc.time()
    sort(rcopyx, method="radix")
    pt2 = proc.time()
    """;

    @rget pt;
    @rget pt2;

    r_radixsort_elapsed = pt2[3] - pt[3]


    c_qsort_strcmp_elapsed = @elapsed str_qsort!(copyx)
    @assert issorted(copyx)

    if skipbase
        DataFrame(
        test = ["julia_lsd_radixsort_elapsed", "r_radixsort_elapsed", "c_qsort_strcmp_elapsed"]
        ,elapsed = [julia_lsd_radixsort_elapsed, r_radixsort_elapsed, c_qsort_strcmp_elapsed]
        ,n = [n for i = 1:3]
        ,strlen = [strlen for i = 1:3]
        ,strlentype = ["variable" for i = 1:3]
        ,grps = [n÷100 for i=1:3])
    else
        DataFrame(
            test = ["julia_sort_elapsed", "julia_lsd_radixsort_elapsed", "r_radixsort_elapsed", "c_qsort_strcmp_elapsed"]
            ,elapsed = [julia_sort_elapsed, julia_lsd_radixsort_elapsed, r_radixsort_elapsed, c_qsort_strcmp_elapsed]
            ,n = [n for i = 1:4]
            ,strlen = [strlen for i = 1:4]
            ,strlentype = ["variable" for i = 1:4]
            ,grps = [n÷100 for i=1:4])
    end
end

function bench_gen_string_vec_fixed_len(n, strlen, skipbase = true)
    x = DataBench.gen_string_vec_fixed_len(n, strlen);
    if skipbase
        copyx = copy(x);
        julia_sort_elapsed = @elapsed sort!(copyx);
        @assert issorted(copyx)
    end

    copyx = copy(x);
    julia_lsd_radixsort_elapsed = @elapsed radixsort_lsd!(copyx);
    @assert issorted(copyx)


    copyx = copy(x);
    R"""
    memory.limit(2^32-1)
    rcopyx = $copyx
    summary(rcopyx)
    pt = proc.time()
    sort(rcopyx, method="radix")
    pt2 = proc.time()
    """;

    @rget pt;
    @rget pt2;

    r_radixsort_elapsed = pt2[3] - pt[3]


    c_qsort_strcmp_elapsed = @elapsed str_qsort!(copyx)
    @assert issorted(copyx)

    if skipbase
        DataFrame(
            test = ["julia_lsd_radixsort_elapsed", "r_radixsort_elapsed", "c_qsort_strcmp_elapsed"]
            ,elapsed = [ julia_lsd_radixsort_elapsed, r_radixsort_elapsed, c_qsort_strcmp_elapsed]
            ,n = [n for i = 1:3]
            ,strlen = [strlen for i = 1:3]
            ,strlentype = ["fixed" for i = 1:3]
            ,grps = [n÷100 for i=1:3])
    else
        DataFrame(
            test = ["julia_sort_elapsed", "julia_lsd_radixsort_elapsed", "r_radixsort_elapsed", "c_qsort_strcmp_elapsed"]
            ,elapsed = [julia_sort_elapsed, julia_lsd_radixsort_elapsed, r_radixsort_elapsed, c_qsort_strcmp_elapsed]
            ,n = [n for i = 1:4]
            ,strlen = [strlen for i = 1:4]
            ,strlentype = ["fixed" for i = 1:4]
            ,grps = [n÷100 for i=1:4])
    end
end

# bench_gen_string_vec_var_len(1_000_000, 8)
# bench_gen_string_vec_var_len(1_000_000, 16)
# bench_gen_string_vec_var_len(1_000_000, 24)
# bench_gen_string_vec_var_len(1_000_000, 32)

# bench_gen_string_vec_fixed_len(1_000_000, 8)
# bench_gen_string_vec_fixed_len(1_000_000, 16)
# bench_gen_string_vec_fixed_len(1_000_000, 24)
# bench_gen_string_vec_fixed_len(1_000_000, 32)

ns = Int.(10.^(6:8))
strlens = 8.*(1:4)

function withrepeat(x,y)
    [repeat(x, inner=[size(y,1)]) repeat(y, outer=[size(x,1)])]
end

nsx = withrepeat(ns, strlens)

first = true
for i in 1:size(nsx,1)
    n = nsx[i,1]
    strlen = nsx[i,2]
    if first
        df = bench_gen_string_vec_var_len(n, strlen)
        first = false
    else
        df = vcat(df, bench_gen_string_vec_var_len(n, strlen))
    end
    print(df)
    df = vcat(df, bench_gen_string_vec_fixed_len(n, strlen))
    print(df)
end
df

# save the results
CSV.write("benchmark/bench_string_results/string_sort_perf $(replace(string(now()),":"," ")).csv", df);
