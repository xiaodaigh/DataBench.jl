using Revise
using DataBench, RCall
using FastGroupBy
using IterableTables
using DataFrames
using CSV

function bench_gen_string_vec_var_len(n, strlen, skipbase = n >= 100_000_000)
    x = DataBench.gen_string_vec_var_len(n, strlen);
    bench_string_vec(x, n, strlen, "variable", skipbase)
end

function bench_gen_string_vec_fixed_len(n, strlen, skipbase = n >= 100_000_000)
    x = DataBench.gen_string_vec_fixed_len(n, strlen);
    bench_string_vec(x, n, strlen, "fixed", skipbase)
end

function bench_string_vec(x, n, strlen, strlentype, skipbase = n >= 100_000_000)
    if !skipbase
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

    copyx = copy(x)
    three_way_radix_qsort_elapsed = @elapsed three_way_radix_qsort!(copyx)
    @assert issorted(copyx)

    if skipbase
        DataFrame(
        test = ["julia_lsd_radixsort_elapsed", "r_radixsort_elapsed", "c_qsort_strcmp_elapsed", "three_way_radix_qsort_elapsed"]
        ,elapsed = [julia_lsd_radixsort_elapsed, r_radixsort_elapsed, c_qsort_strcmp_elapsed, three_way_radix_qsort_elapsed]
        ,n = [n for i = 1:4]
        ,strlen = [strlen for i = 1:4]
        ,strlentype = [strlentype for i = 1:4]
        ,grps = [n÷100 for i=1:4])
    else
        DataFrame(
            test = ["julia_sort_elapsed", "julia_lsd_radixsort_elapsed", "r_radixsort_elapsed", "c_qsort_strcmp_elapsed", "three_way_radix_qsort_elapsed"]
            ,elapsed = [julia_sort_elapsed, julia_lsd_radixsort_elapsed, r_radixsort_elapsed, c_qsort_strcmp_elapsed, three_way_radix_qsort_elapsed]
            ,n = [n for i = 1:5]
            ,strlen = [strlen for i = 1:5]
            ,strlentype = [strlentype for i = 1:5]
            ,grps = [n÷100 for i=1:5])
    end
end

# bench_gen_string_vec_var_len(1_000_000, 8)
# bench_gen_string_vec_var_len(1_000_000, 16)
# bench_gen_string_vec_var_len(1_000_000, 24)
# bench_gen_string_vec_var_len(1_000_000, 32)
# bench_gen_string_vec_var_len(1_000_000, 64)

# bench_gen_string_vec_fixed_len(1_000_000, 8)
# bench_gen_string_vec_fixed_len(1_000_000, 16)
# bench_gen_string_vec_fixed_len(1_000_000, 24)
# bench_gen_string_vec_fixed_len(1_000_000, 32)

ns = Int.(10.^(6:8))
strlens = 8.*(1:8)

function withrepeat(x,y)
    [repeat(x, inner=[size(y,1)]) repeat(y, outer=[size(x,1)])]
end

nsx = withrepeat(ns, strlens)

first = true
df = DataFrame()
for i in 1:size(nsx,1)
    n = nsx[i,1]
    strlen = nsx[i,2]
    df = bench_gen_string_vec_var_len(n, strlen)
    CSV.write("benchmark/bench_string_results/string_sort_perf $(replace(string(now()),":"," ")).csv", df);
    print(df)
    df = bench_gen_string_vec_fixed_len(n, strlen)
    print(df)
    CSV.write("benchmark/bench_string_results/string_sort_perf $(replace(string(now()),":"," ")).csv", df);
end


# save the results
# CSV.write("benchmark/bench_string_results/string_sort_perf $(replace(string(now()),":"," ")).csv", df);