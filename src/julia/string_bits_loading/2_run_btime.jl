include("src/julia/string_bits_loading/loader_fns.jl")
xvar = [randstring(rand(1:8)) for i in 1:100_000];
x = rand(xvar, 10_000_000);
using BenchmarkTools
# @btime leading_bitsdot($x);
# @btime leading_bits_with_fast_pathdot($x);
@btime unsafe_loadbitsdot($x);          # 70ms
@btime load_bits_with_paddingdot($x);   # 240ms
# @btime copy_bits_fast_pathdot($x)     # can't test 
@btime all_lensdot($x);                 # 190ms
@btime check_boundarydot($x);           # 72ms

xfixed = [randstring(8) for i in 1:100_000];
x = rand(xfixed, 10_000_000);
using BenchmarkTools
# @btime leading_bitsdot($x);
# @btime leading_bits_with_fast_pathdot($x);
@btime unsafe_loadbitsdot($x);          # 67ms
@btime load_bits_with_paddingdot($x);   # 78ms
# @btime copy_bits_fast_pathdot($x)     # can't test
@btime all_lensdot($x);                 # 81ms
@btime check_boundarydot($x);           # 74ms


##########################################################################
# Larger tests
######################

xvar = [randstring(rand(1:8)) for i in 1:1_000_000];
x = rand(xvar, 100_000_000);
using BenchmarkTools
# @btime leading_bitsdot($x);
# @btime leading_bits_with_fast_pathdot($x);
@btime unsafe_loadbitsdot($x);          # 70ms
# @btime load_bits_with_paddingdot($x);   # 240ms
# @btime copy_bits_fast_pathdot($x)     # can't test 
# @btime all_lensdot($x);                 # 190ms
@btime check_boundarydot($x);           # 72ms
