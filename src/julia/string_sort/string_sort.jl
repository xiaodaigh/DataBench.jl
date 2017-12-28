# "Generate ASCII string vectors for sorting"
function gen_string_vec_fixed_len(n, strlen, grps = max(n ÷ 100,1), range = vcat(48:57,65:90,97:122))    
    rand([string(rand(Char.(range), strlen)...) for k in 1:grps], n)
end

function gen_string_vec_var_len(n, strlen, grps = max(n ÷ 100,1), range = vcat(48:57,65:90,97:122))    
    rand([string(rand(Char.(range), rand(1:strlen))...) for k in 1:grps], n)
end