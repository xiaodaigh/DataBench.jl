"""
    gen_string_vec_fixed_len(1_000_000, 16)

Generate ASCII string vectors of FIXED lengths

n: length of output vector
strlen: length of each string
grps: number of distinct strings
range: The Char.(range) to choose the letters from
"""

function gen_string_vec_fixed_len(n, strlen, grps = max(n ÷ 100,1), range = vcat(48:57,65:90,97:122))    
    rand([string(rand(Char.(range), strlen)...) for k in 1:grps], n)
end

"""
    gen_string_vec_var_len(1_000_000, 16)

Generate ASCII string vectors of VARIABLE lengths

n: length of output vector
strlen: maximum length of each string; the strings' length varies from 1 to strlen
grps: number of distinct strings
range: The Char.(range) to choose the letters from
"""
function gen_string_vec_var_len(n, strlen, grps = max(n ÷ 100,1), range = vcat(48:57,65:90,97:122))    
    rand([string(rand(Char.(range), rand(1:strlen))...) for k in 1:grps], n)
end


"""
    gen_string_vec_id_fixed_len(1_000_000, 10)

Generate ASCII string with a fixed prefix followed by a number of strlen. So the total length of the generate string is `length(prefix) + strlen` not `strlen`

n: length of output vector
strlen: maximum length of each string; the strings' length varies from 1 to strlen
grps: number of distinct strings
prefix: the prefix in the string; defaults to "id"
"""
function gen_string_vec_id_fixed_len(n, strlen = 10, grps = max(n ÷ 100,1), prefix = "id")
    rand([prefix*dec(k,strlen) for k in 1:grps], n)
end