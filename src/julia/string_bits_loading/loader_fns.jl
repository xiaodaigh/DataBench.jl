
function leading_bits(s::String)
    x = UInt(0)
    for i = 1:min(sizeof(x), sizeof(s))
        @inbounds x |= UInt(codeunit(s, i)) << ((sizeof(x) - i) * 8)
    end
    return x
end


function leading_bits_with_fast_path(s::String)
    if sizeof(s) == 8
        return ntoh(unsafe_load(Ptr{UInt64}(pointer(s))))
    end

    x = UInt(0)
    for i = 1:min(sizeof(x), sizeof(s))
        @inbounds x |= UInt(codeunit(s, i)) << ((sizeof(x) - i) * 8)
    end
    return x
end

function load_bits_with_padding(s::String, skipbytes = 0)  
    n = sizeof(s)    
    remaining_bytes_to_load = min(sizeof(UInt), n)
    # start
    res = zero(UInt)
    shift_for_padding = 64

    if remaining_bytes_to_load == 8
        res = ntoh(unsafe_load(Ptr{UInt64}(pointer(s, skipbytes+1))))
    else 
        if  remaining_bytes_to_load >= 4
            res |= Base.zext_int(UInt, ntoh(unsafe_load(Ptr{UInt32}(pointer(s, skipbytes+1))))) << (shift_for_padding - 32)
            skipbytes += 4
            remaining_bytes_to_load -= 4
            shift_for_padding -= 32
        end
        if  remaining_bytes_to_load >= 2
            res |= Base.zext_int(UInt, ntoh(unsafe_load(Ptr{UInt16}(pointer(s, skipbytes+1))))) << (shift_for_padding - 16)
            skipbytes += 2
            remaining_bytes_to_load -= 2
            shift_for_padding -= 16
        end
        if  remaining_bytes_to_load >= 1
            res |= Base.zext_int(UInt, ntoh(unsafe_load(Ptr{UInt8}(pointer(s, skipbytes+1))))) << (shift_for_padding - 8)
            skipbytes += 1
            remaining_bytes_to_load -= 1
            shift_for_padding -= 8
        end
    end

    return res
end

mutable struct MutableUInt val::UInt end

import Base: unsafe_copy!, unsafe_copyto!
if !isdefined(Symbol("unsafe_copyto!"))
    unsafe_copyto! = unsafe_copy!
end

function copy_bits_fast_path(s::String, skipbytes = 0)  
    n = sizeof(s)    
    remaining_bytes_to_load = min(sizeof(UInt), n)
    # start
   
    if remaining_bytes_to_load == 8
        return ntoh(unsafe_load(Ptr{UInt64}(pointer(s))))
    else 
        res = MutableUInt(0)
        p=reinterpret(Ptr{UInt8}, pointer_from_objref(res))
        if  remaining_bytes_to_load >= 4
            unsafe_copyto!(p, pointer(s), 4)
            skipbytes += 4
            remaining_bytes_to_load -= 4
        end
        if  remaining_bytes_to_load >= 2
            unsafe_copyto!(p+skipbytes, pointer(s, skipbytes+1),2)
            skipbytes += 2
            remaining_bytes_to_load -= 2
        end
        if  remaining_bytes_to_load >= 1
            unsafe_copyto!(p+skipbytes, pointer(s, skipbytes+1),1)
        end
    end

    ntoh(res.val)
end

# all char specialisation
primitive type Bits24 24 end
primitive type Bits40 40 end
primitive type Bits48 48 end
primitive type Bits56 56 end

function all_lens(s::String, skipbytes = 0)::UInt
    n = sizeof(s)    
    remaining_bytes_to_load = min(sizeof(UInt), n)

    if remaining_bytes_to_load == 8
        return ntoh(unsafe_load(Ptr{UInt64}(pointer(s))))
    elseif  remaining_bytes_to_load == 7
        return ntoh(Base.zext_int(UInt, unsafe_load(Ptr{Bits56}(s |> pointer))))
    elseif  remaining_bytes_to_load == 6
        return ntoh(Base.zext_int(UInt, unsafe_load(Ptr{Bits48}(s |> pointer))))
    elseif  remaining_bytes_to_load == 5
        return ntoh(Base.zext_int(UInt, unsafe_load(Ptr{Bits40}(s |> pointer))))
    elseif  remaining_bytes_to_load == 4
        return ntoh(Base.zext_int(UInt, unsafe_load(Ptr{UInt}(s |> pointer))))
    elseif  remaining_bytes_to_load == 3
        return ntoh(Base.zext_int(UInt, unsafe_load(Ptr{Bits24}(s |> pointer))))
    elseif  remaining_bytes_to_load == 2
        return ntoh(Base.zext_int(UInt, unsafe_load(Ptr{UInt16}(s |> pointer))))
    else
        return ntoh(Base.zext_int(UInt, unsafe_load(Ptr{UInt8}(s |> pointer))))
    end
end

function check_boundary(s::String)::UInt
    ptrs  = pointer(s)
    if (UInt(ptrs) & 0xfff) > 0xff8
        return all_lens(s)
    else
        return ntoh(unsafe_load(Ptr{UInt64}(ptrs)))
    end 
end

unsafe_loadbits(s) = s |> pointer |> Ptr{UInt} |> unsafe_load |> ntoh
leading_bitsdot(s) = leading_bits.(s)
leading_bits_with_fast_pathdot(s) = leading_bits_with_fast_path.(s)
unsafe_loadbitsdot(s) = unsafe_loadbits.(s)
load_bits_with_paddingdot(s) = load_bits_with_padding.(s)
copy_bits_fast_pathdot(s) = copy_bits_fast_path.(s)
all_lensdot(s) = all_lens.(s)
check_boundarydot(s) = check_boundary.(s)



