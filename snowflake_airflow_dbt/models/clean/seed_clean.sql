select

    a,
    s,
    h,
    d,
    k,
    f,
    a+s+h+d as ashd_sum


from {{ref('random')}}