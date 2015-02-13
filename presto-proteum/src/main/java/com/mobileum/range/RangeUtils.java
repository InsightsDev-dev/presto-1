package com.mobileum.range;

import static com.mobileum.range.IRange.RANGE_EMPTY;
import static com.mobileum.range.IRange.RANGE_LB_INC;
import static com.mobileum.range.IRange.RANGE_LB_INF;
import static com.mobileum.range.IRange.RANGE_LB_NULL;
import static com.mobileum.range.IRange.RANGE_UB_INC;
import static com.mobileum.range.IRange.RANGE_UB_INF;
import static com.mobileum.range.IRange.RANGE_UB_NULL;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class RangeUtils
{
    //Todo verify
    public static boolean hasLowerBound(byte flags)
    {
        return (flags & (RANGE_EMPTY | RANGE_LB_NULL | RANGE_LB_INF)) == 0;
    }

    //Todo verify
    public static boolean hasUpperBound(byte flags)
    {
        return (flags & (RANGE_EMPTY | RANGE_UB_NULL | RANGE_UB_INF)) == 0;
    }

    public static boolean isEmpty(byte flags)
    {
        return (flags & RANGE_EMPTY) != 0;
    }

    public static boolean isLowerInc(byte flags)
    {
        return (flags & RANGE_LB_INC) != 0;
    }

    public static boolean isUpperInc(byte flags)
    {
        return (flags & RANGE_UB_INC) != 0;
    }

    public static boolean isLowerInf(byte flags)
    {
        return (flags & RANGE_LB_INF) != 0;
    }

    public static boolean isUpperInF(byte flags)
    {
        return (flags & RANGE_UB_INF) != 0;
    }
}
