package com.mobileum.range.presto;

import org.apache.commons.codec.binary.Base64;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import com.mobileum.range.IntegerRange;
import com.mobileum.range.RangeSerializer;
import com.mobileum.range.TimeStampWithTimeZone;
import com.mobileum.range.TimeStampWithTimeZoneRange;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class Int4Range
{

    static RangeSerializer<Integer> rangeSerializer = IntegerRange.emptyRange.getSerializer();

    public static IntegerRange deSerialize(Slice slice)
    {
        byte[] arr = slice.getBytes();
        byte[] decoded = Base64.decodeBase64(arr);
        Slice slice2 = Slices.allocate(decoded.length);
        slice2.setBytes(0, decoded);
        return (IntegerRange) rangeSerializer.parse(slice2);
    }

    public static IntegerRange createRange(long min, long max)
    {
        return (IntegerRange) rangeSerializer.parse("[" + min + "," + max + ")");
    }

    public static IntegerRange createRange(long min, long max, String str)
    {
        return (IntegerRange) rangeSerializer.parse(str.substring(0, 1) + min + "," + max + str.substring(1));
    }
    public static Slice serialize(com.mobileum.range.Range<Integer> range)
    {
        Slice s = range.getRangeAsSlice();
        byte arr[] = s.getBytes();
        byte[] encoded = Base64.encodeBase64(arr);
        Slice slice2 = Slices.allocate(encoded.length);
        slice2.setBytes(0, encoded);
        return slice2;
    }
}
