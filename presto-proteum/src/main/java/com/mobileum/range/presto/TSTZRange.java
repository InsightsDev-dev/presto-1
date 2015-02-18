package com.mobileum.range.presto;


import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import com.google.common.io.BaseEncoding;
import com.mobileum.range.Range;
import com.mobileum.range.RangeSerializer;
import com.mobileum.range.TimeStampWithTimeZone;
import com.mobileum.range.TimeStampWithTimeZoneRange;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class TSTZRange
{

    static RangeSerializer<TimeStampWithTimeZone> rangeSerializer = TimeStampWithTimeZoneRange.emptyRange.getSerializer();

    public static TimeStampWithTimeZoneRange deSerialize(Slice slice)
    {
        byte[] decoded = BaseEncoding.base64().decode(slice.toStringUtf8());
        Slice slice2 = Slices.allocate(decoded.length);
        slice2.setBytes(0, decoded);
        return (TimeStampWithTimeZoneRange) rangeSerializer.parse(slice2);
    }

    public static TimeStampWithTimeZoneRange createRange(long min, long max)
    {
        return (TimeStampWithTimeZoneRange) rangeSerializer.parse("[" + new TimeStampWithTimeZone(min) + "," + new TimeStampWithTimeZone(max) + ")");
    }

    public static TimeStampWithTimeZoneRange createRange(long min, long max, String str)
    {
        return (TimeStampWithTimeZoneRange) rangeSerializer.parse(str.substring(0, 1) + new TimeStampWithTimeZone(min) + "," + new TimeStampWithTimeZone(max) + str.substring(1));
    }

    public static Slice serialize(Range<TimeStampWithTimeZone> range)
    {
        Slice s = range.getRangeAsSlice();
        Slice slice2 = Slices.utf8Slice(BaseEncoding.base64().encode(s.getBytes()));
        return slice2;
    }
}
