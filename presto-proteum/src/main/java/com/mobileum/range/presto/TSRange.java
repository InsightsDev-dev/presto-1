package com.mobileum.range.presto;

import org.apache.commons.codec.binary.Base64;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import com.mobileum.range.Range;
import com.mobileum.range.RangeSerializer;
import com.mobileum.range.TimeStamp;
import com.mobileum.range.TimeStampRange;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class TSRange
{

    static RangeSerializer<TimeStamp> rangeSerializer = TimeStampRange.emptyRange.getSerializer();

    public static TimeStampRange deSerialize(Slice slice)
    {
        byte[] arr = slice.getBytes();
        byte[] decoded = Base64.decodeBase64(arr);
        Slice slice2 = Slices.allocate(decoded.length);
        slice2.setBytes(0, decoded);
        return (TimeStampRange) rangeSerializer.parse(slice2);
    }
//takes timestamp in utc and where new TimeStamp(min) gives timestamp in utc hence parsing works as ok
//but for direct parsing should pass session and then convert and while retriving so too.
    public static TimeStampRange createRange(long min, long max)
    {
        return (TimeStampRange) rangeSerializer.parse("[" + new TimeStamp(min) + "," + new TimeStamp(max) + ")");
    }

    public static TimeStampRange createRange(long min, long max, String str)
    {
        return (TimeStampRange) rangeSerializer.parse(str.substring(0, 1) + new TimeStamp(min) + "," + new TimeStamp(max) + str.substring(1));
    }

    //parse from utc time.not handles local time with session, be careful.
	public static TimeStampRange createRange(String timeRange) 
	{
		return (TimeStampRange) rangeSerializer.parse(timeRange);
	}
    public static Slice serialize(Range<TimeStamp> range)
    {
        Slice s = range.getRangeAsSlice();
        byte arr[] = s.getBytes();
        byte[] encoded = Base64.encodeBase64(arr);
        Slice slice2 = Slices.allocate(encoded.length);
        slice2.setBytes(0, encoded);
        return slice2;
    }
}
