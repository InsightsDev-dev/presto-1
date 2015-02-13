package com.mobileum.range;

import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.util.DateTimeUtils;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class TimeStamp
        implements Comparable<TimeStamp>
{
    public static final int SIZE = Long.SIZE;
    private long timestamp;

    public long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public TimeStamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public static TimeStamp parseLocalTime(TimeZoneKey timeZoneKey, String value)
    {
        return new TimeStamp(DateTimeUtils.parseTimestampWithoutTimeZone(timeZoneKey, value));
    }

    public static TimeStamp parseMillisUtc(String value)
    {
        return new TimeStamp(DateTimeUtils.parseTimestampWithoutTimeZone(TimeZoneKey.UTC_KEY, value));
    }

    @Override
    public int compareTo(TimeStamp other)
    {
        long x = this.timestamp;
        long y = other.timestamp;
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    @Override
    public int hashCode()
    {
        long x = this.timestamp;
        return (int) (x ^ (x >>> 32));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (obj instanceof TimeStamp) {
            long x = this.timestamp;
            long y = ((TimeStamp) obj).timestamp;
            return x == y;
        }
        return false;
    }

    public String toString(TimeZoneKey timeZoneKey)
    {
        return DateTimeUtils.printTimestampWithoutTimeZone(timeZoneKey, timestamp);
    }

    public String toString()
    {
        return DateTimeUtils.printTimestampWithoutTimeZone(TimeZoneKey.UTC_KEY, timestamp);
    }
}
