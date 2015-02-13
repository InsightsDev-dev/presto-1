package com.mobileum.range;

import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;

import com.facebook.presto.util.DateTimeUtils;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class TimeStampWithTimeZone
        implements Comparable<TimeStampWithTimeZone>
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

    public TimeStampWithTimeZone(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public static TimeStampWithTimeZone parse(String value)
    {
        return new TimeStampWithTimeZone(DateTimeUtils.parseTimestampWithTimeZone(value));
    }

    @Override
    public int compareTo(TimeStampWithTimeZone other)
    {
        long x = unpackMillisUtc(this.timestamp);
        long y = unpackMillisUtc(other.timestamp);
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    @Override
    public int hashCode()
    {
        long x = unpackMillisUtc(this.timestamp);
        return (int) (x ^ (x >>> 32));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (obj instanceof TimeStampWithTimeZone) {
            long x = unpackMillisUtc(this.timestamp);
            long y = unpackMillisUtc(((TimeStampWithTimeZone) obj).timestamp);
            return x == y;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return DateTimeUtils.printTimestampWithTimeZone(this.timestamp);
    }

}
