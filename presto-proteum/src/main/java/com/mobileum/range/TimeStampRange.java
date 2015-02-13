package com.mobileum.range;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.TimeZoneKey;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class TimeStampRange
        extends Range<TimeStamp>
{
    public static final TimeStampRange emptyRange = new TimeStampRange(null, null, true);

    public TimeStampRange(RangeBound<TimeStamp> lower, RangeBound<TimeStamp> upper, byte flags)
    {
        super(lower, upper, flags);
    }

    public TimeStampRange(RangeBound<TimeStamp> lower, RangeBound<TimeStamp> upper, boolean isEmpty)
    {
        super(lower, upper, isEmpty);
    }

    @Override
    public Domain<TimeStamp> getRangeDomain()
    {
        return Domain.TimeStampLongDomain.INSTANCE;
    }

    @Override
    public Range<TimeStamp> custructRange(RangeBound<TimeStamp> lower, RangeBound<TimeStamp> upper, byte flags)
    {
        return new TimeStampRange(lower, upper, flags);
    }

    //parse time using UTC
    @Override
    public TimeStamp parseValue(String value)
    {
        return TimeStamp.parseMillisUtc(value);
    }

    @Override
    public TimeStamp parseValue(Slice value, int index)
    {
        increasePointer(TimeStamp.SIZE / Byte.SIZE);
        return new TimeStamp(value.getLong(index));
    }

    @Override
    public Slice getValueAsSlice(TimeStamp value)
    {
        Slice slice = Slices.allocate(TimeStamp.SIZE / Byte.SIZE);
        slice.setLong(0, value.getTimestamp());
        return slice;
    }

    @Override
    public String getValueAsString(TimeStamp value)
    {
        return value.toString();
    }

    @Override
    public String getValueAsString(ConnectorSession session, TimeStamp value)
    {
        return value.toString(session.getTimeZoneKey());
    }

    @Override
    public RangeSerializer<TimeStamp> getSerializer()
    {
        return emptyRange;
    }

    @Override
    public Range<TimeStamp> getEmptyRange()
    {
        return emptyRange;
    }
}
