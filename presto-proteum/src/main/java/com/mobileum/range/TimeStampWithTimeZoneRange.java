package com.mobileum.range;

import com.facebook.presto.spi.ConnectorSession;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class TimeStampWithTimeZoneRange
        extends Range<TimeStampWithTimeZone>
{
    public static final TimeStampWithTimeZoneRange emptyRange = new TimeStampWithTimeZoneRange();

    public TimeStampWithTimeZoneRange(RangeBound<TimeStampWithTimeZone> lower, RangeBound<TimeStampWithTimeZone> upper, byte flags)
    {
        super(lower, upper, flags);
    }

    public TimeStampWithTimeZoneRange()
    {
        super();
    }

    @Override
    public Domain<TimeStampWithTimeZone> getRangeDomain()
    {
        return Domain.TimeStampWithTimeZoneLongDomain.INSTANCE;
    }

    @Override
    public Range<TimeStampWithTimeZone> custructRange(RangeBound<TimeStampWithTimeZone> lower, RangeBound<TimeStampWithTimeZone> upper, byte flags)
    {
        return new TimeStampWithTimeZoneRange(lower, upper, flags);
    }

    //parses string With TimeZone
    @Override
    public TimeStampWithTimeZone parseValue(String value)
    {
        return TimeStampWithTimeZone.parse(value);
    }
    //parse string without timezone 
    //Todo
    @Override
    public TimeStampWithTimeZone parseValue(String value,ConnectorSession session)
    {
        return TimeStampWithTimeZone.parse(value);
    }

    @Override
    public TimeStampWithTimeZone parseValue(Slice value, Pointer index)
    {
    	TimeStampWithTimeZone ret=new TimeStampWithTimeZone(value.getLong(index.getPointer()));
        index.increment(TimeStampWithTimeZone.SIZE / Byte.SIZE);
        return ret;
    }

    @Override
    public Slice getValueAsSlice(TimeStampWithTimeZone value)
    {
        Slice slice = Slices.allocate(TimeStampWithTimeZone.SIZE / Byte.SIZE);
        slice.setLong(0, value.getTimestamp());
        return slice;
    }

    @Override
    public String getValueAsString(TimeStampWithTimeZone value)
    {
        return value.toString();
    }

    public String getValueAsString(ConnectorSession session, TimeStampWithTimeZone value)
    {
        return value.toString();
    }

    @Override
    public RangeSerializer<TimeStampWithTimeZone> getSerializer()
    {
        return emptyRange;
    }

    @Override
    public Range<TimeStampWithTimeZone> getEmptyRange()
    {
        return emptyRange;
    }
}
