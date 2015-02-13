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
    public static final TimeStampWithTimeZoneRange emptyRange = new TimeStampWithTimeZoneRange(null, null, true);

    public TimeStampWithTimeZoneRange(RangeBound<TimeStampWithTimeZone> lower, RangeBound<TimeStampWithTimeZone> upper, byte flags)
    {
        super(lower, upper, flags);
    }

    public TimeStampWithTimeZoneRange(RangeBound<TimeStampWithTimeZone> lower, RangeBound<TimeStampWithTimeZone> upper, boolean isEmpty)
    {
        super(lower, upper, isEmpty);
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

    @Override
    public TimeStampWithTimeZone parseValue(Slice value, int index)
    {
        increasePointer(TimeStampWithTimeZone.SIZE / Byte.SIZE);
        return new TimeStampWithTimeZone(value.getLong(index));
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
