package com.mobileum.range;

import com.facebook.presto.spi.ConnectorSession;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class IntegerRange
        extends Range<Integer>
{
    public static IntegerRange emptyRange = new IntegerRange();

    public IntegerRange(RangeBound<Integer> lower, RangeBound<Integer> upper, byte flags)
    {
        super(lower, upper, flags);
    }

    public IntegerRange()
    {
        super();
    }

    @Override
    public Domain<Integer> getRangeDomain()
    {
        return DiscreteDomain.integers();
    }

    @Override
    public Range<Integer> custructRange(RangeBound<Integer> lower, RangeBound<Integer> upper, byte flags)
    {
        return new IntegerRange(lower, upper, flags);
    }

    @Override
    public Integer parseValue(String value)
    {
        return Integer.parseInt(value);
    }
    
    @Override
    public Integer parseValue(String value,ConnectorSession session)
    {
        return Integer.parseInt(value);
    }

    @Override
    public Integer parseValue(Slice value, Pointer index)
    {
    	Integer ret=value.getInt(index.getPointer());
    	index.increment(Integer.SIZE / Byte.SIZE);
        return ret;
    }

    @Override
    public Slice getValueAsSlice(Integer value)
    {
        Slice slice = Slices.allocate(Integer.SIZE / Byte.SIZE);
        slice.setInt(0, value);
        return slice;
    }

    @Override
    public String getValueAsString(Integer value)
    {
        return value.toString();
    }

    public String getValueAsString(ConnectorSession session, Integer value)
    {
        return value.toString();
    }

    @Override
    public RangeSerializer<Integer> getSerializer()
    {
        return emptyRange;
    }

    @Override
    public final Range<Integer> getEmptyRange()
    {
        return new IntegerRange();
    }

}
