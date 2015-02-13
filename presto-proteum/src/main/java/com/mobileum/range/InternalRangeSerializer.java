package com.mobileum.range;

import com.facebook.presto.spi.ConnectorSession;

import io.airlift.slice.Slice;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public interface InternalRangeSerializer<T extends Comparable<T>>
        extends RangeSerializer<T>
{
    public T parseValue(String value);

    public String getValueAsString(T value);

    public String getValueAsString(ConnectorSession session, T value);

    public T parseValue(Slice value, int index);

    public Slice getValueAsSlice(T value);

    //all uses
    public Range<T> custructRange(RangeBound<T> lower, RangeBound<T> upper, byte flag);
}
