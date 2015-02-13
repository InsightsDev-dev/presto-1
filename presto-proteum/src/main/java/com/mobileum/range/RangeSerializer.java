package com.mobileum.range;

import com.facebook.presto.spi.ConnectorSession;

import io.airlift.slice.Slice;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public interface RangeSerializer<T extends Comparable<T>>
{
    //parsing from string
    public Range<T> parse(String range);

    //representing as String
    public String getRangeAsString();

    public String getRangeAsString(ConnectorSession session);

    //deserializing: geting from disk
    public Range<T> parse(Slice range);

    //serializing: persisting to disk
    public Slice getRangeAsSlice();

}
