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
    
    public T parseValue(String value,ConnectorSession session);

    public String getValueAsString(T value);

    public String getValueAsString(ConnectorSession session, T value);

    public T parseValue(Slice value, Pointer index);

    public Slice getValueAsSlice(T value);

    //all uses
    public Range<T> custructRange(RangeBound<T> lower, RangeBound<T> upper, byte flag);
    class Pointer
    {
    	int pointer;

		public Pointer(int pointer) 
		{
			this.pointer = pointer;
		}

		public int getPointer() 
		{
			return pointer;
		}

		public void increment(int pointer) 
		{
			this.pointer += pointer;
		}
    	
    }
}
