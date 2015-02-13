package com.mobileum.range;

import java.io.Serializable;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public abstract class Domain<T>
{
    abstract public T minValue();

    abstract public T maxValue();

    public static final class TimeStampWithTimeZoneLongDomain
            extends Domain<TimeStampWithTimeZone>
            implements Serializable
    {
        public static final TimeStampWithTimeZoneLongDomain INSTANCE = new TimeStampWithTimeZoneLongDomain();

        @Override
        public TimeStampWithTimeZone minValue()
        {
            return new TimeStampWithTimeZone(Long.MIN_VALUE);
        }

        @Override
        public TimeStampWithTimeZone maxValue()
        {
            return new TimeStampWithTimeZone(Long.MAX_VALUE);
        }

        public Object readResolve()
        {
            return INSTANCE;
        }

        @Override
        public String toString()
        {
            return "DiscreteDomain.longs()";
        }

        private static final long serialVersionUID = 0;
    }

    public static final class TimeStampLongDomain
            extends Domain<TimeStamp>
            implements Serializable
    {
        public static final TimeStampLongDomain INSTANCE = new TimeStampLongDomain();

        @Override
        public TimeStamp minValue()
        {
            return new TimeStamp(Long.MIN_VALUE);
        }

        @Override
        public TimeStamp maxValue()
        {
            return new TimeStamp(Long.MAX_VALUE);
        }

        public Object readResolve()
        {
            return INSTANCE;
        }

        @Override
        public String toString()
        {
            return "DiscreteDomain.longs()";
        }

        private static final long serialVersionUID = 0;
    }
}
