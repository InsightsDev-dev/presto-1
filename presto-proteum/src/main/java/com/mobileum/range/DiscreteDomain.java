package com.mobileum.range;

import java.io.Serializable;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public abstract class DiscreteDomain<T extends Comparable<T>>
        extends Domain<T>
{
    //    private static final IntegerDomain INSTANCE = new IntegerDomain();
//    private Object readResolve() {
//        return INSTANCE;
//      }

    abstract public T next(T value);

    abstract public T previous(T value);

    abstract public long distance(T start, T end);

    public static DiscreteDomain<Integer> integers()
    {
        return IntegerDomain.INSTANCE;
    }

    private static final class IntegerDomain
            extends DiscreteDomain<Integer>
            implements Serializable
    {
        private static final IntegerDomain INSTANCE = new IntegerDomain();

        @Override
        public Integer next(Integer value)
        {
            int i = value;
            if (i == Integer.MAX_VALUE) {
                throw new RuntimeException("integer out of range");
            }
            else {
                return i + 1;
            }
        }

        @Override
        public Integer previous(Integer value)
        {
            int i = value;
            if (i == Integer.MIN_VALUE) {
                throw new RuntimeException("integer out of range");
            }
            else {
                return i - 1;
            }
        }

        @Override
        public long distance(Integer start, Integer end)
        {
            return (long) end - start;
        }

        @Override
        public Integer minValue()
        {
            return Integer.MIN_VALUE;
        }

        @Override
        public Integer maxValue()
        {
            return Integer.MAX_VALUE;
        }

        private Object readResolve()
        {
            return INSTANCE;
        }

        @Override
        public String toString()
        {
            return "DiscreteDomain.integers()";
        }

        private static final long serialVersionUID = 0;
    }

    private static final class LongDomain
            extends DiscreteDomain<Long>
            implements Serializable
    {
        private static final LongDomain INSTANCE = new LongDomain();

        @Override
        public Long next(Long value)
        {
            long l = value;
            if (l == Long.MAX_VALUE) {
                throw new RuntimeException("long out of range");
            }
            else {
                return l + 1;
            }
        }

        @Override
        public Long previous(Long value)
        {
            long l = value;
            if (l == Long.MIN_VALUE) {
                throw new RuntimeException("long out of range");
            }
            else {
                return l - 1;
            }
        }

        @Override
        public long distance(Long start, Long end)
        {
            long result = end - start;
            if (end > start && result < 0) { // overflow
                return Long.MAX_VALUE;
            }
            if (end < start && result > 0) { // underflow
                return Long.MIN_VALUE;
            }
            return result;
        }

        @Override
        public Long minValue()
        {
            return Long.MIN_VALUE;
        }

        @Override
        public Long maxValue()
        {
            return Long.MAX_VALUE;
        }

        private Object readResolve()
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
