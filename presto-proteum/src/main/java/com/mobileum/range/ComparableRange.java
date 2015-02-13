package com.mobileum.range;

/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
/** An immutable range of values. */
public abstract class ComparableRange<T extends Comparable<T>>
        implements IRange<T>, Comparable<ComparableRange<T>>
{

    protected final T lower;
    protected final T upper;
    protected final boolean isPoint;

    public ComparableRange(T lower, T upper)
    {
        if (lower.getClass() != upper.getClass()) {
            throw new IllegalArgumentException("lower Class and upper Class must be the same");
        }
        if (upper.compareTo(lower) >= 0) {
            this.lower = lower;
            this.upper = upper;
        }
        else {
            this.lower = upper;
            this.upper = lower;
        }
        this.isPoint = lower.equals(upper);
    }

    public ComparableRange(T point)
    {
        this.lower = point;
        this.upper = point;
        this.isPoint = true;
    }

    @Override
    public boolean after(Range<T> range)
    {
        return this.lower.compareTo(range.upper()) > 0;
    }

    @Override
    public boolean after(T value)
    {
        return this.lower.compareTo(value) > 0;
    }

    @Override
    public boolean before(Range<T> range)
    {
        return this.upper.compareTo(range.lower()) < 0;
    }

    @Override
    public boolean before(T value)
    {
        return this.upper.compareTo(value) < 0;
    }

    @Override
    public T upper()
    {
        return this.upper;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        try {
            @SuppressWarnings("unchecked")
            ComparableRange<T> that = (ComparableRange<T>) obj;
            return this.lower.equals(that.lower()) && this.upper.equals(that.upper());
        }
        catch (Exception e) {}
        return false;
    }

    @Override
    public int compareTo(ComparableRange<T> range)
    {
        if (this == range) {
            return 0;
        }
        return this.lower.equals(range.lower()) ? this.upper.compareTo(range.upper()) : this.lower.compareTo(range.lower());
    }

    @Override
    public boolean includes(Range<T> range)
    {
        return this.includes(range.lower()) && this.includes(range.upper());
    }

    @Override
    public boolean includes(T value)
    {
        if (this.isPoint) {
            return value.equals(this.lower());
        }
        return value.compareTo(this.lower()) >= 0 && value.compareTo(this.upper()) <= 0;
    }

    @Override
    public boolean isPoint()
    {
        return this.isPoint;
    }

    @Override
    public boolean overlaps(Range<T> range)
    {
        return range.includes(this.lower()) || range.includes(this.upper()) || this.includes(range);
    }

    @Override
    public T lower()
    {
        return this.lower;
    }

    @Override
    public String toString()
    {
        return "[" + this.lower + "," + this.upper + "]";
    }

    @Override
    public boolean isAdjacentTo(Range<T> range)
    {
        return false;
    }

    @Override
    public boolean notEndsAfter(Range<T> range)
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean notEndsAfter(T value)
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean notStartsBefore(Range<T> range)
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean notStartsBefore(T value)
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Range<T> union(Range<T> other)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Range<T> intersection(Range<T> other)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Range<T> minus(Range<T> other)
    {
        // TODO Auto-generated method stub
        return null;
    }
}
