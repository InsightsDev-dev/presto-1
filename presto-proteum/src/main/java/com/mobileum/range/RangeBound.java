package com.mobileum.range;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class RangeBound<T extends Comparable<T>>
        implements Comparable<RangeBound<T>>
{
    private T value;  //the bound value, if any
    private boolean infinite; //bound is +/- infinity
    private boolean inclusive; //bound is inclusive (vs exclusive)
    private boolean lower; //this is the lower (vs upper) bound,this is only applicable in case of infinite

    public RangeBound()
    {}

    public RangeBound(T value, boolean infinite, boolean inclusive, boolean lower)
    {
        this.value = value;
        this.infinite = infinite;
        this.inclusive = inclusive;
        this.lower = lower;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (inclusive ? 1231 : 1237);
        result = prime * result + (infinite ? 1231 : 1237);
        if (infinite) {
            result = prime * result + (lower ? 1231 : 1237);
        }
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RangeBound other = (RangeBound) obj;
        if (inclusive != other.inclusive)
            return false;
        if (infinite != other.infinite)
            return false;
        if (infinite && lower != other.lower)
            return false;
        if (value == null) {
            if (other.value != null)
                return false;
        }
        else if (!value.equals(other.value))
            return false;
        return true;
    }

    public RangeBound(RangeBound<T> rangeBound)
    {
        this(rangeBound.value, rangeBound.infinite, rangeBound.inclusive, rangeBound.lower);
    }

    public T getValue()
    {
        return value;
    }

    public void setValue(T value)
    {
        this.value = value;
    }

    public boolean isInfinite()
    {
        return infinite;
    }

    public void setInfinite(boolean infinite)
    {
        this.infinite = infinite;
    }

    public boolean isInclusive()
    {
        return inclusive;
    }

    public void setInclusive(boolean inclusive)
    {
        this.inclusive = inclusive;
    }

    public boolean isLower()
    {
        return lower;
    }

    public void setLower(boolean lower)
    {
        this.lower = lower;
    }

    /*
     * Compare two range boundary points, returning <0, 0, or >0 according to
     * whether b1 is less than, equal to, or greater than b2.
     *
     * The boundaries can be any combination of upper and lower; so it's useful
     * for a variety of operators.
     *
     * The simple case is when b1 and b2 are both finite and inclusive, in which
     * case the result is just a comparison of the values held in b1 and b2.
     *
     * If a bound is exclusive, then we need to know whether it's a lower bound,
     * in which case we treat the boundary point as "just greater than" the held
     * value; or an upper bound, in which case we treat the boundary point as
     * "just less than" the held value.
     *
     * If a bound is infinite, it represents minus infinity (less than every other
     * point) if it's a lower bound; or plus infinity (greater than every other
     * point) if it's an upper bound.
     *
     * There is only one case where two boundaries compare equal but are not
     * identical: when both bounds are inclusive and hold the same finite value,
     * but one is an upper bound and the other a lower bound.
     */
    @Override
    public int compareTo(RangeBound<T> o)
    {
        int result;

        /*
         * First, handle cases involving infinity, which don't require invoking
         * the comparison proc.
         */
        if (this.infinite && o.infinite)
        {
            /*
             * Both are infinity, so they are equal unless one is lower and the
             * other not.
             */
            if (this.lower == o.lower)
                return 0;
            else
                return this.lower ? -1 : 1;
        }
        else if (this.infinite)
            return this.lower ? -1 : 1;
        else if (o.infinite)
            return o.lower ? 1 : -1;

        /*
         * Both boundaries are finite, so compare the held values.
         */
        result = this.value.compareTo(o.value);

        /*
         * If the comparison is anything other than equal, we're done. If they
         * compare equal though, we still have to consider whether the boundaries
         * are inclusive or exclusive.
         */
        if (result == 0)
        {
            if (!this.inclusive && !o.inclusive)
            {
                /* both are exclusive */
                if (this.lower == o.lower)
                    return 0;
                else
                    return this.lower ? 1 : -1;
            }
            else if (!this.inclusive)
                return this.lower ? 1 : -1;
            else if (!o.inclusive)
                return o.lower ? -1 : 1;
            else
            {
                /*
                 * Both are inclusive and the values held are equal, so they are
                 * equal regardless of whether they are upper or lower boundaries,
                 * or a mix.
                 */
                return 0;
            }
        }

        return result;
    }

//    public void canonicalize(Domain<T> domain)
//    {
//        if (domain instanceof DiscreateDomain) {
//            DiscreateDomain<T> discreateDomain = (DiscreateDomain<T>) domain;
//            
//        }
//    }

    /*
     * Compare two range boundary point values, returning <0, 0, or >0 according
     * to whether b1 is less than, equal to, or greater than b2.
     *
     * This is similar to but simpler than range_cmp_bounds().  We just compare
     * the values held in b1 and b2, ignoring inclusive/exclusive flags.  The
     * lower/upper flags only matter for infinities, where they tell us if the
     * infinity is plus or minus.
     */
    public int compareBoundValuesTo(RangeBound<T> o)
    {/*
         * First, handle cases involving infinity, which don't require invoking
         * the comparison proc.
         */
        if (this.infinite && o.infinite)
        {
            /*
             * Both are infinity, so they are equal unless one is lower and the
             * other not.
             */
            if (this.lower == o.lower)
                return 0;
            else
                return this.lower ? -1 : 1;
        }
        else if (this.infinite)
            return this.lower ? -1 : 1;
        else if (o.infinite)
            return o.lower ? 1 : -1;

        /*
         * Both boundaries are finite, so compare the held values.
         */
        return this.value.compareTo(o.value);

    }
}
