package com.mobileum.range;

import com.facebook.presto.spi.ConnectorSession;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

/*
 A range's flagss byte contains these bits:
#define RANGE_EMPTY         0x01     range is empty
#define RANGE_LB_INC        0x02     lower bound is inclusive
#define RANGE_UB_INC        0x04     upper bound is inclusive
#define RANGE_LB_INF        0x08     lower bound is -infinity
#define RANGE_UB_INF        0x10     upper bound is +infinity
#define RANGE_LB_NULL       0x20     lower bound is null (NOT USED)
#define RANGE_UB_NULL       0x40     upper bound is null (NOT USED)

*/
/**
 *
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public abstract class Range<T extends Comparable<T>>
        implements Comparable<Range<T>>, IRange<T>, InternalRangeSerializer<T>
{
    protected final RangeBound<T> lower;
    protected final RangeBound<T> upper;
    //protected boolean isEmpty;
    private byte flags;

    //Not part of searalization
    protected boolean isPoint;//Unused

    public Range(RangeBound<T> lower, RangeBound<T> upper, byte flags)
    {
        this.lower = lower;
        this.upper = upper;
        this.flags = flags;
    }

    public Range(RangeBound<T> lower, RangeBound<T> upper)
    {
//        if (lower.getClass() != upper.getClass()) {
//            throw new IllegalArgumentException("lower Class and upper Class must be the same");
//        }
        if (upper.compareTo(lower) >= 0) {
            this.lower = lower;
            this.upper = upper;
        }
        else {
            this.lower = upper;
            this.upper = lower;
        }
        this.isPoint = lower.equals(upper);
        makeRange(lower, upper, false);
    }

    public Range(RangeBound<T> point)
    {
        this.lower = point;
        this.upper = point;
        this.isPoint = true;
    }

    public Range()
    {
        this.lower = this.upper = null;
        // this.isEmpty = isEmpty;
        this.flags |= RANGE_EMPTY;
        this.flags |= RANGE_LB_NULL;//Unused
        this.flags |= RANGE_UB_NULL;//Unused
    }

    @Override
    public int compareTo(Range<T> other)
    {
        int cmp;
        if (RangeUtils.isEmpty(flags) && RangeUtils.isEmpty(other.flags))
            cmp = 0;
        else if (RangeUtils.isEmpty(flags))
            cmp = -1;
        else if (RangeUtils.isEmpty(other.flags))
            cmp = 1;
        else
        {
            cmp = compareBounds(this.lower, other.lower);
            if (cmp == 0)
                cmp = compareBounds(this.upper, other.upper);
        }
        return cmp;
    }

    @Override
    public int hashCode()
    {
        if (RangeUtils.isEmpty(flags)) {
            return 0;
        }
        final int prime = 31;
        int result = 1;
        result = prime * result + flags;
        result = prime * result + ((lower == null) ? 0 : lower.hashCode());
        result = prime * result + ((upper == null) ? 0 : upper.hashCode());
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
        Range other = (Range) obj;
        if (flags != other.flags)
            return false;
        if (RangeUtils.isEmpty(flags) && RangeUtils.isEmpty(other.flags)) {
            return true;
        }
        if (lower == null) {
            if (other.lower != null)
                return false;
        }
        else if (!lower.equals(other.lower))
            return false;
        if (upper == null) {
            if (other.upper != null)
                return false;
        }
        else if (!upper.equals(other.upper))
            return false;
        return true;
    }

    private void handleRangeBoundsChanged()
    {
        assert (lower.isLower());
        assert (!upper.isLower());
        //Todo Remove
        if (!lower.isLower()) {
            throw new RuntimeException("assert");
        }
        if (upper.isLower()) {
            throw new RuntimeException("assert");
        }
        boolean isEmpty = RangeUtils.isEmpty(flags);
        byte flags = 0;
        if (isEmpty) {
            flags |= RANGE_EMPTY;
        }
        int cmp = compareBoundValues(lower, upper);
        if (cmp > 0) {
            throw new RuntimeException("range lower bound must be less than or equal to range upper bound");
        }
        if (cmp == 0 && !(lower.isInclusive() && upper.isInclusive()))
            flags |= RANGE_EMPTY;
        else
        {
            if (lower.isInfinite())
                flags |= RANGE_LB_INF;
            else if (lower.isInclusive())
                flags |= RANGE_LB_INC;
            if (upper.isInfinite())
                flags |= RANGE_UB_INF;
            else if (upper.isInclusive())
                flags |= RANGE_UB_INC;
        }
        this.flags = flags;
    }

    /**
     * Create A {@code Range} from {@code lowerValue} and {@code upperValue}.The range flags are passed here.
     * It serializes the flags and construct {@code RangeBound}.
     * @param lowerValue
     * @param upperValue
     * @param flags
     * @return
     */
    public Range<T> makeRange(T lowerValue, T upperValue, byte flags)
    {
        RangeBound<T> lower = new RangeBound<T>();
        RangeBound<T> upper = new RangeBound<T>();
        if (RangeUtils.hasLowerBound(flags)) {
            //Todo remove
            if (lowerValue == null) {
                throw new RuntimeException("NULL");
            }
            lower.setValue(lowerValue);
        }
        if (RangeUtils.hasUpperBound(flags)) {
            //Todo remove
            if (upperValue == null) {
                throw new RuntimeException("NULL");
            }
            upper.setValue(upperValue);
        }
        lower.setInfinite((flags & RANGE_LB_INF) != 0);
        lower.setInclusive((flags & RANGE_LB_INC) != 0);
        lower.setLower(true);
        upper.setInfinite((flags & RANGE_UB_INF) != 0);
        upper.setInclusive((flags & RANGE_UB_INC) != 0);
        upper.setLower(false);
        return makeRange(lower, upper, RangeUtils.isEmpty(flags));
    }

    /**
     * Provided lower and upper RangeBounds.Cunstruct the Range and set flags to it.
     * It performs canonicalization on range.
     * @param lower
     * @param upper
     * @param isEmpty
     * @return
     */
    public Range<T> makeRange(RangeBound<T> lower, RangeBound<T> upper, boolean isEmpty)
    {
        byte flag = isEmpty ? RANGE_EMPTY : 0;
        Range<T> range = custructRange(lower, upper, flag);
        range.handleRangeBoundsChanged();
        if (!RangeUtils.isEmpty(range.flags) && range.getRangeDomain() instanceof DiscreteDomain) {
            range = range.makeCanonicalize();
        }
        return range;
    }

    public Range<T> makeCanonicalize()
    {
        //        range.lower.canonicalize(range.getRangeDomain());
        //        range.upper.canonicalize(range.getRangeDomain());
        //todo null handling on next :: if range outs
        if (RangeUtils.isEmpty(this.flags))
            return getEmptyRange();
        DiscreteDomain<T> discreateDomain = (DiscreteDomain<T>) this.getRangeDomain();

        if (!this.lower.isInfinite() && !this.lower.isInclusive())
        {
            T nextValue = discreateDomain.next(this.lower.getValue());
            this.lower.setValue(nextValue);
            this.lower.setInclusive(true);
        }

        if (!this.upper.isInfinite() && this.upper.isInclusive())
        {
            T nextValue = discreateDomain.next(this.upper.getValue());
            this.upper.setValue(nextValue);
            this.upper.setInclusive(false);
        }
        this.handleRangeBoundsChanged();
        return this;
    }

    /* strictly right of?*/
    //Todo typecheck

    @Override
    public boolean after(Range<T> range)
    {
        /* Different types should be prevented by ANYRANGE matching rules */
        /* An empty range is neither before nor after any other range */
        if (RangeUtils.isEmpty(this.flags) || RangeUtils.isEmpty(range.flags))
            return false;
        return (compareBounds(this.lower, range.upper) > 0);
    }

    @Override
    public boolean after(T value)
    {
        if (RangeUtils.isEmpty(this.flags)) {
            return false;
        }
        else {
            RangeBound<T> r = new RangeBound<T>();
            r.setValue(value);
            r.setInclusive(true);
            r.setInfinite(false);
            return (this.lower.compareTo(r)) > 0;
        }
    }

    ///* strictly left of? */
    //Todo typecheck
    @Override
    public boolean before(Range<T> range)
    {
        /* Different types should be prevented by ANYRANGE matching rules */
        /* An empty range is neither before nor after any other range */
        if (RangeUtils.isEmpty(this.flags) || RangeUtils.isEmpty(range.flags))
            return false;
        return (compareBounds(this.upper, range.lower) < 0);
    }

    @Override
    public boolean before(T value)
    {
        if (RangeUtils.isEmpty(this.flags)) {
            return false;
        }
        else {
            RangeBound<T> r = new RangeBound<T>();
            r.setValue(value);
            r.setInclusive(true);
            r.setInfinite(false);
            return (this.upper.compareTo(r)) < 0;
        }
    }

    //return null
    @Override
    public T upper()
    {
        if (RangeUtils.isEmpty(this.flags) || upper.isInfinite())
            return null;
        else {
            return upper.getValue();
        }
    }

    //type check
    @Override
    public boolean includes(Range<T> range)
    {
        if (RangeUtils.isEmpty(range.flags)) {
            return true;
        }
        if (RangeUtils.isEmpty(this.flags)) {
            return false;
        }
        if (compareBounds(this.lower, range.lower) > 0) {
            return false;
        }
        if (compareBounds(this.upper, range.upper) < 0) {
            return false;
        }
        return true;
    }

    //todo range compare bounds
    private int compareBounds(RangeBound<T> left, RangeBound<T> right)
    {
        return left.compareTo(right);
    }

    //todo range compare bounds values
    private int compareBoundValues(RangeBound<T> left, RangeBound<T> right)
    {
        return left.compareBoundValuesTo(right);
    }

    @Override
    public boolean includes(T value)
    {
        if (RangeUtils.isEmpty(this.flags))
            return false;
        if (!lower.isInfinite()) {
            int cmp = lower.getValue().compareTo(value);
            if (cmp > 0) {
                return false;
            }
            if (cmp == 0 && !lower.isInclusive()) {
                return false;
            }
        }
        if (!upper.isInfinite()) {
            int cmp = upper.getValue().compareTo(value);
            if (cmp < 0) {
                return false;
            }
            if (cmp == 0 && !upper.isInclusive()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isPoint()
    {
        return false;
    }

    //Todo typechaeck
    @Override
    public boolean overlaps(Range<T> range)
    {
        /* An empty range does not overlap any other range */
        if (RangeUtils.isEmpty(this.flags) || RangeUtils.isEmpty(range.flags)) {
            return false;
        }
        if (compareBounds(this.lower, range.lower) >= 0 &&
                compareBounds(this.lower, range.upper) <= 0)
            return true;

        if (compareBounds(range.lower, this.lower) >= 0 &&
                compareBounds(range.lower, this.upper) <= 0)
            return true;

        return false;
    }

    //return null
    @Override
    public T lower()
    {
        if (RangeUtils.isEmpty(this.flags) || lower.isInfinite())
            return null;
        else {
            return lower.getValue();
        }
    }

    /*
     * Check if two bounds A and B are "adjacent", where A is an upper bound and B
     * is a lower bound. For the bounds to be adjacent, each subtype value must
     * satisfy strictly one of the bounds: there are no values which satisfy both
     * bounds (i.e. less than A and greater than B); and there are no values which
     * satisfy neither bound (i.e. greater than A and less than B).
     *
     * For discrete ranges, we rely on the canonicalization function to see if A..B
     * normalizes to empty. (If there is no canonicalization function, it's
     * impossible for such a range to normalize to empty, so we needn't bother to
     * try.)
     *
     * If A == B, the ranges are adjacent only if the bounds have different
     * inclusive flagss (i.e., exactly one of the ranges includes the common
     * boundary point).
     *
     * And if A > B then the ranges are not adjacent in this order.
     */
    //Todo type check
    @Override
    public boolean isAdjacentTo(Range<T> range)
    {
        /* Different types should be prevented by ANYRANGE matching rules */

        /* An empty range is not adjacent to any other range */
        if (RangeUtils.isEmpty(this.flags) || RangeUtils.isEmpty(range.flags))
            return false;

        /*
         * Given two ranges A..B and C..D, the ranges are adjacent if and only if
         * B is adjacent to C, or D is adjacent to A.
         */
        return (isBoundsAdjacent(range.getRangeDomain(), this.upper, range.lower) || isBoundsAdjacent(range.getRangeDomain(), range.upper, this.lower));
    }

    private boolean isBoundsAdjacent(Domain<T> domain, RangeBound<T> boundA, RangeBound<T> boundB)
    {
        assert (!boundA.isLower() && boundB.isLower());
        if (!(!boundA.isLower() && boundB.isLower())) {
            throw new RuntimeException("Assert");
        }
        int cmp = compareBoundValues(boundA, boundB);
        if (cmp < 0)
        {
            Range<T> r;

            /*
             * Bounds do not overlap; see if there are points in between.
             */

            /* in a continuous subtype, there are assumed to be points between */
            if (!(domain instanceof DiscreteDomain))
                return false;

            /*
             * The bounds are of a discrete range type; so make a range A..B and
             * see if it's empty.
             */

            /* flip the inclusion flags */
            RangeBound<T> tempBoundA = new RangeBound<T>(boundA);
            RangeBound<T> tempBoundB = new RangeBound<T>(boundB);
            tempBoundA.setInclusive(!tempBoundA.isInclusive());
            tempBoundB.setInclusive(!tempBoundB.isInclusive());
            /* change upper/lower labels to avoid Assert failures */
            tempBoundA.setLower(true);
            tempBoundB.setLower(false);
            r = makeRange(tempBoundA, tempBoundB, false);
            return isEmpty(r);
        }
        else if (cmp == 0)
            return boundA.isInclusive() != boundB.isInclusive();
        else
            return false; /* bounds overlap */
    }

    public boolean isEmpty(Range<T> range)
    {
        return RangeUtils.isEmpty(range.flags);
    }

    /* does not extend to right of */
    //this range_overleft range1
    //Todo TypeCheck
    @Override
    public boolean notEndsAfter(Range<T> range)
    {
        /* An empty range is neither before nor after any other range */
        if (RangeUtils.isEmpty(this.flags) || RangeUtils.isEmpty(range.flags)) {
            return false;
        }
        if (compareBounds(this.upper, range.upper) <= 0)
            return true;

        return false;
    }

    //Todo optimize
    @Override
    public boolean notEndsAfter(T value)
    {
        RangeBound<T> r = new RangeBound<T>();
        r.setValue(value);
        r.setInclusive(true);
        r.setInfinite(false);
        r.setLower(false);
        /* An empty range is neither before nor after any other range */
        if (RangeUtils.isEmpty(this.flags)) {
            return false;
        }
        if (compareBounds(this.upper, r) <= 0)
            return true;

        return false;
    }

    /* does not extend to left of? */
    //this range_overright range
    @Override
    public boolean notStartsBefore(Range<T> range)
    {
        /* An empty range is neither before nor after any other range */
        if (RangeUtils.isEmpty(this.flags) || RangeUtils.isEmpty(range.flags)) {
            return false;
        }
        if (compareBounds(this.lower, range.lower) >= 0)
            return true;

        return false;
    }

    @Override
    public boolean notStartsBefore(T value)
    {
        RangeBound<T> r = new RangeBound<T>(value, false, true, true);
        /* An empty range is neither before nor after any other range */
        if (RangeUtils.isEmpty(this.flags)) {
            return false;
        }
        if (compareBounds(this.lower, r) >= 0)
            return true;

        return false;
    }

//Todo check type
    @Override
    public Range<T> union(Range<T> range)
    {
        /* Different types should be prevented by ANYRANGE matching rules */
        /* if either is empty, the other is the correct answer */
        if (RangeUtils.isEmpty(this.flags))
            return range;
        if (RangeUtils.isEmpty(range.flags))
            return this;

        if (!this.overlaps(range) &&
                !this.isAdjacentTo(range))
            throw new RuntimeException("result of range union would not be contiguous");

        RangeBound<T> resultLower;
        RangeBound<T> resultUpper;

        if (compareBounds(this.lower, range.lower) < 0)
            resultLower = this.lower;
        else
            resultLower = range.lower;

        if (compareBounds(this.upper, range.upper) > 0)
            resultUpper = this.upper;
        else
            resultUpper = range.upper;

        return makeRange(resultLower, resultUpper, false);
    }

    //Todo check type
    @Override
    public Range<T> intersection(Range<T> range)
    {
        /* Different types should be prevented by ANYRANGE matching rules */

        if (RangeUtils.isEmpty(this.flags) || RangeUtils.isEmpty(range.flags) || !overlaps(range))
            return getEmptyRange();

        RangeBound<T> resultLower;
        RangeBound<T> resultUpper;
        if (compareBounds(this.lower, range.lower) >= 0)
            resultLower = this.lower;
        else
            resultLower = range.lower;

        if (compareBounds(this.upper, range.upper) <= 0)
            resultUpper = this.upper;
        else
            resultUpper = range.upper;

        return makeRange(resultLower, resultUpper, false);
    }

    //Todo Type check
    @Override
    public Range<T> minus(Range<T> range)
    {
        /* Different types should be prevented by ANYRANGE matching rules */

        /* if either is empty, r1 is the correct answer */
        if (RangeUtils.isEmpty(this.flags) || RangeUtils.isEmpty(range.flags))
            return this;

        int cmp_l1l2 = compareBounds(this.lower, range.lower);
        int cmp_l1u2 = compareBounds(this.lower, range.upper);
        int cmp_u1l2 = compareBounds(this.upper, range.lower);
        int cmp_u1u2 = compareBounds(this.upper, range.upper);

        if (cmp_l1l2 < 0 && cmp_u1u2 > 0)
            throw new RuntimeException("result of range difference would not be contiguous");

        if (cmp_l1u2 > 0 || cmp_u1l2 < 0)
            return custructRange(new RangeBound<T>(lower), new RangeBound<T>(upper), flags);

        if (cmp_l1l2 >= 0 && cmp_u1u2 <= 0)
            return getEmptyRange();

        if (cmp_l1l2 <= 0 && cmp_u1l2 >= 0 && cmp_u1u2 <= 0)
        {
            RangeBound<T> tempRange = new RangeBound<T>(range.lower);
            tempRange.setInclusive(!tempRange.isInclusive());
            tempRange.setLower(false); /* it will become the upper bound */
            //Todo: No need for immutable
            return makeRange(new RangeBound<T>(lower), tempRange, false);
        }

        if (cmp_l1l2 >= 0 && cmp_u1u2 >= 0 && cmp_l1u2 <= 0)
        {
            RangeBound<T> tempRange = new RangeBound<T>(range.upper);
            tempRange.setInclusive(!tempRange.isInclusive());
            tempRange.setLower(true); /* it will become the lower bound */
            //Todo: No need for immutable

            return makeRange(tempRange, new RangeBound<T>(upper), false);
        }

        throw new RuntimeException("unexpected case in range_minus");
    }

    @Override
    public Range<T> parse(String range)
    {
        if (range == null) {
            throw new RuntimeException();
        }
        else if (range.isEmpty()) {
            throw new RuntimeException();
        }
        else {
            range = range.trim();
            byte flags = 0;
            if (range.equalsIgnoreCase(RANGE_EMPTY_LITERAL)) {
                flags = RANGE_EMPTY;
//                flags |= RANGE_LB_NULL;//not used
//                flags |= RANGE_UB_NULL;//not used
                //Todo remove cast
                return getEmptyRange();
            }
            else {
                if (range.startsWith("[")) {
                    flags |= RANGE_LB_INC;
                }
                else if (range.startsWith("(")) {

                }
                else {
                    throw new RuntimeException("Not starts with either [ or (");
                }
                String arr[] = range.split(",");
                if (arr.length != 2) {
                    throw new RuntimeException("invalid comma position");
                }
                else {
                    String left = arr[0];
                    String right = arr[1];
                    left = left.substring(1, left.length());
                    if (right.endsWith("]")) {
                        flags |= RANGE_UB_INC;
                    }
                    else if (right.endsWith(")")) {

                    }
                    else {
                        throw new RuntimeException("Not starts with either [ or (");
                    }
                    right = right.substring(0, right.length() - 1);
                    T leftValue = null;
                    T rightValue = null;
                    if (left.isEmpty()) {
                        flags |= RANGE_LB_INF;
                    }
                    else {
                        leftValue = parseValue(left);
                    }
                    if (right.isEmpty()) {
                        flags |= RANGE_UB_INF;
                    }
                    else {
                        rightValue = parseValue(right);
                    }
                    return makeRange(leftValue, rightValue, flags);
                }
            }
        }
    }

    public Range<T> parse(String range,ConnectorSession session){
        if (range == null) {
            throw new RuntimeException();
        }
        else if (range.isEmpty()) {
            throw new RuntimeException();
        }
        else {
            range = range.trim();
            byte flags = 0;
            if (range.equalsIgnoreCase(RANGE_EMPTY_LITERAL)) {
                flags = RANGE_EMPTY;
//                flags |= RANGE_LB_NULL;//not used
//                flags |= RANGE_UB_NULL;//not used
                //Todo remove cast
                return getEmptyRange();
            }
            else {
                if (range.startsWith("[")) {
                    flags |= RANGE_LB_INC;
                }
                else if (range.startsWith("(")) {

                }
                else {
                    throw new RuntimeException("Not starts with either [ or (");
                }
                String arr[] = range.split(",");
                if (arr.length != 2) {
                    throw new RuntimeException("invalid comma position");
                }
                else {
                    String left = arr[0];
                    String right = arr[1];
                    left = left.substring(1, left.length());
                    if (right.endsWith("]")) {
                        flags |= RANGE_UB_INC;
                    }
                    else if (right.endsWith(")")) {

                    }
                    else {
                        throw new RuntimeException("Not starts with either [ or (");
                    }
                    right = right.substring(0, right.length() - 1);
                    T leftValue = null;
                    T rightValue = null;
                    if (left.isEmpty()) {
                        flags |= RANGE_LB_INF;
                    }
                    else {
                        leftValue = parseValue(left,session);
                    }
                    if (right.isEmpty()) {
                        flags |= RANGE_UB_INF;
                    }
                    else {
                        rightValue = parseValue(right,session);
                    }
                    return makeRange(leftValue, rightValue, flags);
                }
            }
        }
    }


    @Override
    public Range<T> parse(Slice range)
    {
        Pointer pointer = new Pointer(0);
        byte flags = range.getByte(0);
        pointer.increment(1);
        if (RangeUtils.isEmpty(flags)) {
            //Todo : Remove Cast
            return getEmptyRange();
        }
        else {
            boolean hasLower = RangeUtils.hasLowerBound(flags);
            boolean hasUpper = RangeUtils.hasUpperBound(flags);
            T lowerValue = null;
            T upperValue = null;
            if (hasLower && hasUpper) {
                lowerValue = parseValue(range, pointer);
                upperValue = parseValue(range, pointer);
            }
            else if (hasLower) {
                lowerValue = parseValue(range, pointer);
            }
            else if (hasUpper) {
                upperValue = parseValue(range, pointer);
            }
            else {
                // both infinite
            }
            return makeRange(lowerValue, upperValue, flags);
        }
    }

    @Override
    public String getRangeAsString()
    {
        if (RangeUtils.isEmpty(flags)) {
            return RANGE_EMPTY_LITERAL;
        }
        else {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(RangeUtils.isLowerInc(flags) ? "[" : "(");
            if (RangeUtils.hasLowerBound(flags)) {
                stringBuilder.append(getValueAsString(this.lower.getValue()));
            }
            stringBuilder.append(",");
            if (RangeUtils.hasUpperBound(flags)) {
                stringBuilder.append(getValueAsString(this.upper.getValue()));
            }
            stringBuilder.append(RangeUtils.isUpperInc(flags) ? "]" : ")");
            return stringBuilder.toString();
        }
    }

    @Override
    public String getRangeAsString(ConnectorSession session)
    {
        if (RangeUtils.isEmpty(flags)) {
            return RANGE_EMPTY_LITERAL;
        }
        else {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(RangeUtils.isLowerInc(flags) ? "[" : "(");
            if (RangeUtils.hasLowerBound(flags)) {
                stringBuilder.append(getValueAsString(session, this.lower.getValue()));
            }
            stringBuilder.append(",");
            if (RangeUtils.hasUpperBound(flags)) {
                stringBuilder.append(getValueAsString(session, this.upper.getValue()));
            }
            stringBuilder.append(RangeUtils.isUpperInc(flags) ? "]" : ")");
            return stringBuilder.toString();
        }
    }

    @Override
    public Slice getRangeAsSlice()
    {
        if (RangeUtils.isEmpty(flags)) {
            Slice slice = Slices.allocate(1);
            slice.setByte(0, flags);
            return slice;
        }
        else {
            boolean hasLower = RangeUtils.hasLowerBound(flags);
            boolean hasUpper = RangeUtils.hasUpperBound(flags);
            if (hasLower && hasUpper) {
                Slice lower = getValueAsSlice(this.lower.getValue());
                Slice upper = getValueAsSlice(this.upper.getValue());
                Slice slice = Slices.allocate(1 + lower.length() + upper.length());
                slice.setByte(0, flags);
                slice.setBytes(1, lower);
                slice.setBytes(1 + lower.length(), upper);
                return slice;
            }
            else if (hasLower) {
                Slice lower = getValueAsSlice(this.lower.getValue());
                Slice slice = Slices.allocate(1 + lower.length());
                slice.setByte(0, flags);
                slice.setBytes(1, lower);
                return slice;
            }
            else if (hasUpper) {
                Slice upper = getValueAsSlice(this.upper.getValue());
                Slice slice = Slices.allocate(1 + upper.length());
                slice.setByte(0, flags);
                slice.setBytes(1, upper);
                return slice;
            }
            else {
                //both infinite
                Slice slice = Slices.allocate(1);
                slice.setByte(0, flags);
                return slice;
            }
        }
    }

    @Override
    abstract public Domain<T> getRangeDomain();

    @Override
    public abstract Range<T> custructRange(RangeBound<T> lower, RangeBound<T> upper, byte flags);

    @Override
    public abstract T parseValue(String value);

    @Override
    public abstract T parseValue(String value,ConnectorSession session);

    @Override
    public abstract T parseValue(Slice value, Pointer index);

    @Override
    public abstract Slice getValueAsSlice(T value);

    @Override
    public abstract String getValueAsString(T value);

    @Override
    public abstract String getValueAsString(ConnectorSession session, T value);

    @Override
    public abstract RangeSerializer<T> getSerializer();

    public abstract Range<T> getEmptyRange();

    @Override
    public String toString()
    {
        return getRangeAsString();
    }
}
