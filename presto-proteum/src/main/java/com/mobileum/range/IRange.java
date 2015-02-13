package com.mobileum.range;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
/** A range of values. */
public interface IRange<T extends Comparable<T>>
{
    /** Returns <code>true</code> if the lowest value in this range
     * occurs after the greatest value in <code>range</code>.
     * 
     * @param range The range to test
     * @return <code>true</code> if the lowest value in this range
     * occurs after the greatest value in <code>range</code>
     */
    boolean after(Range<T> range);

    /** Returns <code>true</code> if this range occurs after <code>value</code>.
     * 
     * @param value The value to test
     * @return <code>true</code> if this range occurs after <code>value</code>
     */
    boolean after(T value);

    /** Returns <code>true</code> if the greatest value in this range
     * occurs before the lowest value in <code>range</code>.
     * 
     * @param range The range to test
     * @return <code>true</code> if the greatest value in this range
     * occurs before the lowest value in <code>range</code>
     */
    boolean before(Range<T> range);

    /** Returns <code>true</code> if this range occurs before <code>value</code>.
     * 
     * @param value The value to test
     * @return <code>true</code> if this range occurs before <code>value</code>
     */
    boolean before(T value);

    /** Returns the ending value of this range.
     * 
     * @return Ending value
     */
    T upper();

    /** Returns <code>true</code> if this range includes <code>range</code>.
     * 
     * @param range The range to test
     * @return <code>true</code> if this range includes <code>range</code>
     */
    boolean includes(Range<T> range);

    /** Returns <code>true</code> if <code>value</code> occurs within this range.
     * 
     * @param value The value to test
     * @return <code>true</code> if <code>value</code> occurs within this range
     */
    boolean includes(T value);

    /** Returns <code>true</code> if the starting and ending values are equal.
     * 
     * @return <code>true</code> if the starting and ending values are equal
     */
    boolean isPoint();

    /** Returns <code>true</code> if this range overlaps <code>range</code>.
     * 
     * @param range The range to test
     * @return <code>true</code> if this range overlaps <code>range</code>
     */
    boolean overlaps(Range<T> range);

    /** Returns the starting value of this range.
     * 
     * @return Starting value
     */
    T lower();

    /**
     * Retuns true if both the ranges are adjacent.means there are no point exists between these two ranges.
     * If lower range ends with T1 and upper range starts with T2.Then will return true if there is no T between
     * T1 and T2.
     * @param range
     * @return
     */
    public boolean isAdjacentTo(Range<T> range);

    /** Returns <code>true</code> if the greatest value in this range
     * not occurs after the greatest value in <code>range</code>.
     * 
     * @param range The range to test
     * @return <code>true</code> if the greatest value in this range
     * not occurs after the greatest value in <code>range</code>.
     */
    boolean notEndsAfter(Range<T> range);

    /** Returns <code>true</code> if the greatest value in this range
     * not occurs after the greatest value in <code>value</code>.
     * 
     * @param range The range to test
     * @return <code>true</code> if the greatest value in this range
     * not occurs after  <code>value</code>.
     */
    boolean notEndsAfter(T value);

    /** Returns <code>true</code> if the lowest value in this range
     * not occurs before the lowest value in <code>range</code>.
     * 
     * @param range The range to test
     * @return <code>true</code> if the lowest value in this range
     * not occurs after the lowest value in <code>range</code>.
     */
    boolean notStartsBefore(Range<T> range);

    /** Returns <code>true</code> if the lowest value in this range
     * not occurs before the lowest value in <code>value</code>.
     * 
     * @param range The range to test
     * @return <code>true</code> if the lowest value in this range
     * not occurs after  <code>value</code>.
     */
    boolean notStartsBefore(T value);

    /** Returns union of this with <code>other</code>.
     * 
     * @param other The range to test
     * @return union of this with <code>other</code>
     */
    Range<T> union(Range<T> other);

    /** Returns intersection of this with <code>other</code>.
     * 
     * @param other The range to test
     * @return intersection of this with <code>other</code>
     */
    Range<T> intersection(Range<T> other);

    /** Returns portion of this not contained by <code>other</code>.
     * 
     * @param other The range to test
     * @return portion of this not contained by <code>other</code> as {@link Range}.
     */
    Range<T> minus(Range<T> other);

    Domain<T> getRangeDomain();

    RangeSerializer<T> getSerializer();

    public static final String RANGE_EMPTY_LITERAL = "empty";
    public static final byte RANGE_EMPTY = 0x01;    // range is empty
    public static final byte RANGE_LB_INC = 0x02;    // lower bound is inclusive 
    public static final byte RANGE_UB_INC = 0x04;    // upper bound is inclusive 
    public static final byte RANGE_LB_INF = 0x08;   // lower bound is -infinity 
    public static final byte RANGE_UB_INF = 0x10;  // upper bound is +infinity 
    public static final byte RANGE_LB_NULL = 0x20; // lower bound is null (NOT USED) 
    public static final byte RANGE_UB_NULL = 0x40;// upper bound is null (NOT USED) 
}
