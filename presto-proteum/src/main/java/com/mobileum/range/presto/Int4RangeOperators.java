package com.mobileum.range.presto;

import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.LEFT_CONTAINS_RIGHT;
import static com.facebook.presto.metadata.OperatorType.RIGHT_CONTAINS_LEFT;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import io.airlift.slice.Slice;

import java.text.ParseException;

import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.mobileum.range.IntegerRange;
import com.mobileum.range.RangeSerializer;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class Int4RangeOperators
{
    private Int4RangeOperators()
    {

    }

    @ScalarOperator(LEFT_CONTAINS_RIGHT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean leftContainsRightValue(
            @SqlType(StandardTypes.BIGINT) long left,
            @SqlType(StandardTypes.BIGINT) long right)
    {
        return left > right;
    }

    static RangeSerializer<Integer> rangeSerializer = IntegerRange.emptyRange.getSerializer();

    @Description("length of the given binary")
    @ScalarFunction("int4range")
    @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME)
    public static Slice int4range(@SqlType(StandardTypes.BIGINT) long min,
            @SqlType(StandardTypes.BIGINT) long max)
    {

        return Int4Range.serialize(Int4Range.createRange(min, max));
    }

    @ScalarFunction("int4range")
    @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME)
    public static Slice int4range(@SqlType(StandardTypes.BIGINT) long min,
            @SqlType(StandardTypes.BIGINT) long max, @SqlType(StandardTypes.VARCHAR) Slice flags)
    {
        return Int4Range.serialize(Int4Range.createRange(min, max, flags.toStringUtf8()));
    }

    public static Slice int4range(@SqlType(StandardTypes.VARCHAR) Slice min,
            @SqlType(StandardTypes.VARCHAR) Slice max)
    {
        long min1 = Long.parseLong(min.toStringUtf8());
        long max1 = Long.parseLong(max.toStringUtf8());
        return Int4Range.serialize(Int4Range.createRange(min1, max1));
    }

    @ScalarFunction("int4range")
    @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME)
    public static Slice int4range(@SqlType(StandardTypes.VARCHAR) Slice min,
            @SqlType(StandardTypes.VARCHAR) Slice max, @SqlType(StandardTypes.VARCHAR) Slice flags)
    {
        long min1 = Long.parseLong(min.toStringUtf8());
        long max1 = Long.parseLong(max.toStringUtf8());
        return Int4Range.serialize(Int4Range.createRange(min1, max1, flags.toStringUtf8()));
    }

    @Description("display as human readable format")
    @ScalarFunction("lower")
    @SqlType(StandardTypes.BIGINT)
    public static long lower(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice slice)
    {
        return Int4Range.deSerialize(slice).lower();
    }

    @Description("display as human readable format")
    @ScalarFunction("upper")
    @SqlType(StandardTypes.BIGINT)
    public static long upper(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice slice)
    {
        return Int4Range.deSerialize(slice).upper();
    }

    // LEFT_CONTAINS_RIGHT
    @ScalarOperator(LEFT_CONTAINS_RIGHT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean leftContainsRight(ConnectorSession session,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
            throws ParseException
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return left1.includes(right1);
    }

    @ScalarOperator(LEFT_CONTAINS_RIGHT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean leftContainsRightValue(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(StandardTypes.BIGINT) long value)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        return left1.includes((int) value);
    }

    @ScalarOperator(RIGHT_CONTAINS_LEFT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean rightContains(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return right1.includes(left1);
    }

    @ScalarOperator(RIGHT_CONTAINS_LEFT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean rightContainsleft(
            @SqlType(StandardTypes.BIGINT) long left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange right1 = Int4Range.deSerialize(right);
        return right1.includes((int) left);
    }

    @ScalarOperator(OperatorType.OVERLAPPING_WITH)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean overlaps(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return left1.overlaps(right1);
    }

    @ScalarOperator(OperatorType.STRICTLY_LEFT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean strictLeft(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return left1.before(right1);
    }

    @ScalarOperator(OperatorType.STRICTLY_RIGHT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean strictRight(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return left1.after(right1);
    }

    @ScalarOperator(OperatorType.ADJACENT_WITH)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean adjacent(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return left1.isAdjacentTo(right1);
    }

    @ScalarOperator(OperatorType.NOT_RIGHT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notRight(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return left1.notEndsAfter(right1);

    }

    @ScalarOperator(OperatorType.NOT_LEFT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notLeft(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return left1.notStartsBefore(right1);

    }

    @ScalarOperator(OperatorType.ADD)
    @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME)
    public static Slice add(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return Int4Range.serialize(left1.union(right1));

    }

    @ScalarOperator(OperatorType.SUBTRACT)
    @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME)
    public static Slice minus(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return Int4Range.serialize(left1.minus(right1));

    }

    @ScalarOperator(OperatorType.MULTIPLY)
    @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME)
    public static Slice intesection(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return Int4Range.serialize(left1.intersection(right1));

    }

    @ScalarOperator(CAST)
    @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME)
    public static Slice castFromVarchar(
            @SqlType(StandardTypes.VARCHAR) Slice slice)
            throws ParseException
    {
        return Int4Range.serialize(rangeSerializer.parse(slice.toStringUtf8()));

    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equals(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        if (left1.hashCode() != right1.hashCode()) {
            return false;
        }
        return left1.equals(right1);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return !left1.equals(right1);

    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return left1.compareTo(right1) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return left1.compareTo(right1) <= 0;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return left1.compareTo(right1) > 0;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice left,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice right)
    {
        IntegerRange left1 = Int4Range.deSerialize(left);
        IntegerRange right1 = Int4Range.deSerialize(right);
        return left1.compareTo(right1) >= 0;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice value,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice min,
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice max)
    {
        IntegerRange min1 = Int4Range.deSerialize(min);
        IntegerRange max1 = Int4Range.deSerialize(max);
        IntegerRange value1 = Int4Range.deSerialize(value);
        return min1.compareTo(value1) <= 0 && value1.compareTo(max1) <= 0;
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(
            @SqlType(Int4RangeType.INT_4_RANGE_TYPE_NAME) Slice value)
    {
        IntegerRange value1 = Int4Range.deSerialize(value);
        return value1.hashCode();
    }
}
