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
import static com.facebook.presto.type.DateTimeOperators.modulo24Hour;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import io.airlift.slice.Slice;

import java.text.ParseException;

import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.DateTimeEncoding;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.util.DateTimeUtils;
import com.mobileum.range.TimeStampRange;
import com.mobileum.range.RangeSerializer;
import com.mobileum.range.TimeStamp;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class TSRangeOperators
{
    private TSRangeOperators()
    {

    }

    static RangeSerializer<TimeStamp> rangeSerializer = TimeStampRange.emptyRange.getSerializer();

    @ScalarFunction("tsrange")
    @SqlType(TSRangeType.TS_RANGE_TYPE_NAME)
    public static Slice tsrange(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long min,
            @SqlType(StandardTypes.TIMESTAMP) long max)
    {
//        long min1 = modulo24Hour(getChronology(session.getTimeZoneKey()), min);
//        long max1 = modulo24Hour(getChronology(session.getTimeZoneKey()), max);

        return TSRange.serialize(TSRange.createRange(min, max));
    }

    @ScalarFunction("tsrange")
    @SqlType(TSRangeType.TS_RANGE_TYPE_NAME)
    public static Slice tsrange(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long min,
            @SqlType(StandardTypes.TIMESTAMP) long max, @SqlType(StandardTypes.VARCHAR) Slice flags)
    {
//        long min1 = modulo24Hour(getChronology(session.getTimeZoneKey()), min);
//        long max1 = modulo24Hour(getChronology(session.getTimeZoneKey()), max);
        return TSRange.serialize(TSRange.createRange(min, max, flags.toStringUtf8()));
    }

    @ScalarFunction("tsrange")
    @SqlType(TSRangeType.TS_RANGE_TYPE_NAME)
    public static Slice tsrange(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice min,
            @SqlType(StandardTypes.VARCHAR) Slice max)
    {
        long min1 = parseTimestamp(session.getTimeZoneKey(), min.toStringUtf8());

        long max1 = parseTimestamp(session.getTimeZoneKey(), max.toStringUtf8());

        return TSRange.serialize(TSRange.createRange(min1, max1));
    }

    private static long parseTimestamp(TimeZoneKey timeZoneKey, String value)
    {

        try {
            long millis = DateTimeUtils.parseTimestampWithTimeZone(value);
            return DateTimeEncoding.unpackMillisUtc(millis);
        }
        catch (Exception e) {
            long millisUtc = DateTimeUtils.parseTimestampWithoutTimeZone(timeZoneKey, value);
            return millisUtc;
        }
    }

    @ScalarFunction("tsrange")
    @SqlType(TSRangeType.TS_RANGE_TYPE_NAME)
    public static Slice tsrange(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice min,
            @SqlType(StandardTypes.VARCHAR) Slice max, @SqlType(StandardTypes.VARCHAR) Slice flags)
    {
        long min1 = parseTimestamp(session.getTimeZoneKey(), min.toStringUtf8());
        long max1 = parseTimestamp(session.getTimeZoneKey(), max.toStringUtf8());
        return TSRange.serialize(TSRange.createRange(min1, max1, flags.toStringUtf8()));
    }

    @Description("display as human readable format")
    @ScalarFunction("lower")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long lower(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice slice)
    {
        return TSRange.deSerialize(slice).lower().getTimestamp();
    }

    @Description("display as human readable format")
    @ScalarFunction("upper")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long upper(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice slice)
    {
        return TSRange.deSerialize(slice).upper().getTimestamp();
    }

    // LEFT_CONTAINS_RIGHT
    @ScalarOperator(LEFT_CONTAINS_RIGHT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean leftContainsRight(ConnectorSession session,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
            throws ParseException
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return left1.includes(right1);
    }

    @ScalarOperator(LEFT_CONTAINS_RIGHT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean leftContainsRightValue(ConnectorSession session,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        //long value1 = modulo24Hour(getChronology(session.getTimeZoneKey()), value);
        return left1.includes(new TimeStamp(value));
    }

    @ScalarOperator(RIGHT_CONTAINS_LEFT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean rightContains(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return right1.includes(left1);
    }

    @ScalarOperator(RIGHT_CONTAINS_LEFT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean rightContainsleft(ConnectorSession session,
            @SqlType(StandardTypes.TIMESTAMP) long left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange right1 = TSRange.deSerialize(right);
        //long left1 = modulo24Hour(getChronology(session.getTimeZoneKey()), left);

        return right1.includes(new TimeStamp(left));
    }

    @ScalarOperator(OperatorType.OVERLAPPING_WITH)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean overlaps(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return left1.overlaps(right1);
    }

    @ScalarOperator(OperatorType.STRICTLY_LEFT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean strictLeft(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return left1.before(right1);
    }

    @ScalarOperator(OperatorType.STRICTLY_RIGHT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean strictRight(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return left1.after(right1);
    }

    @ScalarOperator(OperatorType.ADJACENT_WITH)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean adjacent(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return left1.isAdjacentTo(right1);
    }

    @ScalarOperator(OperatorType.NOT_RIGHT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notRight(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return left1.notEndsAfter(right1);

    }

    @ScalarOperator(OperatorType.NOT_LEFT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notLeft(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return left1.notStartsBefore(right1);

    }

    @ScalarOperator(OperatorType.ADD)
    @SqlType(TSRangeType.TS_RANGE_TYPE_NAME)
    public static Slice add(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return TSRange.serialize(left1.union(right1));

    }

    @ScalarOperator(OperatorType.SUBTRACT)
    @SqlType(TSRangeType.TS_RANGE_TYPE_NAME)
    public static Slice minus(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return TSRange.serialize(left1.minus(right1));

    }

    @ScalarOperator(OperatorType.MULTIPLY)
    @SqlType(TSRangeType.TS_RANGE_TYPE_NAME)
    public static Slice intesection(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return TSRange.serialize(left1.intersection(right1));

    }

    @ScalarOperator(CAST)
    @SqlType(TSRangeType.TS_RANGE_TYPE_NAME)
    public static Slice castFromVarchar(ConnectorSession session,
            @SqlType(StandardTypes.VARCHAR) Slice slice)
            throws ParseException
    {
        return TSRange.serialize(rangeSerializer.parse(slice.toStringUtf8(),session));

    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equals(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        if (left1.hashCode() != right1.hashCode()) {
            return false;
        }
        return left1.equals(right1);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return !left1.equals(right1);

    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return left1.compareTo(right1) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return left1.compareTo(right1) <= 0;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return left1.compareTo(right1) > 0;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampRange left1 = TSRange.deSerialize(left);
        TimeStampRange right1 = TSRange.deSerialize(right);
        return left1.compareTo(right1) >= 0;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice value,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice min,
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice max)
    {
        TimeStampRange min1 = TSRange.deSerialize(min);
        TimeStampRange max1 = TSRange.deSerialize(max);
        TimeStampRange value1 = TSRange.deSerialize(value);
        return min1.compareTo(value1) <= 0 && value1.compareTo(max1) <= 0;
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(
            @SqlType(TSRangeType.TS_RANGE_TYPE_NAME) Slice value)
    {
        TimeStampRange value1 = TSRange.deSerialize(value);
        return value1.hashCode();
    }
}
