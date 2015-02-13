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
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
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
import com.mobileum.range.TimeStampWithTimeZoneRange;
import com.mobileum.range.RangeSerializer;
import com.mobileum.range.TimeStampWithTimeZone;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class TSTZRangeOperators
{
    private TSTZRangeOperators()
    {

    }

    static RangeSerializer<TimeStampWithTimeZone> rangeSerializer = TimeStampWithTimeZoneRange.emptyRange.getSerializer();

    @ScalarFunction("tstzrange")
    @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME)
    public static Slice tstzrange(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long min,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long max)
    {

        return TSTZRange.serialize(TSTZRange.createRange(min, max));
    }

    @ScalarFunction("tstzrange")
    @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME)
    public static Slice tstzrange(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long min,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long max, @SqlType(StandardTypes.VARCHAR) Slice flags)
    {
        return TSTZRange.serialize(TSTZRange.createRange(min, max, flags.toStringUtf8()));
    }

    @ScalarFunction("tstzrange")
    @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME)
    public static Slice tstzrange(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice min,
            @SqlType(StandardTypes.VARCHAR) Slice max)
    {
        long min1 = parseTimestamp(session.getTimeZoneKey(), min.toStringUtf8());

        long max1 = parseTimestamp(session.getTimeZoneKey(), max.toStringUtf8());

        return TSTZRange.serialize(TSTZRange.createRange(min1, max1));
    }

    private static long parseTimestamp(TimeZoneKey timeZoneKey, String value)
    {

        try {
            return DateTimeUtils.parseTimestampWithTimeZone(value);
        }
        catch (Exception e) {
            long millisUtc= DateTimeUtils.parseTimestampWithoutTimeZone(timeZoneKey, value);
            return DateTimeEncoding.packDateTimeWithZone(millisUtc, timeZoneKey);
        }
    }

    @ScalarFunction("tstzrange")
    @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME)
    public static Slice tstzrange(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice min,
            @SqlType(StandardTypes.VARCHAR) Slice max, @SqlType(StandardTypes.VARCHAR) Slice flags)
    {
        long min1 = parseTimestamp(session.getTimeZoneKey(), min.toStringUtf8());
        long max1 = parseTimestamp(session.getTimeZoneKey(), max.toStringUtf8());
        return TSTZRange.serialize(TSTZRange.createRange(min1, max1, flags.toStringUtf8()));
    }

    @Description("display as human readable format")
    @ScalarFunction("lower")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long lower(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice slice)
    {
        return TSTZRange.deSerialize(slice).lower().getTimestamp();
    }

    @Description("display as human readable format")
    @ScalarFunction("upper")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long upper(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice slice)
    {
        return TSTZRange.deSerialize(slice).upper().getTimestamp();
    }

    // LEFT_CONTAINS_RIGHT
    @ScalarOperator(LEFT_CONTAINS_RIGHT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean leftContainsRight(ConnectorSession session,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
            throws ParseException
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return left1.includes(right1);
    }

    @ScalarOperator(LEFT_CONTAINS_RIGHT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean leftContainsRightValue(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        return left1.includes(new TimeStampWithTimeZone(value));
    }

    @ScalarOperator(RIGHT_CONTAINS_LEFT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean rightContains(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return right1.includes(left1);
    }

    @ScalarOperator(RIGHT_CONTAINS_LEFT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean rightContainsleft(
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return right1.includes(new TimeStampWithTimeZone(left));
    }

    @ScalarOperator(OperatorType.OVERLAPPING_WITH)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean overlaps(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return left1.overlaps(right1);
    }

    @ScalarOperator(OperatorType.STRICTLY_LEFT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean strictLeft(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return left1.before(right1);
    }

    @ScalarOperator(OperatorType.STRICTLY_RIGHT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean strictRight(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return left1.after(right1);
    }

    @ScalarOperator(OperatorType.ADJACENT_WITH)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean adjacent(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return left1.isAdjacentTo(right1);
    }

    @ScalarOperator(OperatorType.NOT_RIGHT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notRight(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return left1.notEndsAfter(right1);

    }

    @ScalarOperator(OperatorType.NOT_LEFT)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notLeft(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return left1.notStartsBefore(right1);

    }

    @ScalarOperator(OperatorType.ADD)
    @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME)
    public static Slice add(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return TSTZRange.serialize(left1.union(right1));

    }

    @ScalarOperator(OperatorType.SUBTRACT)
    @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME)
    public static Slice minus(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return TSTZRange.serialize(left1.minus(right1));

    }

    @ScalarOperator(OperatorType.MULTIPLY)
    @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME)
    public static Slice intesection(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return TSTZRange.serialize(left1.intersection(right1));

    }

    @ScalarOperator(CAST)
    @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME)
    public static Slice castFromVarchar(
            @SqlType(StandardTypes.VARCHAR) Slice slice)
            throws ParseException
    {
        return TSTZRange.serialize(rangeSerializer.parse(slice.toStringUtf8()));

    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equals(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        if (left1.hashCode() != right1.hashCode()) {
            return false;
        }
        return left1.equals(right1);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return !left1.equals(right1);

    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return left1.compareTo(right1) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return left1.compareTo(right1) <= 0;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return left1.compareTo(right1) > 0;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice left,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice right)
    {
        TimeStampWithTimeZoneRange left1 = TSTZRange.deSerialize(left);
        TimeStampWithTimeZoneRange right1 = TSTZRange.deSerialize(right);
        return left1.compareTo(right1) >= 0;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice value,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice min,
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice max)
    {
        TimeStampWithTimeZoneRange min1 = TSTZRange.deSerialize(min);
        TimeStampWithTimeZoneRange max1 = TSTZRange.deSerialize(max);
        TimeStampWithTimeZoneRange value1 = TSTZRange.deSerialize(value);
        return min1.compareTo(value1) <= 0 && value1.compareTo(max1) <= 0;
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(
            @SqlType(TSTZRangeType.TSTZ_RANGE_TYPE_NAME) Slice value)
    {
        TimeStampWithTimeZoneRange value1 = TSTZRange.deSerialize(value);
        return value1.hashCode();
    }
}
