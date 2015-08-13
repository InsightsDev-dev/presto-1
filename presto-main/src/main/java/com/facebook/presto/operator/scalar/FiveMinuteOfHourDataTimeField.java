
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DurationField;
import org.joda.time.DurationFieldType;
import org.joda.time.field.DividedDateTimeField;
import org.joda.time.field.OffsetDateTimeField;
import org.joda.time.field.ScaledDurationField;

// Forked from org.elasticsearch.common.joda.Joda
public final class FiveMinuteOfHourDataTimeField
        extends DateTimeFieldType
{
    private static final long serialVersionUID = -5677872459807379123L;

    private static final DurationFieldType FIVE_MINUTE_OF_HOUR_DURATION_FIELD_TYPE = new FiveMinuteOfHourDurationFieldType();

    public static final DateTimeFieldType FIVE_MINUTE_OF_HOUR = new FiveMinuteOfHourDataTimeField();

    private FiveMinuteOfHourDataTimeField()
    {
        super("fiveMinuteOfHour");
    }

    @Override
    public DurationFieldType getDurationType()
    {
        return FIVE_MINUTE_OF_HOUR_DURATION_FIELD_TYPE;
    }

    @Override
    public DurationFieldType getRangeDurationType()
    {
        return DurationFieldType.minutes();
    }

    @Override
    public DateTimeField getField(Chronology chronology)
    {
        return new DividedDateTimeField(chronology.minuteOfDay(), FIVE_MINUTE_OF_HOUR, 5);
    }

    private static class FiveMinuteOfHourDurationFieldType
            extends DurationFieldType
    {
        private static final long serialVersionUID = -8167713675442491871L;

        public FiveMinuteOfHourDurationFieldType()
        {
            super("fiveminute");
        }

        @Override
        public DurationField getField(Chronology chronology)
        {
            return new ScaledDurationField(chronology.minutes(), FIVE_MINUTE_OF_HOUR_DURATION_FIELD_TYPE, 5);
        }
    }
}
