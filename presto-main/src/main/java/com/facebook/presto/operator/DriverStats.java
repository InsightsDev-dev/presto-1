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
package com.facebook.presto.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class DriverStats
{
    private final DateTime createTime;
    private final DateTime startTime;
    private final DateTime endTime;

    private final Duration queuedTime;
    private final Duration elapsedTime;

    private final DataSize memoryReservation;

    private final Duration totalScheduledTime;
    private final Duration totalCpuTime;
    private final Duration totalUserTime;
    private final Duration totalBlockedTime;

    private final DataSize rawInputDataSize;
    private final long rawInputPositions;

    private final DataSize processedInputDataSize;
    private final long processedInputPositions;

    private final DataSize outputDataSize;
    private final long outputPositions;

    private final List<OperatorStats> operatorStats;

    @JsonCreator
    public DriverStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("startTime") DateTime startTime,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("elapsedTime") Duration elapsedTime,

            @JsonProperty("memoryReservation") DataSize memoryReservation,

            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("totalUserTime") Duration totalUserTime,
            @JsonProperty("totalBlockedTime") Duration totalBlockedTime,

            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,

            @JsonProperty("processedInputDataSize") DataSize processedInputDataSize,
            @JsonProperty("processedInputPositions") long processedInputPositions,

            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("operatorStats") List<OperatorStats> operatorStats)
    {
        this.createTime = checkNotNull(createTime, "createTime is null");
        this.startTime = startTime;
        this.endTime = endTime;
        this.queuedTime = checkNotNull(queuedTime, "queuedTime is null");
        this.elapsedTime = checkNotNull(elapsedTime, "elapsedTime is null");

        this.memoryReservation = checkNotNull(memoryReservation, "memoryReservation is null");

        this.totalScheduledTime = checkNotNull(totalScheduledTime, "totalScheduledTime is null");
        this.totalCpuTime = checkNotNull(totalCpuTime, "totalCpuTime is null");
        this.totalUserTime = checkNotNull(totalUserTime, "totalUserTime is null");
        this.totalBlockedTime = checkNotNull(totalBlockedTime, "totalBlockedTime is null");

        this.rawInputDataSize = checkNotNull(rawInputDataSize, "rawInputDataSize is null");
        Preconditions.checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;

        this.processedInputDataSize = checkNotNull(processedInputDataSize, "processedInputDataSize is null");
        Preconditions.checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        this.outputDataSize = checkNotNull(outputDataSize, "outputDataSize is null");
        Preconditions.checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        this.operatorStats = ImmutableList.copyOf(checkNotNull(operatorStats, "operatorStats is null"));
    }

    @JsonProperty
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @Nullable
    @JsonProperty
    public DateTime getStartTime()
    {
        return startTime;
    }

    @Nullable
    @JsonProperty
    public DateTime getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    public Duration getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    public DataSize getMemoryReservation()
    {
        return memoryReservation;
    }

    @JsonProperty
    public Duration getTotalScheduledTime()
    {
        return totalScheduledTime;
    }

    @JsonProperty
    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    @JsonProperty
    public Duration getTotalUserTime()
    {
        return totalUserTime;
    }

    @JsonProperty
    public Duration getTotalBlockedTime()
    {
        return totalBlockedTime;
    }

    @JsonProperty
    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    @JsonProperty
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    public DataSize getProcessedInputDataSize()
    {
        return processedInputDataSize;
    }

    @JsonProperty
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    @JsonProperty
    public DataSize getOutputDataSize()
    {
        return outputDataSize;
    }

    @JsonProperty
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    public List<OperatorStats> getOperatorStats()
    {
        return operatorStats;
    }
}
