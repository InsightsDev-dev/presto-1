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

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class ExchangeClientConfig
{
    private DataSize maxBufferSize = new DataSize(32, Unit.MEGABYTE);
    private int concurrentRequestMultiplier = 3;

    @NotNull
    public DataSize getMaxBufferSize()
    {
        return maxBufferSize;
    }

    @Config("exchange.max-buffer-size")
    public ExchangeClientConfig setMaxBufferSize(DataSize maxBufferSize)
    {
        this.maxBufferSize = maxBufferSize;
        return this;
    }

    @Min(1)
    public int getConcurrentRequestMultiplier()
    {
        return concurrentRequestMultiplier;
    }

    @Config("exchange.concurrent-request-multiplier")
    public ExchangeClientConfig setConcurrentRequestMultiplier(int concurrentRequestMultiplier)
    {
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        return this;
    }
}
