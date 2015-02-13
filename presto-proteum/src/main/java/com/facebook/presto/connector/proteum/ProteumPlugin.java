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
package com.facebook.presto.connector.proteum;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mobileum.range.presto.Int4RangeType;
import com.mobileum.range.presto.TSRangeType;
import com.mobileum.range.presto.TSTZRangeType;

public class ProteumPlugin implements Plugin
{
    private TypeManager typeManager;
    private Map<String, String> optionalConfig = ImmutableMap.of();

    @Override
    public synchronized void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
    }
    @Inject
    public synchronized void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    public synchronized Map<String, String> getOptionalConfig()
    {
        return optionalConfig;
    }

    @Override
    public synchronized <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type.cast(new ProteumConnectorFactory(typeManager, getOptionalConfig())));
        }else if (type == FunctionFactory.class) {
            return ImmutableList.of(type.cast(new ProteumFunctionFactory(
                    this.typeManager)));
        }
        else if (type == Type.class) {
            List<T> types = new ArrayList<T>();
            types.add(type.cast(Int4RangeType.INT_4_RANGE_TYPE));
            types.add(type.cast(TSTZRangeType.TSTZ_RANGE_TYPE));
            types.add(type.cast(TSRangeType.TS_RANGE_TYPE));
            return types;
        }
        return ImmutableList.of();
    }
}