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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.FilterJoinCondition;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class CustomizedLookupJoinOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final LookupSourceSupplier lookupSourceSupplier;
    private final List<Type> probeTypes;
    private final boolean enableOuterJoin;
    private final List<Type> types;
    private final JoinProbeFactory joinProbeFactory;
    private boolean closed;
    private final FilterJoinCondition filterJoinCondition;
	public CustomizedLookupJoinOperatorFactory(int operatorId,
            LookupSourceSupplier lookupSourceSupplier,
            List<Type> probeTypes,
            boolean enableOuterJoin,
            JoinProbeFactory joinProbeFactory,
            FilterJoinCondition filterJoinCondition)
    {
        this.operatorId = operatorId;
        this.lookupSourceSupplier = lookupSourceSupplier;
        this.probeTypes = probeTypes;
        this.enableOuterJoin = enableOuterJoin;

        this.joinProbeFactory = joinProbeFactory;

        this.types = ImmutableList.<Type>builder()
                .addAll(probeTypes)
                .addAll(lookupSourceSupplier.getTypes())
                .build();
        this.filterJoinCondition=filterJoinCondition;
        
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, LookupJoinOperator.class.getSimpleName());
        return new CustomizedLookupJoinOperator(operatorContext, lookupSourceSupplier, probeTypes, enableOuterJoin, joinProbeFactory,filterJoinCondition);
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
