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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockBuilderStatus;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

import static com.facebook.presto.operator.aggregation.VarianceAggregations.DOUBLE_STDDEV_POP_INSTANCE;
import static com.facebook.presto.type.DoubleType.DOUBLE;

public class TestDoubleStdDevPopAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(new BlockBuilderStatus());
        for (int i = start; i < start + length; i++) {
            blockBuilder.append((double) i);
        }
        return blockBuilder.build();
    }

    @Override
    public AggregationFunction getFunction()
    {
        return DOUBLE_STDDEV_POP_INSTANCE;
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        double[] values = new double[length];
        for (int i = 0; i < length; i++) {
            values[i] = start + i;
        }

        StandardDeviation stdDev = new StandardDeviation(false);
        return stdDev.evaluate(values);
    }
}
