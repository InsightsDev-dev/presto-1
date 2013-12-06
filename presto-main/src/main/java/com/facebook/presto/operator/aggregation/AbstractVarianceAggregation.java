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
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.array.DoubleBigArray;
import com.facebook.presto.util.array.LongBigArray;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.base.Preconditions.checkState;

/**
 * Generate the variance for a given set of values. This implements the
 * <a href="http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm">online algorithm</a>.
 */
public abstract class AbstractVarianceAggregation
        extends SimpleAggregationFunction
{
    protected final boolean population;

    /**
     * Describes the tuple used by to calculate the variance.
     */
    static final TupleInfo VARIANCE_CONTEXT_INFO = new TupleInfo(
            Type.FIXED_INT_64,  // n
            Type.DOUBLE,        // mean
            Type.DOUBLE);       // m2


    AbstractVarianceAggregation(boolean population, Type parameterType)
    {
        // Intermediate type should be a fixed width structure
        super(SINGLE_DOUBLE, SINGLE_VARBINARY, parameterType);

        this.population = population;
    }

    public static class VarianceGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final boolean inputIsLong;
        private final boolean population;
        private final boolean standardDeviation;

        private final LongBigArray counts;
        private final DoubleBigArray means;
        private final DoubleBigArray m2s;

        public static VarianceGroupedAccumulator longVarianceGrouped(int valueChannel)
        {
            return new VarianceGroupedAccumulator(valueChannel, true, false, false);
        }

        public static VarianceGroupedAccumulator longVariancePopulationGrouped(int valueChannel)
        {
            return new VarianceGroupedAccumulator(valueChannel, true, true, false);
        }

        public static VarianceGroupedAccumulator longStandardDeviationGrouped(int valueChannel)
        {
            return new VarianceGroupedAccumulator(valueChannel, true, false, true);
        }

        public static VarianceGroupedAccumulator longStandardDeviationPopulationGrouped(int valueChannel)
        {
            return new VarianceGroupedAccumulator(valueChannel, true, true, true);
        }

        public static VarianceGroupedAccumulator doubleVarianceGrouped(int valueChannel)
        {
            return new VarianceGroupedAccumulator(valueChannel, false, false, false);
        }

        public static VarianceGroupedAccumulator doubleVariancePopulationGrouped(int valueChannel)
        {
            return new VarianceGroupedAccumulator(valueChannel, false, true, false);
        }

        public static VarianceGroupedAccumulator doubleStandardDeviationGrouped(int valueChannel)
        {
            return new VarianceGroupedAccumulator(valueChannel, false, false, true);
        }

        public static VarianceGroupedAccumulator doubleStandardDeviationPopulationGrouped(int valueChannel)
        {
            return new VarianceGroupedAccumulator(valueChannel, false, true, true);
        }

        private VarianceGroupedAccumulator(int valueChannel, boolean inputIsLong, boolean population, boolean standardDeviation)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_VARBINARY);

            this.inputIsLong = inputIsLong;
            this.population = population;
            this.standardDeviation = standardDeviation;

            this.counts = new LongBigArray();
            this.means = new DoubleBigArray();
            this.m2s = new DoubleBigArray();
        }

        @Override
        public long getEstimatedSize()
        {
            return counts.sizeOf() + means.sizeOf() + m2s.sizeOf();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            means.ensureCapacity(groupIdsBlock.getGroupCount());
            m2s.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull(0)) {

                    long groupId = groupIdsBlock.getGroupId(position);
                    double inputValue;
                    if (inputIsLong) {
                        inputValue = values.getLong(0);
                    }
                    else {
                        inputValue = values.getDouble(0);
                    }

                    long currentCount = counts.get(groupId);
                    double currentMean = means.get(groupId);

                    // Use numerically stable variant
                    currentCount++;
                    double delta = inputValue - currentMean;
                    currentMean += (delta / currentCount);
                    // update m2 inline
                    m2s.add(groupId, (delta * (inputValue - currentMean)));

                    // write values back out
                    counts.set(groupId, currentCount);
                    means.set(groupId, currentMean);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        protected void processIntermediate(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            means.ensureCapacity(groupIdsBlock.getGroupCount());
            m2s.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull(0)) {
                    long groupId = groupIdsBlock.getGroupId(position);

                    Slice slice = values.getSlice(0);
                    long inputCount = VARIANCE_CONTEXT_INFO.getLong(slice, 0);
                    double inputMean = VARIANCE_CONTEXT_INFO.getDouble(slice, 1);
                    double inputM2 = VARIANCE_CONTEXT_INFO.getDouble(slice, 2);

                    long currentCount = counts.get(groupId);
                    double currentMean = means.get(groupId);
                    double currentM2 = m2s.get(groupId);

                    // Use numerically stable variant
                    long newCount = currentCount + inputCount;
                    double newMean = ((currentCount * currentMean) + (inputCount * inputMean)) / newCount;
                    double delta = inputMean - currentMean;
                    double newM2 = currentM2 + inputM2 + ((delta * delta) * (currentCount * inputCount)) / newCount;

                    counts.set(groupId, newCount);
                    means.set(groupId, newMean);
                    m2s.set(groupId, newM2);

                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            long count = counts.get((long) groupId);
            double mean = means.get((long) groupId);
            double m2 = m2s.get((long) groupId);

            Slice intermediateValue = Slices.allocate(VARIANCE_CONTEXT_INFO.getFixedSize());
            VARIANCE_CONTEXT_INFO.setNotNull(intermediateValue, 0);
            VARIANCE_CONTEXT_INFO.setLong(intermediateValue, 0, count);
            VARIANCE_CONTEXT_INFO.setDouble(intermediateValue, 1, mean);
            VARIANCE_CONTEXT_INFO.setDouble(intermediateValue, 2, m2);

            output.append(intermediateValue);
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            long count = counts.get((long) groupId);
            if (population) {
                if (count == 0) {
                    output.appendNull();
                }
                else {
                    double m2 = m2s.get((long) groupId);
                    double result = m2 / count;
                    if (standardDeviation) {
                        result = Math.sqrt(result);
                    }
                    output.append(result);
                }
            }
            else {
                if (count < 2) {
                    output.appendNull();
                }
                else {
                    double m2 = m2s.get((long) groupId);
                    double result = m2 / (count - 1);
                    if (standardDeviation) {
                        result = Math.sqrt(result);
                    }
                    output.append(result);
                }
            }
        }
    }

    public static class VarianceAccumulator
            extends SimpleAccumulator
    {
        private final boolean inputIsLong;
        private final boolean population;
        private final boolean standardDeviation;

        private long currentCount;
        private double currentMean;
        private double currentM2;

        public static VarianceAccumulator longVariance(int valueChannel)
        {
            return new VarianceAccumulator(valueChannel, true, false, false);
        }

        public static VarianceAccumulator longVariancePopulation(int valueChannel)
        {
            return new VarianceAccumulator(valueChannel, true, true, false);
        }

        public static VarianceAccumulator longStandardDeviation(int valueChannel)
        {
            return new VarianceAccumulator(valueChannel, true, false, true);
        }

        public static VarianceAccumulator longStandardDeviationPopulation(int valueChannel)
        {
            return new VarianceAccumulator(valueChannel, true, true, true);
        }

        public static VarianceAccumulator doubleVariance(int valueChannel)
        {
            return new VarianceAccumulator(valueChannel, false, false, false);
        }

        public static VarianceAccumulator doubleVariancePopulation(int valueChannel)
        {
            return new VarianceAccumulator(valueChannel, false, true, false);
        }

        public static VarianceAccumulator doubleStandardDeviation(int valueChannel)
        {
            return new VarianceAccumulator(valueChannel, false, false, true);
        }

        public static VarianceAccumulator doubleStandardDeviationPopulation(int valueChannel)
        {
            return new VarianceAccumulator(valueChannel, false, true, true);
        }

        private VarianceAccumulator(int valueChannel, boolean inputIsLong, boolean population, boolean standardDeviation)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_VARBINARY);

            this.inputIsLong = inputIsLong;
            this.population = population;
            this.standardDeviation = standardDeviation;
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull(0)) {
                    double inputValue;
                    if (inputIsLong) {
                        inputValue = values.getLong(0);
                    }
                    else {
                        inputValue = values.getDouble(0);
                    }

                    // Use numerically stable variant
                    currentCount++;
                    double delta = inputValue - currentMean;
                    currentMean += (delta / currentCount);
                    // update m2 inline
                    currentM2 += (delta * (inputValue - currentMean));
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        protected void processIntermediate(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull(0)) {
                    Slice slice = values.getSlice(0);
                    long inputCount = VARIANCE_CONTEXT_INFO.getLong(slice, 0);
                    double inputMean = VARIANCE_CONTEXT_INFO.getDouble(slice, 1);
                    double inputM2 = VARIANCE_CONTEXT_INFO.getDouble(slice, 2);

                    // Use numerically stable variant
                    long newCount = currentCount + inputCount;
                    double newMean = ((currentCount * currentMean) + (inputCount * inputMean)) / newCount;
                    double delta = inputMean - currentMean;
                    double newM2 = currentM2 + inputM2 + ((delta * delta) * (currentCount * inputCount)) / newCount;

                    currentCount = newCount;
                    currentMean = newMean;
                    currentM2 = newM2;

                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(BlockBuilder output)
        {
            Slice intermediateValue = Slices.allocate(VARIANCE_CONTEXT_INFO.getFixedSize());
            VARIANCE_CONTEXT_INFO.setNotNull(intermediateValue, 0);
            VARIANCE_CONTEXT_INFO.setLong(intermediateValue, 0, currentCount);
            VARIANCE_CONTEXT_INFO.setDouble(intermediateValue, 1, currentMean);
            VARIANCE_CONTEXT_INFO.setDouble(intermediateValue, 2, currentM2);

            output.append(intermediateValue);
        }

        @Override
        public void evaluateFinal(BlockBuilder output)
        {
            if (population) {
                if (currentCount == 0) {
                    output.appendNull();
                }
                else {
                    double result = currentM2 / currentCount;
                    if (standardDeviation) {
                        result = Math.sqrt(result);
                    }
                    output.append(result);
                }
            }
            else {
                if (currentCount < 2) {
                    output.appendNull();
                }
                else {
                    double result = currentM2 / (currentCount - 1);
                    if (standardDeviation) {
                        result = Math.sqrt(result);
                    }
                    output.append(result);
                }
            }
        }
    }
}
