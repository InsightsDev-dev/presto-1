package com.facebook.presto.operator.window;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

import java.util.List;

import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;

public abstract class SumFunction implements WindowFunction {
	public static class BigintSumFunction extends SumFunction {
		private long sum;
		public BigintSumFunction(List<Integer> argumentChannels) {
			super(BIGINT, argumentChannels);
		}
		
		@Override
		public void reset(int partitionStartPosition, int partitionRowCount,
				PagesIndex pagesIndex) {
			this.sum=0;
			super.reset(partitionStartPosition, partitionRowCount, pagesIndex);
		}
		
		@Override
		public void processRow(BlockBuilder output, boolean newPeerGroup,
				int peerGroupCount) {

			long valuePosition = currentPosition;

			if ((valuePosition >= 0)
					&& (valuePosition < (partitionStartPosition + partitionRowCount))) {
				sum += pagesIndex.getLong(valueChannel,
						Ints.checkedCast(valuePosition));
				type.writeLong(output, sum);
			}

			currentPosition++;
		}
	}

	public static class DoubleSumFunction extends SumFunction {
		private double sum;
		public DoubleSumFunction(List<Integer> argumentChannels) {
			super(DOUBLE, argumentChannels);
		}
		@Override
		public void reset(int partitionStartPosition, int partitionRowCount,
				PagesIndex pagesIndex) {
			this.sum=0;
			super.reset(partitionStartPosition, partitionRowCount, pagesIndex);
		}
		
		@Override
		public void processRow(BlockBuilder output, boolean newPeerGroup,
				int peerGroupCount) {

			long valuePosition = currentPosition;

			if ((valuePosition >= 0)
					&& (valuePosition < (partitionStartPosition + partitionRowCount))) {
				sum += pagesIndex.getDouble(valueChannel,
						Ints.checkedCast(valuePosition));
				type.writeDouble(output, sum);
			}

			currentPosition++;
		}
	}

	protected final Type type;
	protected int partitionStartPosition;
	protected int currentPosition;
	protected int partitionRowCount;
	protected PagesIndex pagesIndex;
	protected int valueChannel;

	public SumFunction(Type type, List<Integer> argumentChannels) {
		this.type = type;
		this.valueChannel = argumentChannels.get(0);
	}

	@Override
	public Type getType() {
		return type;
	}

	@Override
	public void reset(int partitionStartPosition, int partitionRowCount,
			PagesIndex pagesIndex) {
		this.partitionStartPosition = partitionStartPosition;
		this.currentPosition = partitionStartPosition;
		this.partitionRowCount = partitionRowCount;
		this.pagesIndex = pagesIndex;
	}

	
}
