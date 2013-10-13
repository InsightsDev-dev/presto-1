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
package com.facebook.presto.block.snappy;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.block.BlockEncoding;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.iq80.snappy.Snappy;

import javax.annotation.concurrent.GuardedBy;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class SnappyBlock
        implements Block
{
    private static final DataSize ENCODING_BUFFER_OVERHEAD = new DataSize(1, Unit.KILOBYTE);
    private final int positionCount;
    private final TupleInfo tupleInfo;
    private final Slice compressedSlice;
    private final BlockEncoding uncompressedBlockEncoding;

    @GuardedBy("this")
    private Block uncompressedBlock;

    public SnappyBlock(int positionCount, TupleInfo tupleInfo, Slice compressedSlice, BlockEncoding uncompressedBlockEncoding)
    {
        this.tupleInfo = checkNotNull(tupleInfo, "tupleInfo is null");
        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = positionCount;
        this.compressedSlice = checkNotNull(compressedSlice, "compressedSlice is null");
        this.uncompressedBlockEncoding = checkNotNull(uncompressedBlockEncoding, "uncompressedBlockEncoding is null");
    }

    public SnappyBlock(Block block)
    {
        tupleInfo = block.getTupleInfo();
        positionCount = block.getPositionCount();

        uncompressedBlock = block;
        uncompressedBlockEncoding = block.getEncoding();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(Ints.checkedCast(uncompressedBlock.getDataSize().toBytes() + ENCODING_BUFFER_OVERHEAD.toBytes()));
        uncompressedBlockEncoding.writeBlock(sliceOutput, uncompressedBlock);
        Slice uncompressedSlice = sliceOutput.slice();

        byte[] compressedBytes = new byte[Snappy.maxCompressedLength(uncompressedSlice.length())];
        int actualLength = Snappy.compress(uncompressedSlice.getBytes(), 0, uncompressedSlice.length(), compressedBytes, 0);
        compressedSlice = Slices.wrappedBuffer(Arrays.copyOf(compressedBytes, actualLength));
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public Slice getCompressedSlice()
    {
        return compressedSlice;
    }

    public synchronized Block getUncompressedBlock()
    {
        if (uncompressedBlock == null) {
            // decompress the slice
            int uncompressedLength = Snappy.getUncompressedLength(compressedSlice.getBytes(), 0);
            checkState(uncompressedLength > 0, "Empty block encountered!");
            byte[] output = new byte[uncompressedLength];
            Snappy.uncompress(compressedSlice.getBytes(), 0, compressedSlice.length(), output, 0);

            // decode the block
            uncompressedBlock = uncompressedBlockEncoding.readBlock(Slices.wrappedBuffer(output).getInput());
        }
        return uncompressedBlock;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public DataSize getDataSize()
    {
        return getUncompressedBlock().getDataSize();
    }

    @Override
    public BlockCursor cursor()
    {
        return getUncompressedBlock().cursor();
    }

    @Override
    public SnappyBlockEncoding getEncoding()
    {
        return new SnappyBlockEncoding(tupleInfo, uncompressedBlockEncoding);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        Preconditions.checkPositionIndexes(positionOffset, positionOffset + length, positionCount);
        return cursor().getRegionAndAdvance(length);
    }

    @Override
    public RandomAccessBlock toRandomAccessBlock()
    {
        return getUncompressedBlock().toRandomAccessBlock();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", positionCount)
                .add("tupleInfo", tupleInfo)
                .add("compressedSlice", compressedSlice)
                .toString();
    }

    @Override
    public Slice getRawSlice()
    {
        return getUncompressedBlock().getRawSlice();
    }
}
