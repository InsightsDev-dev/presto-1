package com.mobileum.range.presto;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import io.airlift.slice.Slice;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.mobileum.range.TimeStampWithTimeZoneRange;
public class TSTZRangeType
        extends AbstractVariableWidthType
{
    public static final TSTZRangeType TSTZ_RANGE_TYPE = new TSTZRangeType();
    public static final String TSTZ_RANGE_TYPE_NAME = "tstzrange";

    @JsonCreator
    public TSTZRangeType()
    {
        super(parseTypeSignature(TSTZ_RANGE_TYPE_NAME), Slice.class);
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block,
            int position)
    {
        TimeStampWithTimeZoneRange d = getValue(block, position);
        if (d == null) {
            return null;
        }
        String str = d.toString();
        return str;
    }

    private TimeStampWithTimeZoneRange getValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return TSTZRange.deSerialize(block.getSlice(position, 0,
                block.getLength(position)));
    }

    private boolean equalTo(TimeStampWithTimeZoneRange left, TimeStampWithTimeZoneRange right)
    {
        return left.equals(right);
    }

    private int compareTo(TimeStampWithTimeZoneRange left, TimeStampWithTimeZoneRange right)
    {
        return left.compareTo(right);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock,
            int rightPosition)
    {
        TimeStampWithTimeZoneRange left = getValue(leftBlock, leftPosition);
        TimeStampWithTimeZoneRange right = getValue(rightBlock, rightPosition);
        try {
            return equalTo(left, right);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int hash(Block block, int position)
    {
        return getValue(block, position).hashCode();
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock,
            int rightPosition)
    {
        TimeStampWithTimeZoneRange left = getValue(leftBlock, leftPosition);
        TimeStampWithTimeZoneRange right = getValue(rightBlock, rightPosition);
        try {
            return compareTo(left, right);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writeBytesTo(position, 0, block.getLength(position),
                    blockBuilder);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getLength(position));
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset,
            int length)
    {
        blockBuilder.writeBytes(value, offset, length).closeEntry();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        return new VariableWidthBlockBuilder(blockBuilderStatus);
    }
}
