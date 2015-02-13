package com.mobileum.range.presto;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import io.airlift.slice.Slice;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.mobileum.range.IntegerRange;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015 
 */
public class Int4RangeType
        extends AbstractVariableWidthType
{
    public static final Int4RangeType INT_4_RANGE_TYPE = new Int4RangeType();
    public static final String INT_4_RANGE_TYPE_NAME = "int4range";

    @JsonCreator
    public Int4RangeType()
    {
        super(parseTypeSignature(INT_4_RANGE_TYPE_NAME), Slice.class);
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
        IntegerRange d = getValue(block, position);
        if (d == null) {
            return null;
        }
        String str = d.toString();
        return str;
    }

    private IntegerRange getValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return Int4Range.deSerialize(block.getSlice(position, 0,
                block.getLength(position)));
    }

    private boolean equalTo(IntegerRange left, IntegerRange right)
    {
        return left.equals(right);
    }

    private int compareTo(IntegerRange left, IntegerRange right)
    {
        return left.compareTo(right);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock,
            int rightPosition)
    {
        IntegerRange left = getValue(leftBlock, leftPosition);
        IntegerRange right = getValue(rightBlock, rightPosition);
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
        IntegerRange left = getValue(leftBlock, leftPosition);
        IntegerRange right = getValue(rightBlock, rightPosition);
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
