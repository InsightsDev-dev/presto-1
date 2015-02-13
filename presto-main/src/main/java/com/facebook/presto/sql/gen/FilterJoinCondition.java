package com.facebook.presto.sql.gen;

import java.util.List;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;

public interface FilterJoinCondition {
	public boolean applyFilter(final ConnectorSession connectorSession,final List<Block> probeBlocks,
	        final List<List<Block>> buildBlocks, final int n, final int n2,final int blockIndex);
}
