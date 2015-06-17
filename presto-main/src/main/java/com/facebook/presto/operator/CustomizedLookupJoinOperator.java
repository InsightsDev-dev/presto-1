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

import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.FilterJoinCondition;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.longs.LongIterator;

import java.io.Closeable;
import java.util.List;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.LOOKUP_OUTER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.facebook.presto.util.MoreFutures.tryGetUnchecked;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * 
 * @author dilip kasana
 * @Date 13-Feb-2015
 */
public class CustomizedLookupJoinOperator implements Operator, Closeable {
	private final ListenableFuture<LookupSource> lookupSourceFuture;

	private final OperatorContext operatorContext;
	private final JoinProbeFactory joinProbeFactory;
	private final boolean enableOuterJoin;
	private final List<Type> types;
	private final List<Type> probeTypes;
	private final PageBuilder pageBuilder;
	FilterJoinCondition filterJoinCondition;
	Page probePage;
	private final boolean lookupOnOuterSide;
	private final boolean probeOnOuterSide;
	private LookupSource lookupSource;
	private JoinProbe probe;
	private boolean finishing;
	private long joinPosition = -1;
	private boolean noRowJoinedWithThisRow = true;

	private LongIterator unvisitedJoinPositions;

	public CustomizedLookupJoinOperator(OperatorContext operatorContext,
			LookupSourceSupplier lookupSourceSupplier, List<Type> probeTypes,
			JoinType joinType, JoinProbeFactory joinProbeFactory,
			boolean enableOuterJoin, FilterJoinCondition filterJoinCondition) {
		this.operatorContext = checkNotNull(operatorContext,
				"operatorContext is null");

		// todo pass in desired projection
		checkNotNull(lookupSourceSupplier, "lookupSourceSupplier is null");
		checkNotNull(probeTypes, "probeTypes is null");

		this.lookupSourceFuture = lookupSourceSupplier
				.getLookupSource(operatorContext);
		this.joinProbeFactory = joinProbeFactory;
		this.enableOuterJoin = enableOuterJoin;
		// Cannot use switch case here, because javac will synthesize an inner
		// class and cause IllegalAccessError
		probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;
		lookupOnOuterSide = joinType == LOOKUP_OUTER || joinType == FULL_OUTER;

		this.types = ImmutableList.<Type> builder().addAll(probeTypes)
				.addAll(lookupSourceSupplier.getTypes()).build();
		this.probeTypes = probeTypes;
		this.pageBuilder = new PageBuilder(types);
		this.filterJoinCondition = filterJoinCondition;
	}

	@Override
	public OperatorContext getOperatorContext() {
		return operatorContext;
	}

	@Override
	public List<Type> getTypes() {
		return types;
	}

	@Override
	public void finish() {
		finishing = true;
	}

	@Override
	public boolean isFinished() {
		boolean finished = finishing
				&& probe == null
				&& pageBuilder.isEmpty()
				&& (!lookupOnOuterSide || (unvisitedJoinPositions != null && !unvisitedJoinPositions
						.hasNext()));

		// if finished drop references so memory is freed early
		if (finished) {
			if (lookupSource != null) {
				lookupSource.close();
				lookupSource = null;
			}
			probe = null;
			pageBuilder.reset();
		}
		return finished;
	}

	@Override
	public ListenableFuture<?> isBlocked() {
		return lookupSourceFuture;
	}

	@Override
	public boolean needsInput() {
		if (finishing) {
			return false;
		}

		if (lookupSource == null) {
			lookupSource = tryGetUnchecked(lookupSourceFuture);
		}
		return lookupSource != null && probe == null;
	}

	@Override
	public void addInput(Page page) {
		checkNotNull(page, "page is null");
		checkState(!finishing, "Operator is finishing");
		checkState(lookupSource != null, "Lookup source has not been built yet");
		checkState(probe == null,
				"Current page has not been completely processed yet");

		// create probe
		probe = joinProbeFactory.createJoinProbe(lookupSource, page);
		probePage = page;
		// initialize to invalid join position to force output code to advance
		// the cursors
		joinPosition = -1;
	}

	@Override
	public Page getOutput() {
		// If needsInput was never called, lookupSource has not been initialized
		// so far.
		if (lookupSource == null) {
			lookupSource = tryGetUnchecked(lookupSourceFuture);
			if (lookupSource == null) {
				return null;
			}
		}
		// join probe page with the lookup source
		if (probe != null) {
			while (joinCurrentPosition()) {
				if (!advanceProbePosition()) {
					break;
				}
				if (!outerJoinCurrentPosition()) {
					break;
				}
			}
		}

		if (lookupOnOuterSide && finishing && probe == null) {
			buildSideOuterJoinUnvisitedPositions();
		}

		// only flush full pages unless we are done
		if (pageBuilder.isFull()
				|| (finishing && !pageBuilder.isEmpty() && probe == null)) {
			Page page = pageBuilder.build();
			pageBuilder.reset();
			return page;
		}

		return null;
	}

	@Override
	public void close() {
		if (lookupSource != null) {
			lookupSource.close();
			lookupSource = null;
		}
	}

	private boolean joinCurrentPosition() {
		/*
		 * if join position is greater than 0 than set flag to true;when we
		 * process set flag to false;check condition here while we have a
		 * position to join against...
		 */
		boolean thereExistsARowToJoin = false;
		while (joinPosition >= 0) {
			CustomizedInMemoryJoinHash customizedInMemoryJoinHash = (CustomizedInMemoryJoinHash) lookupSource;
			boolean isFilterEveluvateToTrue = customizedInMemoryJoinHash
					.applyFilter(operatorContext.getSession()
							.toConnectorSession(), filterJoinCondition,
							((SimpleJoinProbe) probe).getBlocks(),
							((SimpleJoinProbe) probe).getPosition(),
							joinPosition);
			thereExistsARowToJoin = true;
			if (isFilterEveluvateToTrue) {
				noRowJoinedWithThisRow = false;
				pageBuilder.declarePosition();
				// write probe columns
				probe.appendTo(pageBuilder);

				// write build columns
				lookupSource.appendTo(joinPosition, pageBuilder,
						probe.getChannelCount());
			}
			// get next join position for this row
			joinPosition = lookupSource.getNextJoinPosition(joinPosition);
			if (pageBuilder.isFull()) {
				return false;
			}
		}
		if (thereExistsARowToJoin && noRowJoinedWithThisRow) {
			if (!outerJoinCurrentPosition()) {
				return false;
			}
		}
		noRowJoinedWithThisRow = true;
		return true;
	}

	private boolean advanceProbePosition() {
		if (!probe.advanceNextPosition()) {
			probe = null;
			return false;
		}

		// update join position
		joinPosition = probe.getCurrentJoinPosition();
		return true;
	}

	private boolean outerJoinCurrentPosition() {
		if (enableOuterJoin && probeOnOuterSide && joinPosition < 0) {
			// write probe columns
			pageBuilder.declarePosition();
			probe.appendTo(pageBuilder);

			// write nulls into build columns
			int outputIndex = probe.getChannelCount();
			for (int buildChannel = 0; buildChannel < lookupSource
					.getChannelCount(); buildChannel++) {
				pageBuilder.getBlockBuilder(outputIndex).appendNull();
				outputIndex++;
			}
			if (pageBuilder.isFull()) {
				return false;
			}
		}
		return true;
	}

	private void buildSideOuterJoinUnvisitedPositions() {
		if (unvisitedJoinPositions == null) {
			unvisitedJoinPositions = lookupSource.getUnvisitedJoinPositions();
		}

		while (unvisitedJoinPositions.hasNext()) {
			long buildSideOuterJoinPosition = unvisitedJoinPositions.nextLong();
			pageBuilder.declarePosition();

			// write nulls into probe columns
			for (int probeChannel = 0; probeChannel < probeTypes.size(); probeChannel++) {
				pageBuilder.getBlockBuilder(probeChannel).appendNull();
			}

			// write build columns
			lookupSource.appendTo(buildSideOuterJoinPosition, pageBuilder,
					probeTypes.size());

			if (pageBuilder.isFull()) {
				return;
			}
		}
	}
}
