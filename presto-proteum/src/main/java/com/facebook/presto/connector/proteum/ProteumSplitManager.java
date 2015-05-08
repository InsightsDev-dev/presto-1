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

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.sql.planner.optimizations.ProteumTupleDomain;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mobileum.range.presto.ExpressionCheckerVisitor;
import com.mobileum.range.presto.ExpressionFormatter;

public class ProteumSplitManager implements ConnectorSplitManager {
	private final String connectorId;
	private final ProteumClient client;
	private final ProteumConfig config;

	@Inject
	public ProteumSplitManager(@Named("connectorId") String connectorId,
			ProteumClient client, ProteumConfig config) {
		this.connectorId = connectorId;
		this.client = client;
		this.config = config;
	}

	public static Expression fromPredicate(Expression expression) {
		Expression e = new com.mobileum.range.presto.RewritingVisitor<Void>()
				.process(
						expression,
						new com.mobileum.range.presto.Context<Void>(null, false));
		return com.mobileum.range.presto.RewritingVisitor.handleBoolean(e);
	}

	@Override
	public ConnectorPartitionResult getPartitions(
			ConnectorTableHandle tableHandle,
			TupleDomain<ConnectorColumnHandle> tupleDomain) {
		ProteumTableHandle proteumTableHandle = (ProteumTableHandle) tableHandle;
		if (!config.getApplyFilter()) {
			List<ConnectorPartition> partitions = ImmutableList
					.<ConnectorPartition> of(new ProteumPartition(
							proteumTableHandle.getSchemaName(),
							proteumTableHandle.getTableName(),
							new ProteumPredicatePushDown(Lists
									.<ProteumColumnFilter> newArrayList())));
			if (tupleDomain instanceof ProteumTupleDomain) {
				((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
						.setAggregatePushDownable(false);
			}
			return new ConnectorPartitionResult(partitions, tupleDomain);
		}
		if (!config.getApplyGroupBy()
				&& tupleDomain instanceof ProteumTupleDomain) {
			((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
					.setAggregatePushDownable(false);
		}

		if (tupleDomain instanceof ProteumTupleDomain
				&& ((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
						.isAggregatePushDownable()
				&& ((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
						.getMinimumExpression() != null) {
			boolean isOkExpression = ExpressionCheckerVisitor
					.isOkExpression(((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
							.getMinimumExpression());
			if (!isOkExpression) {
				((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
						.setAggregatePushDownable(false);
			}
		}
		String str = null;
		if (tupleDomain instanceof ProteumTupleDomain) {
			try {
				Expression expression = ((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
						.getRemainingExpresstion();
				Expression expression2 = fromPredicate(expression);
				str = ExpressionFormatter.formatExpression(expression2);
			} catch (Exception e) {
				System.out.println("While formatting filters : "
						+ e.getMessage());
			}
		}
		List<ProteumColumnFilter> columnFilters = new ArrayList<ProteumColumnFilter>();
		for (Entry<ConnectorColumnHandle, Domain> entry : tupleDomain
				.getDomains().entrySet()) {
			columnFilters.add(new ProteumColumnFilter(
					(ProteumColumnHandle) entry.getKey(), entry.getValue(),
					null));
		}
		if (str != null
				&& !str.toLowerCase().equals("true")
				&& !str.equalsIgnoreCase(ExpressionFormatter
						.formatExpression(BooleanLiteral.TRUE_LITERAL))) {
			columnFilters.add(new ProteumColumnFilter(null, null, str));
		}
		ProteumPredicatePushDown proteumPredicatePushDown = new ProteumPredicatePushDown(
				columnFilters);
		if (tupleDomain instanceof ProteumTupleDomain
				&& ((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
						.isAggregatePushDownable()
				&& ((((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
						.getPushDownAggregationList() != null && !((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
						.getPushDownAggregationList().isEmpty())
				|| (((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
						.getGroupBy() != null && !((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
						.getGroupBy().isEmpty()))) {
			proteumPredicatePushDown
					.setAggregates(((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
							.getPushDownAggregationList());
			proteumPredicatePushDown
					.setGroupBy(((ProteumTupleDomain<ConnectorColumnHandle>) tupleDomain)
							.getGroupBy());
		}
		List<ConnectorPartition> partitions = ImmutableList
				.<ConnectorPartition> of(new ProteumPartition(
						proteumTableHandle.getSchemaName(), proteumTableHandle
								.getTableName(), proteumPredicatePushDown));
		return new ConnectorPartitionResult(partitions, tupleDomain);
	}

	@Override
	public ConnectorSplitSource getPartitionSplits(
			ConnectorTableHandle tableHandle,
			List<ConnectorPartition> partitions) {
		ConnectorPartition partition = partitions.get(0);
		ProteumPartition proteumPartition = (ProteumPartition) partition;
		ProteumTableHandle proteumTableHandle = (ProteumTableHandle) tableHandle;
		ProteumTable table = client.getTable(
				proteumTableHandle.getSchemaName(),
				proteumTableHandle.getTableName());
		List<ConnectorSplit> splits = Lists.newArrayList();
		for (URL uri : table.getSources()) {
			splits.add(new ProteumSplit(connectorId, proteumPartition
					.getSchemaName(), proteumPartition.getTableName(), uri,
					proteumPartition.getProteumPredicatePushDown()));
		}
		Collections.shuffle(splits);
		return new FixedSplitSource(connectorId, splits);
	}
}
