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
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class ProteumSplitManager
implements ConnectorSplitManager
{
    private final String connectorId;
    private final ProteumClient client;

    public ProteumSplitManager(String connectorId, ProteumClient client)
    {
        this.connectorId = connectorId;
        this.client = client;
    }

   
	public static Expression fromPredicate(Expression expression) {
		return new com.mobileum.range.presto.RewritingVisitor<Void>().process(expression,
				new com.mobileum.range.presto.Context<Void>(null, false));
	}
	@Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
    	if (tupleDomain instanceof ProteumTupleDomain) {
    	    @SuppressWarnings("rawtypes")
			Expression expression = ((ProteumTupleDomain) tupleDomain)
					.getRemainingExpresstion();
    	    Expression expression2= fromPredicate(expression);
			//System.out.println(expression2);
		}
        ProteumTableHandle proteumTableHandle = (ProteumTableHandle) tableHandle;
        List<ProteumColumnFilter> columnFilters = new ArrayList<ProteumColumnFilter>();
        for(Entry<ConnectorColumnHandle, Domain> entry : tupleDomain.getDomains().entrySet()){
            columnFilters.add(new ProteumColumnFilter((ProteumColumnHandle) entry.getKey(), entry.getValue()));
        }
        List<ConnectorPartition> partitions = ImmutableList.<ConnectorPartition>of(new ProteumPartition(proteumTableHandle.getSchemaName(), 
                proteumTableHandle.getTableName(), columnFilters));
        return new ConnectorPartitionResult(partitions, tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        ConnectorPartition partition = partitions.get(0);
        ProteumPartition proteumPartition = (ProteumPartition) partition;
        ProteumTableHandle proteumTableHandle = (ProteumTableHandle) tableHandle;
        ProteumTable table = client.getTable(proteumTableHandle.getSchemaName(), proteumTableHandle.getTableName());
        List<ConnectorSplit> splits = Lists.newArrayList();
        for (URL uri : table.getSources()) {
            splits.add(new ProteumSplit(connectorId, proteumPartition.getSchemaName(), proteumPartition.getTableName(), uri, proteumPartition.getColumnFilters()));
        }
        Collections.shuffle(splits);
        return new FixedSplitSource(connectorId, splits);
    }
}

