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
package com.facebook.presto.connector.textfile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class TextSplitManager implements ConnectorSplitManager{
    private String connectorID;
    
    public TextSplitManager(String connectorID){
        this.connectorID = connectorID;
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle table,
            TupleDomain<ConnectorColumnHandle> tupleDomain) {
        TextTableHandle exampleTableHandle = (TextTableHandle) table;

        // example connector has only one partition
        List<ConnectorPartition> partitions = ImmutableList.<ConnectorPartition>of(new TextPartition(exampleTableHandle.getSchemaName(), exampleTableHandle.getTableName()));
        // example connector does not do any additional processing/filtering with the TupleDomain, so just return the whole TupleDomain
        return new ConnectorPartitionResult(partitions, tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle table,
            List<ConnectorPartition> partitions) {
        // TODO Auto-generated method stub
       // TextPartition partition = (TextPartition)partitions.get(0);
        List<ConnectorSplit> splits = Lists.newArrayList();
        splits.add(new TextSplit("/Users/ajaygarg/prestotmp.txt"));

        return new FixedSplitSource(connectorID, splits);
    }

}
