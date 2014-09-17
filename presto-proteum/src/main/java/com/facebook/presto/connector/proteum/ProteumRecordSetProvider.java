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


import java.util.List;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

public class ProteumRecordSetProvider implements ConnectorRecordSetProvider
{
    private final String connectorId;

    public ProteumRecordSetProvider(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ConnectorColumnHandle> columns)
    {

        ProteumSplit proteumSplit = (ProteumSplit) split;

        ImmutableList.Builder<ProteumColumnHandle> handles = ImmutableList.builder();
        for (ConnectorColumnHandle handle : columns) {
            handles.add((ProteumColumnHandle) handle);
        }

        return new ProteumRecordSet(proteumSplit, handles.build());
    }
}
