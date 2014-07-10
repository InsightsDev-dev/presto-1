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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableMap;

public class ProteumConnectorFactory implements ConnectorFactory
{
    private final TypeManager typeManager;
    private final Map<String, String> optionalConfig;
    
    public ProteumConnectorFactory(TypeManager typeManager, Map<String, String> optionalConfig)
    {
        this.typeManager = typeManager;
        this.optionalConfig = optionalConfig;
    }

    @Override
    public String getName()
    {
        return "proteum";
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> requiredConfig)
    {
        ProteumClient client = new ProteumClient(requiredConfig.get("proteum.host"), 
                requiredConfig.get("proteum.port")); 
        ProteumMetadata metadata = new ProteumMetadata(connectorId, client);
        ProteumSplitManager splitManager = new ProteumSplitManager(connectorId, client);
        ProteumRecordSetProvider recordSetProvider = new ProteumRecordSetProvider(connectorId);
        ProteumHandleResolver handleResolver = new ProteumHandleResolver(connectorId);
        ProteumConnector connector = new ProteumConnector(metadata, splitManager, recordSetProvider, handleResolver);
        return connector;
    }
}