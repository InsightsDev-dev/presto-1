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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableMap;

public class TextConnectorFactory implements ConnectorFactory{

    private final TypeManager typeManager;
    private final Map<String, String> optionalConfig;

    public TextConnectorFactory(TypeManager typeManager, Map<String, String> optionalConfig)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    @Override
    public String getName()
    {
        return "text";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config) {
        TextMetadata metadata = new TextMetadata();
        TextSplitManager splitManager = new TextSplitManager(connectorId);
        TextRecordSetProvider recordSetProvider = new TextRecordSetProvider();
        TextHandleResolver handleResolver = new TextHandleResolver();
        Connector connector = new TextConnector(metadata, splitManager, recordSetProvider, handleResolver);
        return connector;
    }

}
