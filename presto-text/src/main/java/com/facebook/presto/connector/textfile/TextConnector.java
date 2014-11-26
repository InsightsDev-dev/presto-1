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

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;

public class TextConnector implements Connector{
    private TextMetadata metadata;
    private TextSplitManager splitManager;
    private TextRecordSetProvider recordSetProvider;
    private TextHandleResolver handleResolver;
    public TextConnector(TextMetadata metadata, TextSplitManager splitManager,
            TextRecordSetProvider recordSetProvider,
            TextHandleResolver handleResolver) {
        super();
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.recordSetProvider = recordSetProvider;
        this.handleResolver = handleResolver;
    }
    @Override
    public ConnectorHandleResolver getHandleResolver() {
        // TODO Auto-generated method stub
        return handleResolver;
    }
    
    @Override
    public ConnectorMetadata getMetadata() {
        // TODO Auto-generated method stub
        return metadata;
    }
    @Override
    public ConnectorSplitManager getSplitManager() {
        // TODO Auto-generated method stub
        return splitManager;
    }
    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        // TODO Auto-generated method stub
        return recordSetProvider;
    }
    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }
    @Override
    public ConnectorIndexResolver getIndexResolver() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }
    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        throw new UnsupportedOperationException();
    }
    
    
}
