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

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;

public class TextHandleResolver implements ConnectorHandleResolver{

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle) {
        // TODO Auto-generated method stub
        return tableHandle instanceof TextTableHandle;
    }

    @Override
    public boolean canHandle(ConnectorColumnHandle columnHandle) {
        // TODO Auto-generated method stub
        return columnHandle instanceof TextColumnHandle;
    }

    @Override
    public boolean canHandle(ConnectorSplit split) {
        // TODO Auto-generated method stub
        return split instanceof TextSplit;
    }

    @Override
    public boolean canHandle(ConnectorIndexHandle indexHandle) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        // TODO Auto-generated method stub
        return TextTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorColumnHandle> getColumnHandleClass() {
        // TODO Auto-generated method stub
        return TextColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorIndexHandle> getIndexHandleClass() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        // TODO Auto-generated method stub
        return TextSplit.class;
    }

    @Override
    public boolean canHandle(ConnectorOutputTableHandle tableHandle) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean canHandle(ConnectorInsertTableHandle tableHandle) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass() {
        throw new UnsupportedOperationException();
    }

}
