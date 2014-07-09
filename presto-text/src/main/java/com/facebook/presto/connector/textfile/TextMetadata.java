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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.type.BigintType;

public class TextMetadata extends ReadOnlyConnectorMetadata{

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        // TODO Auto-generated method stub
        List<String> metadata = new ArrayList<String>();
        metadata.add("default");
        return metadata;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session,
            SchemaTableName tableName) {
        // TODO Auto-generated method stub
        return new TextTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table) {
        TextTableHandle textTable = (TextTableHandle)table;
        // TODO Auto-generated method stub
        ColumnMetadata column1 = new ColumnMetadata("a", BigintType.getInstance(), 0, false);
        ColumnMetadata column2 = new ColumnMetadata("b", BigintType.getInstance(), 1, false);
        List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
        columns.add(column1);columns.add(column2);
        SchemaTableName tableName = new SchemaTableName(textTable.getSchemaName(), textTable.getTableName());
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session,
            String schemaNameOrNull) {
        // TODO Auto-generated method stub
        if(schemaNameOrNull == null || !schemaNameOrNull.equals("default"))
            return null;
        SchemaTableName schemaTableName  = new SchemaTableName(schemaNameOrNull, "test");
        List<SchemaTableName> table = new ArrayList<SchemaTableName>();
        table.add(schemaTableName);
        return table;
    }

    @Override
    public ConnectorColumnHandle getColumnHandle(
            ConnectorTableHandle tableHandle, String columnName) {
        // TODO Auto-generated method stub
        return getColumnHandles(tableHandle).get(columnName);
    }

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(
            ConnectorTableHandle tableHandle) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorTableHandle tableHandle) {
        Map<String, ConnectorColumnHandle> result = new HashMap<String, ConnectorColumnHandle>();
        ConnectorTableMetadata meta = getTableMetadata(tableHandle);
        for(ColumnMetadata columnMetaData : meta.getColumns()){
            result.put(columnMetaData.getName(), new TextColumnHandle("connector", columnMetaData));
        }
        return result;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle,
            ConnectorColumnHandle columnHandle) {
        // TODO Auto-generated method stub
        return ((TextColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session, SchemaTablePrefix prefix) {
        Map<SchemaTableName, List<ColumnMetadata>> result = new HashMap<SchemaTableName, List<ColumnMetadata>>();
        SchemaTableName tableName = new SchemaTableName("default", "test");
        ConnectorTableMetadata metaData = getTableMetadata(getTableHandle(session, tableName));
        result.put(tableName, metaData.getColumns());
        return result;
    }

}
