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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class InMemoryMetadata
        implements ConnectorMetadata
{
    private final ConcurrentMap<SchemaTableName, ConnectorTableMetadata> tables = new ConcurrentHashMap<>();

    @Override
    public List<String> listSchemaNames(Session session)
    {
        Set<String> schemaNames = new HashSet<>();

        for (SchemaTableName schemaTableName : tables.keySet()) {
            schemaNames.add(schemaTableName.getSchemaName());
        }

        return ImmutableList.copyOf(schemaNames);
    }

    @Override
    public ConnectorTableHandle getTableHandle(Session session, SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        if (!tables.containsKey(tableName)) {
            return null;
        }
        return new InMemoryTableHandle(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        ConnectorTableMetadata tableMetadata = tables.get(tableName);
        checkArgument(tableMetadata != null, "Table %s does not exist", tableName);
        return tableMetadata;
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ConnectorColumnHandle> builder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : getTableMetadata(tableHandle).getColumns()) {
            builder.put(columnMetadata.getName(), new InMemoryColumnHandle(columnMetadata.getName(), columnMetadata.getOrdinalPosition(), columnMetadata.getType()));
        }
        return builder.build();
    }

    @Override
    public ConnectorColumnHandle getColumnHandle(ConnectorTableHandle tableHandle, String columnName)
    {
        for (ColumnMetadata columnMetadata : getTableMetadata(tableHandle).getColumns()) {
            if (columnMetadata.getName().equals(columnName)) {
                return new InMemoryColumnHandle(columnMetadata.getName(), columnMetadata.getOrdinalPosition(), columnMetadata.getType());
            }
        }
        return null;
    }

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public boolean canCreateSampledTables(Session session)
    {
        return false;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(Session session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix.getSchemaName())) {
            int position = 1;
            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
            for (ColumnMetadata column : tables.get(tableName).getColumns()) {
                columns.add(new ColumnMetadata(column.getName(), column.getType(), position, false));
                position++;
            }
            tableColumns.put(tableName, columns.build());
        }
        return tableColumns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle)
    {
        SchemaTableName tableName = getTableName(tableHandle);
        checkArgument(columnHandle instanceof InMemoryColumnHandle, "columnHandle is not an instance of InMemoryColumnHandle");
        InMemoryColumnHandle inMemoryColumnHandle = (InMemoryColumnHandle) columnHandle;
        int columnIndex = inMemoryColumnHandle.getOrdinalPosition();
        return tables.get(tableName).getColumns().get(columnIndex);
    }

    @Override
    public List<SchemaTableName> listTables(Session session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : tables.keySet()) {
            if (schemaNameOrNull == null || schemaNameOrNull.equals(tableName.getSchemaName())) {
                builder.add(tableName);
            }
        }
        return builder.build();
    }

    @Override
    public ConnectorTableHandle createTable(Session session, ConnectorTableMetadata tableMetadata)
    {
        ConnectorTableMetadata existingTable = tables.putIfAbsent(tableMetadata.getTable(), tableMetadata);
        checkArgument(existingTable == null, "Table %s already exists", tableMetadata.getTable());
        return new InMemoryTableHandle(tableMetadata.getTable());
    }

    @Override
    public void dropTable(ConnectorTableHandle tableHandle)
    {
        tables.remove(getTableName(tableHandle));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(Session session, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitCreateTable(ConnectorOutputTableHandle tableHandle, Collection<String> fragments)
    {
        throw new UnsupportedOperationException();
    }

    private SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof InMemoryTableHandle, "tableHandle is not an instance of InMemoryTableHandle");
        InMemoryTableHandle inMemoryTableHandle = (InMemoryTableHandle) tableHandle;
        return inMemoryTableHandle.getTableName();
    }

    public static class InMemoryTableHandle
            implements ConnectorTableHandle
    {
        private final SchemaTableName tableName;

        public InMemoryTableHandle(SchemaTableName schemaTableName)
        {
            this.tableName = schemaTableName;
        }

        public SchemaTableName getTableName()
        {
            return tableName;
        }
    }

    public static class InMemoryColumnHandle
            implements ConnectorColumnHandle
    {
        private final String name;
        private final int ordinalPosition;
        private final Type type;

        public InMemoryColumnHandle(String name, int ordinalPosition, Type type)
        {
            this.name = name;
            this.ordinalPosition = ordinalPosition;
            this.type = type;
        }

        public String getName()
        {
            return name;
        }

        public int getOrdinalPosition()
        {
            return ordinalPosition;
        }

        public Type getType()
        {
            return type;
        }
    }
}
