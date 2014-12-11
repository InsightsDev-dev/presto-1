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
package com.mobileum.presto.metadata;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MetadataMetadata extends ReadOnlyConnectorMetadata {
	private final String connectorId;

	private final MetadataClient metedataClient;

	@Inject
	public MetadataMetadata(MetadataConnectorId connectorId,
			MetadataClient exampleClient) {
		this.connectorId = checkNotNull(connectorId, "connectorId is null")
				.toString();
		this.metedataClient = checkNotNull(exampleClient, "client is null");
	}

	@Override
	public List<String> listSchemaNames(ConnectorSession session) {
		return listSchemaNames();
	}

	public List<String> listSchemaNames() {
		return ImmutableList.copyOf(metedataClient.getSchemaNames());
	}

	@Override
	public MetadataTableHandle getTableHandle(ConnectorSession session,
			SchemaTableName tableName) {
		if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
			return null;
		}

		MetadataTable table = metedataClient.getTable(
				tableName.getSchemaName(), tableName.getTableName());
		if (table == null) {
			return null;
		}

		return new MetadataTableHandle(connectorId, tableName.getSchemaName(),
				tableName.getTableName());
	}

	@Override
	public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table) {
		checkArgument(table instanceof MetadataTableHandle,
				"tableHandle is not an instance of MetadataTableHandle");
		MetadataTableHandle exampleTableHandle = (MetadataTableHandle) table;
		checkArgument(exampleTableHandle.getConnectorId().equals(connectorId),
				"tableHandle is not for this connector");
		SchemaTableName tableName = new SchemaTableName(
				exampleTableHandle.getSchemaName(),
				exampleTableHandle.getTableName());

		return getTableMetadata(tableName);
	}

	@Override
	public List<SchemaTableName> listTables(ConnectorSession session,
			String schemaNameOrNull) {
		Set<String> schemaNames;
		if (schemaNameOrNull != null) {
			schemaNames = ImmutableSet.of(schemaNameOrNull);
		} else {
			schemaNames = metedataClient.getSchemaNames();
		}

		ImmutableList.Builder<SchemaTableName> builder = ImmutableList
				.builder();
		for (String schemaName : schemaNames) {
			for (String tableName : metedataClient.getTableNames(schemaName)) {
				builder.add(new SchemaTableName(schemaName, tableName));
			}
		}
		return builder.build();
	}

	@Override
	public ConnectorColumnHandle getSampleWeightColumnHandle(
			ConnectorTableHandle tableHandle) {
		return null;
	}

	@Override
	public Map<String, ConnectorColumnHandle> getColumnHandles(
			ConnectorTableHandle tableHandle) {
		checkNotNull(tableHandle, "tableHandle is null");
		checkArgument(tableHandle instanceof MetadataTableHandle,
				"tableHandle is not an instance of ExampleTableHandle");
		MetadataTableHandle exampleTableHandle = (MetadataTableHandle) tableHandle;
		checkArgument(exampleTableHandle.getConnectorId().equals(connectorId),
				"tableHandle is not for this connector");

		MetadataTable table = metedataClient.getTable(
				exampleTableHandle.getSchemaName(),
				exampleTableHandle.getTableName());
		if (table == null) {
			throw new TableNotFoundException(
					exampleTableHandle.toSchemaTableName());
		}

		ImmutableMap.Builder<String, ConnectorColumnHandle> columnHandles = ImmutableMap
				.builder();
		for (ColumnMetadata columnMetadata : table.getColumnsMetadata()) {
			columnHandles.put(columnMetadata.getName(),
					new MetadataColumnHandle(connectorId, columnMetadata));
		}
		return columnHandles.build();
	}

	@Override
	public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
			ConnectorSession session, SchemaTablePrefix prefix) {
		checkNotNull(prefix, "prefix is null");
		ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap
				.builder();
		for (SchemaTableName tableName : listTables(session, prefix)) {
			ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
			// table can disappear during listing operation
			if (tableMetadata != null) {
				columns.put(tableName, tableMetadata.getColumns());
			}
		}
		return columns.build();
	}

	private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName) {
		if (!listSchemaNames().contains(tableName.getSchemaName())) {
			return null;
		}

		MetadataTable table = metedataClient.getTable(
				tableName.getSchemaName(), tableName.getTableName());
		if (table == null) {
			return null;
		}

		return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
	}

	private List<SchemaTableName> listTables(ConnectorSession session,
			SchemaTablePrefix prefix) {
		if (prefix.getSchemaName() == null) {
			return listTables(session, prefix.getSchemaName());
		}
		return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(),
				prefix.getTableName()));
	}

	@Override
	public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle,
			ConnectorColumnHandle columnHandle) {
		checkNotNull(tableHandle, "tableHandle is null");
		checkNotNull(columnHandle, "columnHandle is null");
		checkArgument(tableHandle instanceof MetadataTableHandle,
				"tableHandle is not an instance of ExampleTableHandle");
		checkArgument(((MetadataTableHandle) tableHandle).getConnectorId()
				.equals(connectorId), "tableHandle is not for this connector");
		checkArgument(columnHandle instanceof MetadataColumnHandle,
				"columnHandle is not an instance of ExampleColumnHandle");

		return ((MetadataColumnHandle) columnHandle).getColumnMetadata();
	}
}
