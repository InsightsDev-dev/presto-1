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

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class ProteumMetadata extends ReadOnlyConnectorMetadata {
	private String connectorID;
	private ProteumClient client;

	@Inject
	public ProteumMetadata(@Named("connectorId") String connectorID,
			ProteumClient client) {
		this.connectorID = connectorID;
		this.client = client;
	}

	@Override
	public List<String> listSchemaNames(ConnectorSession session) {
		return Lists.newArrayList(client.getSchemas());
	}

	public List<String> listSchemaNames() {
		return ImmutableList.copyOf(client.getSchemas());
	}

	@Override
	public ConnectorTableHandle getTableHandle(ConnectorSession session,
			SchemaTableName tableName) {
		return new ProteumTableHandle(connectorID, tableName.getSchemaName(),
				tableName.getTableName());
	}

	@Override
	public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table) {
		ProteumTableHandle handle = (ProteumTableHandle) table;
		ProteumTable proteumTable = client.getTable(handle.getSchemaName(),
				handle.getTableName());
		if (proteumTable == null) {
			return null;
		}
		SchemaTableName schemaTableName = new SchemaTableName(
				handle.getSchemaName(), handle.getTableName());
		return new ConnectorTableMetadata(schemaTableName,
				proteumTable.getColumnsMetadata());
	}

	@Override
	public List<SchemaTableName> listTables(ConnectorSession session,
			String schemaNameOrNull) {
		if (schemaNameOrNull == null)
			schemaNameOrNull = "default";
		Set<String> tableNames = client.getTableNames(schemaNameOrNull);
		if (tableNames == null)
			return null;
		List<SchemaTableName> result = new ArrayList<SchemaTableName>();
		for (String table : tableNames) {
			result.add(new SchemaTableName(schemaNameOrNull, table));
		}
		return result;
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
		ProteumTableHandle pTableHandle = (ProteumTableHandle) tableHandle;
		ProteumTable table = client.getTable(pTableHandle.getSchemaName(),
				pTableHandle.getTableName());
		if (table == null)
			return null;
		List<ColumnMetadata> columnsMetaData = table.getColumnsMetadata();
		Map<String, ConnectorColumnHandle> result = new HashMap<String, ConnectorColumnHandle>();
		for (ColumnMetadata meta : columnsMetaData) {
			result.put(meta.getName(), new ProteumColumnHandle(connectorID,
					meta));
		}
		return result;
	}

	@Override
	public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle,
			ConnectorColumnHandle columnHandle) {
		// TODO Auto-generated method stub
		return ((ProteumColumnHandle) columnHandle).getColumnMetadata();
	}

	@Override
	public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
			ConnectorSession session, SchemaTablePrefix prefix) {
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

		ProteumTable table = client.getTable(tableName.getSchemaName(),
				tableName.getTableName());
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

	public void addTable(String schema) {
		try {
			client.addTable(schema);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("Exception in creating the view");
		}
	}

	public String getBaseURL() {
		return client.getBaseURL();
	}
}
