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

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;

import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.mobileum.presto.metadata.MetadataTable.nameGetter;

public class MetadataClient {
	/**
	 * SchemaName -> (TableName -> TableMetadata)
	 */
	private final Supplier<Map<String, Map<String, MetadataTable>>> schemas;
	private final static String json = "{\"default\":[{\"name\":\"partition_info\",\"columns\":[{\"name\":\"modelname\",\"type\":\"VARCHAR\"},{\"name\":\"partitionid\",\"type\":\"VARCHAR\"},{\"name\":\"no_of_record\",\"type\":\"VARCHAR\"},{\"name\":\"partition_info\",\"type\":\"VARCHAR\"},{\"name\":\"generic_string\",\"type\":\"VARCHAR\"}]},{\"name\":\"card\",\"columns\":[{\"name\":\"modelname\",\"type\":\"VARCHAR\"}]},{\"name\":\"partition_metadata\",\"columns\":[{\"name\":\"modelname\",\"type\":\"VARCHAR\"},{\"name\":\"partitionid\",\"type\":\"VARCHAR\"},{\"name\":\"columnname\",\"type\":\"VARCHAR\"},{\"name\":\"min\",\"type\":\"VARCHAR\"},{\"name\":\"max\",\"type\":\"VARCHAR\"},{\"name\":\"cardinality\",\"type\":\"HyperLogLog\"}]}]}";

	@Inject
	public MetadataClient(MetadataConfig config,
			JsonCodec<Map<String, List<MetadataTable>>> catalogCodec)
			throws IOException {
		checkNotNull(config, "config is null");
		checkNotNull(catalogCodec, "catalogCodec is null");
		schemas = Suppliers.memoize(schemasSupplier(catalogCodec,
				config.getTableJsonUri()));
	}

	public Set<String> getSchemaNames() {
		return schemas.get().keySet();
	}

	public Set<String> getTableNames(String schema) {
		checkNotNull(schema, "schema is null");
		Map<String, MetadataTable> tables = schemas.get().get(schema);
		if (tables == null) {
			return ImmutableSet.of();
		}
		return tables.keySet();
	}

	public MetadataTable getTable(String schema, String tableName) {
		checkNotNull(schema, "schema is null");
		checkNotNull(tableName, "tableName is null");
		Map<String, MetadataTable> tables = schemas.get().get(schema);
		if (tables == null) {
			return null;
		}
		return tables.get(tableName);
	}

	private static Supplier<Map<String, Map<String, MetadataTable>>> schemasSupplier(
			final JsonCodec<Map<String, List<MetadataTable>>> catalogCodec,
			final URI metadataUri) {
		return new Supplier<Map<String, Map<String, MetadataTable>>>() {
			@Override
			public Map<String, Map<String, MetadataTable>> get() {
				try {
					return lookupSchemas(metadataUri, catalogCodec);
				} catch (IOException e) {
					throw Throwables.propagate(e);
				}
			}
		};
	}

	private static Map<String, Map<String, MetadataTable>> lookupSchemas(
			URI metadataUri,
			JsonCodec<Map<String, List<MetadataTable>>> catalogCodec)
			throws IOException {
		Map<String, List<MetadataTable>> catalog;
		if (metadataUri != null) {
			URL result = metadataUri.toURL();
			String json = Resources.toString(result, Charsets.UTF_8);
			catalog = catalogCodec.fromJson(json);
		} else {
			catalog = catalogCodec.fromJson(json);
		}
		return ImmutableMap.copyOf(transformValues(catalog,
				resolveAndIndexTables(metadataUri)));
	}

	private static Function<List<MetadataTable>, Map<String, MetadataTable>> resolveAndIndexTables(
			final URI metadataUri) {
		return new Function<List<MetadataTable>, Map<String, MetadataTable>>() {
			@Override
			public Map<String, MetadataTable> apply(List<MetadataTable> tables) {
				Iterable<MetadataTable> resolvedTables = transform(tables,
						tableUriResolver(metadataUri));
				return ImmutableMap.copyOf(uniqueIndex(resolvedTables,
						nameGetter()));
			}
		};
	}

	private static Function<MetadataTable, MetadataTable> tableUriResolver(
			final URI baseUri) {
		return new Function<MetadataTable, MetadataTable>() {
			@Override
			public MetadataTable apply(MetadataTable table) {
				return new MetadataTable(table.getName(), table.getColumns());
			}
		};
	}
}
