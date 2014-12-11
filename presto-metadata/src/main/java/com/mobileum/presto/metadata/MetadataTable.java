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

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

public class MetadataTable {
	private final String name;
	private final List<MetadataColumn> columns;
	private final List<ColumnMetadata> columnsMetadata;

	@JsonCreator
	public MetadataTable(@JsonProperty("name") String name,
			@JsonProperty("columns") List<MetadataColumn> columns) {
		checkArgument(!isNullOrEmpty(name), "name is null or is empty");
		this.name = checkNotNull(name, "name is null");
		this.columns = ImmutableList.copyOf(checkNotNull(columns,
				"columns is null"));

		int index = 0;
		ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList
				.builder();
		for (MetadataColumn column : this.columns) {
			columnsMetadata.add(new ColumnMetadata(column.getName(), column
					.getType(), index, false));
			index++;
		}
		this.columnsMetadata = columnsMetadata.build();
	}

	public MetadataTable(String string, ImmutableList<MetadataColumn> of,
			ImmutableList<URI> of2) {
		this(string, of);
	}

	@JsonProperty
	public String getName() {
		return name;
	}

	@JsonProperty
	public List<MetadataColumn> getColumns() {
		return columns;
	}

	public List<ColumnMetadata> getColumnsMetadata() {
		return columnsMetadata;
	}

	public static Function<MetadataTable, String> nameGetter() {
		return new Function<MetadataTable, String>() {
			@Override
			public String apply(MetadataTable table) {
				return table.getName();
			}
		};
	}
}
