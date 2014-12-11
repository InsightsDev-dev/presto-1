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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class MetadataSplit implements ConnectorSplit {
	private final String connectorId;
	private final String schemaName;
	private final String tableName;
	private final String path;
	private final boolean remotelyAccessible;
	private final ImmutableList<HostAddress> addresses;

	@JsonCreator
	public MetadataSplit(@JsonProperty("connectorId") String connectorId,
			@JsonProperty("schemaName") String schemaName,
			@JsonProperty("tableName") String tableName,
			@JsonProperty("path") String path) {
		this.schemaName = checkNotNull(schemaName, "schema name is null");
		this.connectorId = checkNotNull(connectorId, "connector id is null");
		this.tableName = checkNotNull(tableName, "table name is null");
		this.path = checkNotNull(path, "path is null");

		// if ("http".equalsIgnoreCase(uri.getScheme()) ||
		// "https".equalsIgnoreCase(uri.getScheme())) {
		remotelyAccessible = true;
		addresses = ImmutableList.of(HostAddress.fromString("localhost:8080"));
	}

	public MetadataSplit(String connectorId2, String schemaName2,
			String tableName2, URI dataUri) {
		this(connectorId2, schemaName2, tableName2, dataUri.getPath());
	}

	@JsonProperty
	public String getConnectorId() {
		return connectorId;
	}

	@JsonProperty
	public String getSchemaName() {
		return schemaName;
	}

	@JsonProperty
	public String getTableName() {
		return tableName;
	}

	@JsonProperty
	public String getPath() {
		return path;
	}

	@Override
	public boolean isRemotelyAccessible() {
		// only http or https is remotely accessible
		return remotelyAccessible;
	}

	@Override
	public List<HostAddress> getAddresses() {
		return addresses;
	}

	@Override
	public Object getInfo() {
		return this;
	}
}
