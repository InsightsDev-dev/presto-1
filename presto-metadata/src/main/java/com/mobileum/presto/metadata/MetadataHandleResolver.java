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
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class MetadataHandleResolver implements ConnectorHandleResolver {
	private final String connectorId;

	@Inject
	public MetadataHandleResolver(MetadataConnectorId clientId) {
		this.connectorId = checkNotNull(clientId, "clientId is null")
				.toString();
	}

	@Override
	public boolean canHandle(ConnectorTableHandle tableHandle) {
		return tableHandle instanceof MetadataTableHandle
				&& ((MetadataTableHandle) tableHandle).getConnectorId().equals(
						connectorId);
	}

	@Override
	public boolean canHandle(ConnectorColumnHandle columnHandle) {
		return columnHandle instanceof MetadataColumnHandle
				&& ((MetadataColumnHandle) columnHandle).getConnectorId()
						.equals(connectorId);
	}

	@Override
	public boolean canHandle(ConnectorSplit split) {
		return split instanceof MetadataSplit
				&& ((MetadataSplit) split).getConnectorId().equals(connectorId);
	}

	@Override
	public boolean canHandle(ConnectorIndexHandle indexHandle) {
		return false;
	}

	@Override
	public Class<? extends ConnectorTableHandle> getTableHandleClass() {
		return MetadataTableHandle.class;
	}

	@Override
	public Class<? extends ConnectorColumnHandle> getColumnHandleClass() {
		return MetadataColumnHandle.class;
	}

	@Override
	public Class<? extends ConnectorSplit> getSplitClass() {
		return MetadataSplit.class;
	}

	@Override
	public Class<? extends ConnectorIndexHandle> getIndexHandleClass() {
		throw new UnsupportedOperationException();
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass() {
		// TODO Auto-generated method stub
		return null;
	}
}
