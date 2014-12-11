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
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MetadataRecordSetProvider implements ConnectorRecordSetProvider {
	private final String connectorId;
	private MetadataConfig config;

	@Inject
	public MetadataRecordSetProvider(MetadataConnectorId connectorId,
			MetadataConfig config) {
		this.connectorId = checkNotNull(connectorId, "connectorId is null")
				.toString();
		checkNotNull(config, "metadata config is null");
		this.config = config;
	}

	@Override
	public RecordSet getRecordSet(ConnectorSplit split,
			List<? extends ConnectorColumnHandle> columns) {
		checkNotNull(split, "partitionChunk is null");
		checkArgument(split instanceof MetadataSplit);

		MetadataSplit exampleSplit = (MetadataSplit) split;
		checkArgument(exampleSplit.getConnectorId().equals(connectorId),
				"split is not for this connector");

		ImmutableList.Builder<MetadataColumnHandle> handles = ImmutableList
				.builder();
		for (ConnectorColumnHandle handle : columns) {
			checkArgument(handle instanceof MetadataColumnHandle);
			handles.add((MetadataColumnHandle) handle);
		}

		return new MetadataRecordSet(exampleSplit, this.config, handles.build());
	}
}
