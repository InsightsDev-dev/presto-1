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

import io.airlift.configuration.Config;

import java.net.URI;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

public class MetadataConfig {
	private URI tableJsonUri;
	private String rootDirPath;
	private List<String> resourceConfigFiles;
	private static final Splitter SPLITTER = Splitter.on(',').trimResults()
			.omitEmptyStrings();

	public List<String> getResourceConfigFiles() {
		return resourceConfigFiles;
	}

	@Config("hadoop.config.resources")
	public MetadataConfig setResourceConfigFiles(String files) {
		this.resourceConfigFiles = (files == null) ? null : SPLITTER
				.splitToList(files);
		return this;
	}

	public MetadataConfig setResourceConfigFiles(List<String> files) {
		this.resourceConfigFiles = (files == null) ? null : ImmutableList
				.copyOf(files);
		return this;
	}

	public String getRootDirPath() {
		return rootDirPath;
	}

	@Config("root-dir-path")
	public MetadataConfig setRootDirPath(String rootDirPath) {
		this.rootDirPath = rootDirPath;
		return this;
	}

	public URI getTableJsonUri() {
		return tableJsonUri;
	}

	@Config("table-json-uri")
	public MetadataConfig setTableJsonUri(URI tableJsonUri) {
		this.tableJsonUri = tableJsonUri;
		return this;
	}

}
