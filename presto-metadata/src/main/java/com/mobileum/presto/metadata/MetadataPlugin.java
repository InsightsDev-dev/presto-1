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

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is the entry point for the plugin and an implementation of Plugin
 * interface.This class name is provided to Presto via the standard Java
 * <code>ServiceLoader</code> interface: the classpath contains a resource file
 * named <code>com.facebook.presto.spi.Plugin</code> in the META-INF/services
 * directory. The content of this file is a single line listing the name of the
 * plugin class:<code>MetadataPlugin</code>,
 * 
 * @author dilipsingh
 * @see java.util.ServiceLoader
 */
public class MetadataPlugin implements Plugin {
	private TypeManager typeManager;
	private Map<String, String> optionalConfig = ImmutableMap.of();

	@Override
	public synchronized void setOptionalConfig(
			Map<String, String> optionalConfig) {
		this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig,
				"optionalConfig is null"));
	}

	@Inject
	public synchronized void setTypeManager(TypeManager typeManager) {
		this.typeManager = typeManager;
	}

	public synchronized Map<String, String> getOptionalConfig() {
		return optionalConfig;
	}

	/**
	 * The <code>getServices()</code> method is a top-level function that Presto
	 * calls to retrieve a ConnectorFactory when Presto is ready to create an
	 * instance of a connector to back a catalog.
	 */
	@Override
	public synchronized <T> List<T> getServices(Class<T> type) {
		if (type == ConnectorFactory.class) {
			return ImmutableList.of(type.cast(new MetadataConnectorFactory(
					typeManager, getOptionalConfig())));
		} else if (type == FunctionFactory.class) {
			return ImmutableList.of(type.cast(new MetadataFunctionFactory(this.typeManager)));
		}
		return ImmutableList.of();
	}
}
