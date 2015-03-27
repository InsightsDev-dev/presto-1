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

import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Injector;

public class ProteumConnectorFactory implements ConnectorFactory {
	private final TypeManager typeManager;
	private final Map<String, String> optionalConfig;

	public ProteumConnectorFactory(TypeManager typeManager,
			Map<String, String> optionalConfig) {
		this.typeManager = typeManager;
		this.optionalConfig = optionalConfig;
	}

	@Override
	public String getName() {
		return "proteum";
	}

	@Override
	public Connector create(final String connectorId,
			Map<String, String> requiredConfig) {
		Bootstrap app = new Bootstrap(new ProteumModule(connectorId,
				typeManager));
		Injector injector = null;
		try {
			injector = app.strictConfig().doNotInitializeLogging()
					.setRequiredConfigurationProperties(requiredConfig)
					.setOptionalConfigurationProperties(optionalConfig)
					.initialize();
			final ProteumClient client = injector
					.getInstance(ProteumClient.class);
			final PrestoProteumService proteum = injector
					.getInstance(PrestoProteumService.class);
			new Thread(new Runnable() {

				@Override
				public void run() {
					proteum.start(client);
				}
			}).start();
			return injector.getInstance(ProteumConnector.class);
		} catch (Throwable e) {
			throw new RuntimeException(e.getMessage() != null
					&& e.getMessage().length() > 250 ? e.getMessage()
					.substring(0, 250) : e.getMessage());
		}
	}
}