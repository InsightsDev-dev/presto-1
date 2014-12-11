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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

/**
 * To initialize all required objects using Guice.
 * 
 * @author dilipsingh
 * 
 */
public class MetadataModule implements Module {
	private final String connectorId;
	private final TypeManager typeManager;

	public MetadataModule(String connectorId, TypeManager typeManager) {
		this.connectorId = checkNotNull(connectorId, "connector id is null");
		this.typeManager = checkNotNull(typeManager, "typeManager is null");
	}

	@Override
	public void configure(Binder binder) {
		binder.bind(TypeManager.class).toInstance(typeManager);

		binder.bind(MetadataConnector.class).in(Scopes.SINGLETON);
		binder.bind(MetadataConnectorId.class).toInstance(
				new MetadataConnectorId(connectorId));
		binder.bind(MetadataMetadata.class).in(Scopes.SINGLETON);
		binder.bind(MetadataClient.class).in(Scopes.SINGLETON);
		binder.bind(MetadataSplitManager.class).in(Scopes.SINGLETON);
		bindConfig(binder).to(MetadataConfig.class);
		binder.bind(MetadataRecordSetProvider.class).in(Scopes.SINGLETON);
		binder.bind(MetadataHandleResolver.class).in(Scopes.SINGLETON);
		jsonBinder(binder).addDeserializerBinding(Type.class).to(
				TypeDeserializer.class);
		jsonCodecBinder(binder).bindMapJsonCodec(String.class,
				listJsonCodec(MetadataTable.class));
	}

	public static final class TypeDeserializer extends
			FromStringDeserializer<Type> {
		private static final long serialVersionUID = 1L;
		private final TypeManager typeManager;

		@Inject
		public TypeDeserializer(TypeManager typeManager) {
			super(Type.class);
			this.typeManager = checkNotNull(typeManager, "typeManager is null");
		}

		@Override
		protected Type _deserialize(String value, DeserializationContext context) {
			Type type = typeManager.getType(parseTypeSignature(value));
			checkArgument(type != null, "Unknown type %s", value);
			return type;
		}
	}
}
