package com.facebook.presto.connector.proteum;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.configuration.ConfigurationModule.bindConfig;

import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
/**
 * 
 * @author Dilip Kasana
 * @Date 27 Mar 2015
 */
public class ProteumModule implements Module {
	private final String connectorId;
	private final TypeManager typeManager;

	public ProteumModule(String connectorId, TypeManager typeManager) {
		this.connectorId = checkNotNull(connectorId, "connector id is null");
		this.typeManager = checkNotNull(typeManager, "typeManager is null");
	}

	@Override
	public void configure(Binder binder) {
		binder.bind(TypeManager.class).toInstance(typeManager);
		binder.bind(ProteumClient.class).in(Scopes.SINGLETON);
		binder.bind(PrestoProteumService.class).in(Scopes.SINGLETON);
		binder.bind(ProteumSplitManager.class).in(Scopes.SINGLETON);
		binder.bind(ProteumRecordSetProvider.class).in(Scopes.SINGLETON);
		binder.bind(ProteumHandleResolver.class).in(Scopes.SINGLETON);
		binder.bind(ProteumConnector.class).in(Scopes.SINGLETON);
		binder.bind(ProteumMetadata.class).in(Scopes.SINGLETON);
		binder.bindConstant().annotatedWith(Names.named("connectorId"))
				.to(connectorId);
		bindConfig(binder).to(ProteumConfig.class);
	}
}
