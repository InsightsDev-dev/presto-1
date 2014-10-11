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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.guice.AbstractConfigurationAwareModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.dbpool.H2EmbeddedDataSourceModule;
import io.airlift.dbpool.MySqlDataSourceModule;

import java.lang.annotation.Annotation;

import static com.facebook.presto.raptor.util.ConditionalModule.installIfPropertyEquals;
import static com.facebook.presto.raptor.util.DbiProvider.bindDbiToDataSource;
import static com.facebook.presto.raptor.util.DbiProvider.bindResultSetMapper;

public class MetadataModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(ShardManager.class).to(DatabaseShardManager.class).in(Scopes.SINGLETON);

        bindDataSource(binder, "metadata", ForMetadata.class);
        bindResultSetMapper(binder, TableColumn.Mapper.class, ForMetadata.class);
    }

    private void bindDataSource(Binder binder, String type, Class<? extends Annotation> annotation)
    {
        String property = type + ".db.type";
        install(installIfPropertyEquals(new MySqlDataSourceModule(type, annotation), property, "mysql"));
        install(installIfPropertyEquals(new H2EmbeddedDataSourceModule(type, annotation), property, "h2"));

        bindDbiToDataSource(binder, annotation);
    }
}
