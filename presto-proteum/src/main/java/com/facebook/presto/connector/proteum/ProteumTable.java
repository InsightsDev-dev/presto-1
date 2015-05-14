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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

public class ProteumTable {
    private final String name;
    private final List<ProteumColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private volatile List<URL> sources;
    private final String schema;
    private final String baseURL;
    private final boolean visible;

    @JsonCreator
    public ProteumTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<ProteumColumn> columns,
            @JsonProperty("sources") List<URL> sources,
            @JsonProperty("schema") String schema,
            @JsonProperty("baseURL") String baseURL,
            @JsonProperty("visible") boolean visible)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = checkNotNull(name, "name is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
        this.sources = ImmutableList.copyOf(checkNotNull(sources, "sources is null"));
        this.schema = schema;
        this.baseURL = baseURL;
        this.visible = visible;
        int index = 0;
        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (ProteumColumn column : this.columns) {
            columnsMetadata.add(new ProteumColumnMetaData(column.getName(), column.getType(),column.getProteumType(), index, false));
            index++;
        }
        this.columnsMetadata = columnsMetadata.build();
    }
    
    public boolean isVisible(){
        return this.visible;
    }
    
    public void updateSources() throws Exception{
        if(!visible) return;
        URL url = new URL(baseURL+"/splits/"+schema+"/"+name);
        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        connection.setRequestMethod("GET");
        connection.connect();
        BufferedReader in = new BufferedReader(new InputStreamReader(
                connection.getInputStream()));
        String[] splits = in.readLine().split("\\|");
        in.close();
        List<URL> urls = new ArrayList<URL>();
        for(String split : splits){
            urls.add(new URL(baseURL+"/print/"+schema+"/"+name+"/"+split));
        }
        this.sources = ImmutableList.copyOf(checkNotNull(urls, "sources is null"));
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<ProteumColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<URL> getSources()
    {
        return sources;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }

    public static Function<ProteumTable, String> nameGetter()
    {
        return new Function<ProteumTable, String>()
        {
            @Override
            public String apply(ProteumTable table)
            {
                return table.getName();
            }
        };
    }
}
