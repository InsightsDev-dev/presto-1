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

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;

public class ProteumRecordSet implements RecordSet
{
    private final List<ProteumColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final URL url;
    private final ProteumPredicatePushDown proteumPredicatePushDown;

    public ProteumRecordSet(ProteumSplit split, List<ProteumColumnHandle> columnHandles)
    {
        this.columnHandles = columnHandles;
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ProteumColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        url = split.getUri();
        this.proteumPredicatePushDown = split.getProteumPredicatePushDown();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new ProteumRecordCursor(columnHandles, url, proteumPredicatePushDown);
    }
}