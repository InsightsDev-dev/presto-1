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

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.CountingInputStream;
import com.google.common.io.InputSupplier;

public class ProteumRecordCursor implements RecordCursor
{
    private static final Splitter LINE_SPLITTER = Splitter.on(";").trimResults();

    private final List<ProteumColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final Iterator<String> lines;
    private final long totalBytes;

    private List<String> fields;

    public ProteumRecordCursor(List<ProteumColumnHandle> columnHandles, URL url, List<ProteumColumnFilter> filters)
    {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            fieldToColumnIndex[i] = i;
        }
        try{
            String path = url.toString()+"?";
            path+=buildColumnURL(columnHandles)+":";
            path+=buildFilterURL(filters);
            url = new URL(path);
            HttpURLConnection connection = (HttpURLConnection)url.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String inputLine;
            List<String> tempLines = new ArrayList<String>();
            int length = 0;
            while ((inputLine = in.readLine()) != null) {
                tempLines.add(inputLine);
                length+=inputLine.length();
            }
            lines = tempLines.iterator();
            totalBytes = length;
        }
        catch(Exception e){
            throw new RuntimeException(e);
        }
        
    }
    
    private String buildColumnURL(List<ProteumColumnHandle> columns){
        List<String> columnsName = new ArrayList<String>();
        for(ProteumColumnHandle column : columns) columnsName.add(column.getColumnName());
        return "columns={"+Joiner.on(",").join(columnsName)+"}";
    }
    
    private String buildFilterURL(List<ProteumColumnFilter> filters){
        
        
        
        return "filters={"+Joiner.on("&&").join(filters)+"}";
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!lines.hasNext()) {
            return false;
        }
        String line = lines.next();
        fields = LINE_SPLITTER.splitToList(line);

        return true;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yes");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, VARCHAR);
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}
