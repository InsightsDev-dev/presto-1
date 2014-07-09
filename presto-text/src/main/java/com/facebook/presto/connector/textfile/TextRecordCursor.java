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
package com.facebook.presto.connector.textfile;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.ByteStreams.asByteSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.CountingInputStream;
import com.google.common.io.InputSupplier;

public class TextRecordCursor implements RecordCursor{
    
    private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private List<TextColumnHandle> columnHandles;
    private int[] fieldToColumnIndex;
    private Queue<String> lines;
    private long totalBytes;
    private List<String> fields;
    
    public TextRecordCursor(List<TextColumnHandle> columnHandles, String stream)
    {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            TextColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }
        totalBytes = 0;
        lines = new LinkedList<String>();
        BufferedReader br= null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(stream))));
        } catch (FileNotFoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        String line;
        try {
            while((line = br.readLine()) != null){
                totalBytes+=line.length();
                lines.add(line);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public long getTotalBytes() {
        // TODO Auto-generated method stub
        return totalBytes;
    }

    @Override
    public long getCompletedBytes() {
        // TODO Auto-generated method stub
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Type getType(int field) {
        // TODO Auto-generated method stub
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition() {
        // TODO Auto-generated method stub
        if (lines.isEmpty()) {
            return false;
        }
        String line = lines.poll();
        fields = LINE_SPLITTER.splitToList(line);

        return true;
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
    public Slice getSlice(int field) {
        checkFieldType(field, VARCHAR);
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }
    
    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }
    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yes");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
    }
    
    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

}
