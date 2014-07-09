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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

public class TextRecordSetProvider implements ConnectorRecordSetProvider{
    
    @Override
    public RecordSet getRecordSet(ConnectorSplit split,
            List<? extends ConnectorColumnHandle> columns) {
        // TODO Auto-generated method stub
        TextSplit tSplit = (TextSplit)split;
        ImmutableList.Builder<TextColumnHandle> handles = ImmutableList.builder();
        for (ConnectorColumnHandle handle : columns) {
            checkArgument(handle instanceof TextColumnHandle);
            handles.add((TextColumnHandle) handle);
        }
        return new TextRecordSet(tSplit, handles.build());
    }

}
