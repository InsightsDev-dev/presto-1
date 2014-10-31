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

import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Marker;
import com.facebook.presto.spi.Range;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

public class ProteumColumnFilter {
    private final ProteumColumnHandle columnHandle;
    private List<String> ranges;
    private final Domain domain;
    
    @JsonCreator
    public ProteumColumnFilter(@JsonProperty("columnHandle")ProteumColumnHandle columnHandle,
            @JsonProperty("domain")Domain domain){
        this.domain = domain;
        this.columnHandle = columnHandle;
        ranges = new ArrayList<String>();
        for(Range range : domain.getRanges()){
            String currentRange;
            if(range.isSingleValue()){
                currentRange = "["+getStringValue(range.getLow())+"%20"+getStringValue(range.getLow())+"]";
                ranges.add(currentRange);
            }
            else{
                StringBuilder sb = new StringBuilder();
                sb.append((range.getLow().getBound() == Marker.Bound.EXACTLY) ? '[' : '(');
                sb.append(range.getLow().isLowerUnbounded() ? "min" : getStringValue(range.getLow()));
                sb.append("%20");
                sb.append(range.getHigh().isUpperUnbounded() ? "max" : getStringValue( range.getHigh()));
                sb.append((range.getHigh().getBound() == Marker.Bound.EXACTLY) ? ']' : ')');
                ranges.add(sb.toString());
            }
        }
    }
    
    private String getStringValue(Marker marker){
        Object value = marker.getValue();
        if(value instanceof Slice){
                return new String(((Slice) value).getBytes());
        }
        else return value.toString();
    }
    
    @JsonProperty
    public Domain getDomain(){
        return this.domain;
    }
    @JsonProperty
    public ProteumColumnHandle getColumnHandle(){
        return this.columnHandle;
    }
    
    @Override
    public String toString() {
        String result = columnHandle.getColumnName()+":";
        result+=Joiner.on(":").join(ranges);
        return result;
    }
}
