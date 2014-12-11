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
    	StringBuilder sb = new StringBuilder();
    	int domainNum = 1;
    	int numOfDomains = domain.getRanges().getRangeCount();
    	String columnName = columnHandle.getColumnName();
    	System.out.println(domain.getRanges());
    	sb.append("(");
		for (Range range : domain.getRanges()) {
			if (range.isSingleValue()) {
				sb.append(columnName);
				sb.append("=");
				sb.append(getStringValue(range.getLow()));
			} else {
				// filter corresponds to <=, <
				if(range.getLow().isLowerUnbounded() && range.getHigh().getBound() == Marker.Bound.EXACTLY) {
					sb.append(columnName);
					sb.append("%20");
					sb.append("<=");
					sb.append("%20");
					sb.append(getStringValue(range.getHigh()));
					continue;
				}else if(range.getLow().isLowerUnbounded()){
					sb.append(columnName);
					sb.append("%20");
					sb.append("<");
					sb.append("%20");
					sb.append(getStringValue(range.getHigh()));
					continue;
				}
				// filter corresponds to >=, >
				if(range.getHigh().isUpperUnbounded() && range.getLow().getBound() == Marker.Bound.EXACTLY){
					sb.append(columnName);
					sb.append("%20");
					sb.append(">=");
					sb.append("%20");
					sb.append(getStringValue(range.getLow()));
					continue;
				}else if(range.getHigh().isUpperUnbounded()){
					sb.append(columnName);
					sb.append("%20");
					sb.append(">");
					sb.append("%20");
					sb.append(getStringValue(range.getLow()));
					continue;
				}
				//bounded range filters
				sb.append(columnName);
				sb.append("%20");
				if(range.getLow().getBound() == Marker.Bound.EXACTLY){
					sb.append(">=");
				}else{
					sb.append(">");
				}
				sb.append("%20");
				sb.append(getStringValue(range.getLow()));
				sb.append("%20");
				sb.append("&&");
				
				sb.append(columnName);
				sb.append("%20");
				if(range.getHigh().getBound() == Marker.Bound.EXACTLY){
					sb.append("<=");
				}else{
					sb.append("<");
				}
				sb.append(getStringValue(range.getHigh()));
				sb.append("%20");
			}
		
			if(domainNum != numOfDomains){
				sb.append("||");
			}
			domainNum++;
		}
		sb.append(")");
        return sb.toString();
    }
}
