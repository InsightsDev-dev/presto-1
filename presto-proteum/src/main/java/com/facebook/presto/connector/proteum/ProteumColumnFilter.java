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

import javax.annotation.Nullable;

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
	private final String remainingExpression;

	@JsonCreator
	public ProteumColumnFilter(
			@JsonProperty("columnHandle") ProteumColumnHandle columnHandle,
			@JsonProperty("domain") Domain domain,
			@Nullable @JsonProperty("remainingExpression") String remainingExpression) {
		this.domain = domain;
		this.columnHandle = columnHandle;
		ranges = new ArrayList<String>();
		this.remainingExpression = remainingExpression;
	}

	private String getStringValue(Marker marker) {
		Object value = marker.getValue();
		if (value instanceof Slice) {
			return "'" + new String(((Slice) value).getBytes()) + "'";
		} else
			return value.toString();
	}

	@JsonProperty
	public Domain getDomain() {
		return this.domain;
	}

	@JsonProperty
	public ProteumColumnHandle getColumnHandle() {
		return this.columnHandle;
	}

	@JsonProperty
	public String getRemainingExpression() {
		return remainingExpression;
	}

	@Override
	public String toString() {
		if (remainingExpression != null && remainingExpression.length() > 0) {
			return remainingExpression.toString();
		}
		StringBuilder sb = new StringBuilder();
		int domainNum = 1;
		int numOfDomains = domain.getRanges().getRangeCount();
		String columnName = columnHandle.getColumnName();
		// System.out.println(domain.getRanges());

		sb.append("(");
		if (domain.isNone()) {
			sb.append(columnName);
			sb.append(" is NULL");
			sb.append(")");
			return sb.toString();
		}
		if (domain.isNullAllowed()) {
			sb.append("(");
			sb.append(columnName);
			sb.append(" is NULL");
			sb.append(")");
			if (domain.getRanges().getRanges().size() > 0) {
				sb.append("||");
			}
		}
		for (Range range : domain.getRanges()) {
			sb.append("(");
			if (range.isAll()) {
				sb.append(columnName);
				sb.append(" is NOT NULL");
			} else if (range.isSingleValue()) {
				sb.append(columnName);
				sb.append("=");
				sb.append(getStringValue(range.getLow()));
			} else if (range.getLow().isLowerUnbounded()
					|| range.getHigh().isUpperUnbounded()) {
				// filter corresponds to <=, <
				if (range.getLow().isLowerUnbounded()
						&& range.getHigh().getBound() == Marker.Bound.EXACTLY) {
					sb.append(columnName);
					sb.append(" ");
					sb.append("<=");
					sb.append(" ");
					sb.append(getStringValue(range.getHigh()));
				} else if (range.getLow().isLowerUnbounded()) {
					sb.append(columnName);
					sb.append(" ");
					sb.append("<");
					sb.append(" ");
					sb.append(getStringValue(range.getHigh()));
				}
				// filter corresponds to >=, >
				if (range.getHigh().isUpperUnbounded()
						&& range.getLow().getBound() == Marker.Bound.EXACTLY) {
					sb.append(columnName);
					sb.append(" ");
					sb.append(">=");
					sb.append(" ");
					sb.append(getStringValue(range.getLow()));
				} else if (range.getHigh().isUpperUnbounded()) {
					sb.append(columnName);
					sb.append(" ");
					sb.append(">");
					sb.append(" ");
					sb.append(getStringValue(range.getLow()));
				}
			} else {
				// bounded range filters
				sb.append(columnName);
				sb.append(" ");
				if (range.getLow().getBound() == Marker.Bound.EXACTLY) {
					sb.append(">=");
				} else {
					sb.append(">");
				}
				sb.append(" ");
				sb.append(getStringValue(range.getLow()));
				sb.append(" ");
				sb.append("&&");

				sb.append(columnName);
				sb.append(" ");
				if (range.getHigh().getBound() == Marker.Bound.EXACTLY) {
					sb.append("<=");
				} else {
					sb.append("<");
				}
				sb.append(getStringValue(range.getHigh()));
				sb.append(" ");
			}
			sb.append(")");
			if (domainNum < numOfDomains) {
				sb.append("||");
			}
			domainNum++;
		}
		sb.append(")");
		return sb.toString();
	}
}
