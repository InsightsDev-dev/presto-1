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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.CountingInputStream;
import com.google.common.io.InputSupplier;
import com.mobileum.presto.proteum.datatype.IDataType;
import com.mobileum.presto.proteum.datatype.LongDataType;
import com.mobileum.presto.proteum.datatype.Tuple;
import com.mobileum.range.TimeStamp;
import com.mobileum.range.TimeStampRange;
import com.mobileum.range.presto.TSRange;
import com.mobileum.range.presto.TSRangeType;

public class ProteumRecordCursor implements RecordCursor {
	private static final Splitter LINE_SPLITTER = Splitter.on(";");

	private final List<ProteumColumnHandle> columnHandles;
	private IDataType[] proteumDataType;
	private final int[] fieldToColumnIndex;

	private final Iterator<UnsafeMemory> data;
	private UnsafeMemory current;
	private final long totalBytes;
	Object[] tempValues;
	private List<String> fields;

	public ProteumRecordCursor(List<ProteumColumnHandle> columnHandles,
			URL url, ProteumPredicatePushDown proteumPredicatePushDown) {
		final int listenPort = ProteumClient.getListenPort();
		this.columnHandles = columnHandles;
		fieldToColumnIndex = new int[columnHandles.size()];
		for (int i = 0; i < columnHandles.size(); i++) {
			fieldToColumnIndex[i] = i;
		}
		BufferedReader in = null;
		try {
			ProteumScanThread scanThread = new ProteumScanThread(listenPort);
			scanThread.setPriority(10);
			scanThread.start();
			while (!scanThread.isSocketAccepting()) {
				Thread.sleep(5);
			}
			String path = url.toString() + "?";
			String queryParameters = buildColumnURL(columnHandles);
			queryParameters += "&"
					+ buildFilterURL(proteumPredicatePushDown
							.getColumnFilters());
			queryParameters += "&"
					+ buildAggregates(proteumPredicatePushDown.getAggregates());
			queryParameters += "&"
					+ buildGroupBy(proteumPredicatePushDown.getGroupBy());
			queryParameters += "&" + "port=" + listenPort;
			queryParameters += "&" + "host="
					+ InetAddress.getLocalHost().getHostName();
			byte[] postData = queryParameters.getBytes(StandardCharsets.UTF_8);
			HttpURLConnection connection = (HttpURLConnection) url
					.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Length", ""
					+ postData.length);
			connection.setUseCaches(false);
			connection.setDoInput(true);
			connection.setDoOutput(true);
			try (DataOutputStream wr = new DataOutputStream(
					connection.getOutputStream())) {
				wr.write(postData);
			}
			in = new BufferedReader(new InputStreamReader(
					connection.getInputStream()));
			String inputLine;
			inputLine = in.readLine();
			String[] types = inputLine.split(":");
			proteumDataType = new IDataType[types.length];
			tempValues = new Object[types.length];
			for(int i = 0 ; i < proteumDataType.length ; i++){
			    proteumDataType[i] = IDataType.getProteumTypeFromString(types[i]);
			}
			while ((inputLine = in.readLine()) != null) {
			}
			List<String> tempLines = new ArrayList<String>();
			int length = 0;
			scanThread.setFinished(true);
			
			data = scanThread.getData().iterator();
			totalBytes = scanThread.getSize();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					System.out.println(e.getMessage());
				}
			}
		}

	}

	public static String buildAggregates(List<Expression> aggregates) {
		String defaultString = "aggregates=";
		if (aggregates == null || aggregates.isEmpty()) {
			return defaultString;
		}
		try {
			return "aggregates="
					+ URLEncoder.encode(Joiner.on(",").join(aggregates),
							"UTF-8");
		} catch (UnsupportedEncodingException e) {
			System.out
					.println("ERROR for encoding aggregates" + e.getMessage());
			return defaultString;
		}
	}

	public static String buildGroupBy(List<Symbol> groupBy) {
		String defaultString = "groupBy=";
		if (groupBy == null || groupBy.isEmpty()) {
			return defaultString;
		}
		try {
			return "groupBy="
					+ URLEncoder.encode(Joiner.on(",").join(groupBy), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			System.out.println("ERROR for encoding groupBy" + e.getMessage());
			return defaultString;
		}
	}

	public static String buildColumnURL(List<ProteumColumnHandle> columns) {
		List<String> columnsName = new ArrayList<String>();
		for (ProteumColumnHandle column : columns)
			columnsName.add(column.getColumnName());
		try {
			return "columns="
					+ URLEncoder.encode(Joiner.on(",").join(columnsName),
							"UTF-8");
		} catch (UnsupportedEncodingException e) {
			System.out.println("ERROR for encoding column" + e.getMessage());
			return "columns=";
		}
	}

	private String buildFilterURL(List<ProteumColumnFilter> filters) {

		try {
			return "filters="
					+ URLEncoder.encode(Joiner.on("&&").join(filters), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			System.out.println("ERROR for encoding filter " + e.getMessage());
			return "filters=";
		}
	}

	@Override
	public long getTotalBytes() {
		return totalBytes;
	}

	@Override
	public long getCompletedBytes() {
		return totalBytes;
	}

	@Override
	public long getReadTimeNanos() {
		return 0;
	}

	@Override
	public Type getType(int field) {
		checkArgument(field < columnHandles.size(), "Invalid field index");
		return columnHandles.get(field).getColumnType();
	}

	@Override
	public boolean advanceNextPosition() {
	    if(current == null){
	        if(data.hasNext()) current = data.next();
	        else return false;
	    }
		if(current.hasNext()){
		    moveCursorToNext();
		    return true;
		}
		else{
		    while(data.hasNext()){
		        current = data.next();
		        if(current.hasNext()) {
		            moveCursorToNext();
		            return true;
		        }
		    }
		}
		return false;
	}

	public void moveCursorToNext(){
	    for(int i = 0 ; i < proteumDataType.length ; i++){
	        if(current.getBoolean() == true){
	        tempValues[i] = proteumDataType[i].readData(current);
	        }
	        else tempValues[i] = null;
	    }
	}

	@Override
	public boolean getBoolean(int field) {
	    return (boolean)tempValues[field];
	}

	@Override
	public long getLong(int field) {
	    Number n = (Number)tempValues[field];
	    return n.longValue();
	}

	@Override
	public double getDouble(int field) {
	    Number n = (Number)tempValues[field];
        return n.doubleValue();
	}

	@Override
	public Slice getSlice(int field) {
	    if (getType(field).equals(TSRangeType.TS_RANGE_TYPE)) {
            Tuple<Long, Long> r = (Tuple)tempValues[field];
            Long min = r.getFirst();
            Long max = r.getSecond();
            return TSRange.serialize(TSRange.createRange("["
                    + (min == -1 ? "" : new TimeStamp(min * 1000)) + ","
                    + (max == 0 ? "" : new TimeStamp(max * 1000)) + "]"));
        }
	    return Slices.utf8Slice((String)tempValues[field]);
	}

	@Override
	public boolean isNull(int field) {
		return tempValues[field] == null;
	}

	private void checkFieldType(int field, Type expected) {
		Type actual = getType(field);
		checkArgument(actual.equals(expected),
				"Expected field %s to be type %s but is %s", field, expected,
				actual);
	}

	@Override
	public void close() {
	}
}
