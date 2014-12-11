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
package com.mobileum.presto.metadata;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.facebook.presto.sql.tree.QualifiedName;

public class MetdataRecordset {

	public static void main(String[] args) {
//		List<FunctionInfo> customFunctions = new FunctionRegistry.FunctionListBuilder()
//				.aggregate("custom_cardinality", VARCHAR,
//						ImmutableList.of(VARCHAR), VARCHAR,
//						new AggregateCardinality())
//				.scalar(ScalarCardinality.class).getFunctions();
//		System.out.println(customFunctions.size());
//		for (FunctionInfo f : customFunctions) {
//			System.out.println(f.getName());
//		}
		MetadataConfig config = new MetadataConfig();
		RecordSet recordSet = new MetadataRecordSet(new MetadataSplit("test",
				"schema", "partitiondata", ""), config, ImmutableList.of(
				new MetadataColumnHandle("test", "modelname", VARCHAR, 0),
				new MetadataColumnHandle("test", "partitionid", VARCHAR, 1),
				new MetadataColumnHandle("test", "columname", VARCHAR, 2),
				new MetadataColumnHandle("test", "min", VARCHAR, 3),
				new MetadataColumnHandle("test", "max", VARCHAR, 4),
				new MetadataColumnHandle("test", "cardinality", VARCHAR, 5)));
		RecordCursor cursor = recordSet.cursor();

		Map<String, Long> data = new LinkedHashMap<>();
		while (cursor.advanceNextPosition()) {
			System.out.println(cursor.getSlice(0).toStringUtf8());
			System.out.println(cursor.getSlice(1).toStringUtf8());
			System.out.println(cursor.getSlice(2).toStringUtf8());
			System.out.println(cursor.getSlice(3).toStringUtf8());
			System.out.println(cursor.getSlice(4).toStringUtf8());
			System.out.println(cursor.getSlice(5).toStringUtf8());

			// assertEquals(cursor.getLong(0), cursor.getLong(1));
			// data.put(cursor.getSlice(2).toStringUtf8(), cursor.getLong(0));
		}
		// assertEquals(data, ImmutableMap.<String, Long> builder()
		// .put("ten", 10L).put("eleven", 11L).put("twelve", 12L).build());

	}

}
