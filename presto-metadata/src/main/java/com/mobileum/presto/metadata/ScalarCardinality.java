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

import io.airlift.slice.Slice;

import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Charsets;
import com.mobileum.common.analytics.cardinality.ICardinality;
import com.mobileum.metadata.columnmetadata.impl.StringColumnMetadata;

public class ScalarCardinality {
	@ScalarFunction
	@SqlType(StandardTypes.BIGINT)
	public static long castCardinality(
			@SqlType(StandardTypes.VARCHAR) Slice cardinality) {
		String cardinalityString = cardinality.toString(Charsets.UTF_8);
		StringColumnMetadata stringColumnMetadata = new StringColumnMetadata();
		ICardinality card = stringColumnMetadata
				.getCardinality(cardinalityString);
		return card.cardinality();
	}
}