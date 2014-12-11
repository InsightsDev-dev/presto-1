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

import java.util.List;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.spi.type.TypeManager;

public class MetadataFunctionFactory implements FunctionFactory {
	TypeManager t;

	public MetadataFunctionFactory(TypeManager typeManager) {
		this.t = typeManager;
	}

	@Override
	public List<ParametricFunction> listFunctions() {
		List<ParametricFunction> customFunctions = new FunctionListBuilder(t)// .aggregate(new
																				// InternalAggregationFunction(name,
																				// parameterTypes,
																				// intermediateType,
																				// finalType,
																				// decomposable,
																				// approximate,
																				// factory))
				.scalar(ScalarCardinality.class).getFunctions();
		/*
		 * .aggregate("agg_cardinality", VARCHAR, ImmutableList.of(VARCHAR),
		 * VARCHAR, new AggregateCardinality())
		 */
		return customFunctions;
	}

	// public List<FunctionInfo> listFunctions() {

	// }

}
