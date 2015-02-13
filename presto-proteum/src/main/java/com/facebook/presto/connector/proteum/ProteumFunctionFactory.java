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

import java.util.List;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.spi.type.TypeManager;
import com.mobileum.range.presto.Int4RangeOperators;
import com.mobileum.range.presto.TSRangeOperators;
import com.mobileum.range.presto.TSTZRangeOperators;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class ProteumFunctionFactory implements FunctionFactory {
	TypeManager t;

	public ProteumFunctionFactory(TypeManager typeManager) {
		this.t = typeManager;
	}

	@Override
	public List<ParametricFunction> listFunctions() {
		List<ParametricFunction> customFunctions = new FunctionListBuilder(t)

		.scalar(Int4RangeOperators.class).scalar(TSTZRangeOperators.class)
				.scalar(TSRangeOperators.class).getFunctions();

		return customFunctions;
	}
}
