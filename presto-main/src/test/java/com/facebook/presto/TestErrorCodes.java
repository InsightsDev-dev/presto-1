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
package com.facebook.presto;

import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import org.testng.annotations.Test;

import java.util.Set;

import static io.airlift.testing.Assertions.assertLessThanOrEqual;
import static org.testng.Assert.assertEquals;

public class TestErrorCodes
{
    @Test
    public void testUnique()
    {
        Set<Integer> codes = IterableTransformer.on(StandardErrorCode.values()).transform(new Function<StandardErrorCode, Integer>()
        {
            @Override
            public Integer apply(StandardErrorCode input)
            {
                return input.toErrorCode().getCode();
            }
        }).set();

        assertEquals(codes.size(), StandardErrorCode.values().length);
    }

    @Test
    public void testReserved()
    {
        for (StandardErrorCode errorCode : StandardErrorCode.values()) {
            assertLessThanOrEqual(errorCode.toErrorCode().getCode(), StandardErrorCode.EXTERNAL.toErrorCode().getCode());
        }
    }
}
