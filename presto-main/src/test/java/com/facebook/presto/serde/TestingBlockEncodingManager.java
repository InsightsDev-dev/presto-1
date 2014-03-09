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
package com.facebook.presto.serde;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.dictionary.DictionaryBlockEncoding;
import com.facebook.presto.block.rle.RunLengthBlockEncoding;
import com.facebook.presto.block.snappy.SnappyBlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.type.BigintType;
import com.facebook.presto.type.BooleanType;
import com.facebook.presto.type.DoubleType;
import com.facebook.presto.type.NullType;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.type.VarcharType;

public final class TestingBlockEncodingManager
{
    private TestingBlockEncodingManager()
    {
    }

    public static BlockEncodingSerde createTestingBlockEncodingManager()
    {
        return new BlockEncodingManager(
                new TypeRegistry(),
                NullType.BLOCK_ENCODING_FACTORY,
                BooleanType.BLOCK_ENCODING_FACTORY,
                BigintType.BLOCK_ENCODING_FACTORY,
                DoubleType.BLOCK_ENCODING_FACTORY,
                VarcharType.BLOCK_ENCODING_FACTORY,
                RunLengthBlockEncoding.FACTORY,
                DictionaryBlockEncoding.FACTORY,
                SnappyBlockEncoding.FACTORY);
    }
}
