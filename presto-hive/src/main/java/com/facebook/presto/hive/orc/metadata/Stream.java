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
package com.facebook.presto.hive.orc.metadata;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class Stream
{
    public enum StreamKind
    {
        PRESENT,
        DATA,
        LENGTH,
        DICTIONARY_DATA,
        DICTIONARY_COUNT,
        SECONDARY,
        ROW_INDEX,
        NANO_DATA,
        IN_DICTIONARY,
        ROW_GROUP_DICTIONARY,
        ROW_GROUP_DICTIONARY_LENGTH,
    }

    private final int column;
    private final StreamKind streamKind;
    private final int length;

    public Stream(int column, StreamKind streamKind, int length)
    {
        this.column = column;
        this.streamKind = checkNotNull(streamKind, "streamKind is null");
        this.length = length;
    }

    public int getColumn()
    {
        return column;
    }

    public StreamKind getStreamKind()
    {
        return streamKind;
    }

    public int getLength()
    {
        return length;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("column", column)
                .add("streamKind", streamKind)
                .add("length", length)
                .toString();
    }
}
