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
package com.facebook.presto.parquet.readerv2;

import com.facebook.presto.parquet.readerv2.decoders.DLDecoder;
import com.facebook.presto.parquet.readerv2.decoders.ValuesDecoder;

public class DLValuesDecoderInfo
{
    private final DLDecoder dlDecoder;
    private final ValuesDecoder valuesDecoder;
    private final int start;
    private final int end;

    public DLValuesDecoderInfo(DLDecoder dlDecoder, ValuesDecoder valuesDecoder, int start, int end)
    {
        this.dlDecoder = dlDecoder;
        this.valuesDecoder = valuesDecoder;
        this.start = start;
        this.end = end;
    }

    public DLDecoder getDlDecoder()
    {
        return dlDecoder;
    }

    public ValuesDecoder getValuesDecoder()
    {
        return valuesDecoder;
    }

    public int getStart()
    {
        return start;
    }

    public int getEnd()
    {
        return end;
    }
}
