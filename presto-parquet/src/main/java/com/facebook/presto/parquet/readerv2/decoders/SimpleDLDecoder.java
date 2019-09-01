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

package com.facebook.presto.parquet.readerv2.decoders;

import com.facebook.presto.parquet.readerv2.decoders.rle.BaseRLEBitPackedDecoder;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkState;

public class SimpleDLDecoder
        extends BaseRLEBitPackedDecoder
{
    public SimpleDLDecoder(int valueCount, InputStream in)
    {
        super(valueCount, 1, in);
    }

    public SimpleDLDecoder(int rleValue, int valueCount)
    {
        super(rleValue, valueCount);
    }

    public int readNext(boolean[] values, int offset, int length)
            throws IOException
    {
        int nonNullCount = 0;
        int destIndex = offset;
        int remainingToCopy = length;
        while (remainingToCopy > 0) {
            if (this.currentCount == 0) {
                if (!this.readNext()) {
                    break;
                }
            }

            int readChunkSize = Math.min(remainingToCopy, this.currentCount);
            int endIndex = destIndex + readChunkSize;
            switch (this.mode) {
                case RLE: {
                    boolean rleValue = this.currentValue == 0;
                    while (destIndex < endIndex) {
                        values[destIndex++] = rleValue;
                    }
                    nonNullCount += this.currentValue * readChunkSize;
                    break;
                }
                case PACKED: {
                    int[] currentBuffer = this.currentBuffer;
                    for (int srcIdx = currentBuffer.length - this.currentCount; destIndex < endIndex; srcIdx++, destIndex++) {
                        final int value = currentBuffer[srcIdx];
                        values[destIndex] = value == 0;
                        nonNullCount += value;
                    }
                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + this.mode);
            }
            this.currentCount -= readChunkSize;
            remainingToCopy -= readChunkSize;
        }

        checkState(remainingToCopy == 0, "Failed to copy the requested number of DLs");
        return nonNullCount;
    }
}
