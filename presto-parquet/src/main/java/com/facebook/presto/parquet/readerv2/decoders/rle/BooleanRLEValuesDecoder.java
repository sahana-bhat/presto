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
package com.facebook.presto.parquet.readerv2.decoders.rle;

import com.facebook.presto.parquet.readerv2.BytesUtils;
import com.facebook.presto.parquet.readerv2.decoders.BooleanValuesDecoder;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.ParquetDecodingException;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkState;

public class BooleanRLEValuesDecoder
        implements BooleanValuesDecoder
{
    private final ByteBuffer inputBuffer;

    private MODE mode;
    private int currentCount;
    private byte currentValue;

    private int currentByteOffset;
    private byte currentByte;

    public BooleanRLEValuesDecoder(ByteBuffer inputBuffer)
    {
        this.inputBuffer = inputBuffer;
    }

    /**
     * Copied from BytesUtils.readUnsignedVarInt(InputStream in)
     */
    public static int readUnsignedVarInt(ByteBuffer in)
    {
        int value = 0;

        int i;
        int b = in.get();
        for (i = 0; (b & 128) != 0; i += 7) {
            value |= (b & 127) << i;
            b = in.get();
        }

        return value | b << i;
    }

    @Override
    public void readNext(byte[] values, int offset, int length)
    {
        int destIndex = offset;
        int remainingToCopy = length;
        while (remainingToCopy > 0) {
            if (this.currentCount == 0) {
                this.readNext();
                if (this.currentCount == 0) {
                    break;
                }
            }

            int numEntriesToFill = Math.min(remainingToCopy, this.currentCount);
            int endIndex = destIndex + numEntriesToFill;
            switch (this.mode) {
                case RLE: {
                    byte rleValue = this.currentValue;
                    while (destIndex < endIndex) {
                        values[destIndex] = rleValue;
                        destIndex++;
                    }
                    break;
                }
                case PACKED: {
                    int remainingPackedBlock = numEntriesToFill;
                    if (currentByteOffset > 0) {
                        // read from the partial values remaining in current byte
                        int readChunk = Math.min(remainingPackedBlock, 8 - currentByteOffset);

                        final byte inValue = currentByte;
                        for (int i = 0; i < readChunk; i++) {
                            values[destIndex++] = (byte) (inValue >> currentByteOffset & 1);
                            currentByteOffset++;
                        }

                        remainingPackedBlock -= readChunk;
                        currentByteOffset = currentByteOffset % 8;
                    }

                    final ByteBuffer localInputBuffer = inputBuffer;
                    while (remainingPackedBlock >= 8) {
                        BytesUtils.unpack8Values(localInputBuffer.get(), values, destIndex);
                        remainingPackedBlock -= 8;
                        destIndex += 8;
                    }

                    if (remainingPackedBlock > 0) {
                        // read partial values from current byte until the requested length is satisfied
                        byte inValue = localInputBuffer.get();
                        for (int i = 0; i < remainingPackedBlock; i++) {
                            values[destIndex++] = (byte) (inValue >> i & 1);
                        }

                        currentByte = inValue;
                        currentByteOffset = remainingPackedBlock;
                    }

                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + this.mode);
            }

            this.currentCount -= numEntriesToFill;
            remainingToCopy -= numEntriesToFill;
        }
    }

    @Override
    public void skip(int length)
    {
        int remainingToSkip = length;
        while (remainingToSkip > 0) {
            if (this.currentCount == 0) {
                this.readNext();
                if (this.currentCount == 0) {
                    break;
                }
            }

            int numEntriesToSkip = Math.min(remainingToSkip, this.currentCount);
            switch (this.mode) {
                case RLE:
                    break;
                case PACKED: {
                    int remainingPackedBlock = numEntriesToSkip;
                    if (currentByteOffset > 0) {
                        // read from the partial values remaining in current byte
                        int skipChunk = Math.min(remainingPackedBlock, 8 - currentByteOffset);

                        currentByteOffset += skipChunk;

                        remainingPackedBlock -= skipChunk;
                        currentByteOffset = currentByteOffset % 8;
                    }

                    int fullBytes = remainingPackedBlock / 8;

                    if (fullBytes > 0) {
                        inputBuffer.position(inputBuffer.position() + fullBytes);
                    }

                    remainingPackedBlock = remainingPackedBlock % 8;

                    if (remainingPackedBlock > 0) {
                        // read partial values from current byte until the requested length is satisfied
                        currentByte = inputBuffer.get();
                        currentByteOffset = remainingPackedBlock;
                    }

                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + this.mode);
            }

            this.currentCount -= numEntriesToSkip;
            remainingToSkip -= numEntriesToSkip;
        }

        checkState(remainingToSkip == 0, "Invalid read size request");
    }

    private void readNext()
    {
        Preconditions.checkArgument(this.inputBuffer.hasRemaining(), "Reading past RLE/BitPacking stream.");
        int header = readUnsignedVarInt(this.inputBuffer);
        this.mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
        switch (this.mode) {
            case RLE:
                this.currentCount = header >>> 1;
                this.currentValue = inputBuffer.get();
                return;
            case PACKED:
                int numGroups = header >>> 1;
                this.currentCount = numGroups * 8;
                this.currentByteOffset = 0;
                return;
            default:
                throw new ParquetDecodingException("not a valid mode " + this.mode);
        }
    }

    private enum MODE
    {
        RLE,
        PACKED;
    }
}
