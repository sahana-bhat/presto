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

import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.reader.ColumnChunk;
import com.facebook.presto.parquet.readerv2.decoders.BinaryValuesDecoder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.block.VariableWidthBlock;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class BinarySimpleColumnReaderV2
        extends AbstractSimpleColumnReaderV2
{
    public BinarySimpleColumnReaderV2(RichColumnDescriptor columnDescriptor)
    {
        super(columnDescriptor);
    }

    @Override
    protected ColumnChunk readWithNull()
            throws IOException
    {
        boolean[] isNull = new boolean[nextBatchSize];

        List<BinaryValuesDecoder.ReadChunk> readChunkList = new ArrayList<>();
        List<ValuesDecoderInfo> valuesDecoderInfos = new ArrayList<>();
        int bufferSize = 0;

        int totalNonNullCount = 0;
        int remainingInBatch = nextBatchSize;
        int startOffset = 0;
        while (remainingInBatch > 0) {
            if (remainingCountInPage == 0) {
                if (!readNextPage()) {
                    break;
                }
            }

            int readChunkSize = Math.min(remainingCountInPage, remainingInBatch);
            int nonNullCount = dlDecoder.readNext(isNull, startOffset, readChunkSize);
            totalNonNullCount += nonNullCount;

            BinaryValuesDecoder.ReadChunk readChunk = ((BinaryValuesDecoder) valuesDecoder).readNext(nonNullCount);

            bufferSize += readChunk.getBufferSize();
            readChunkList.add(readChunk);

            ValuesDecoderInfo valuesDecoderInfo = new ValuesDecoderInfo(valuesDecoder, startOffset, startOffset + readChunkSize);
            valuesDecoderInfo.setValueCount(readChunkSize);
            valuesDecoderInfo.setNonNullCount(nonNullCount);
            valuesDecoderInfos.add(valuesDecoderInfo);

            startOffset += readChunkSize;
            remainingInBatch -= readChunkSize;
            remainingCountInPage -= readChunkSize;
        }

        if (totalNonNullCount == 0) {
            Block block = RunLengthEncodedBlock.create(field.getType(), null, nextBatchSize);
            return new ColumnChunk(block, new int[0], new int[0]);
        }

        byte[] byteBuffer = new byte[bufferSize];
        int[] offsets = new int[nextBatchSize + 1];

        int i = 0;
        int bufferIdx = 0;
        int offsetIdx = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : valuesDecoderInfos) {
            BinaryValuesDecoder binaryValuesDecoder = (BinaryValuesDecoder) valuesDecoderInfo.getValuesDecoder();
            BinaryValuesDecoder.ReadChunk readChunk = readChunkList.get(i);
            bufferIdx = binaryValuesDecoder.readIntoBuffer(byteBuffer, bufferIdx, offsets, offsetIdx, readChunk);
            offsetIdx += valuesDecoderInfo.getValueCount();
            i++;
        }

        Collections.reverse(valuesDecoderInfos);
        for (ValuesDecoderInfo valuesDecoderInfo : valuesDecoderInfos) {
            int destIdx = valuesDecoderInfo.getEnd() - 1;
            int srcIdx = valuesDecoderInfo.getStart() + valuesDecoderInfo.getNonNullCount() - 1;

            offsets[destIdx + 1] = offsets[srcIdx + 1];
            while (destIdx >= valuesDecoderInfo.getStart()) {
                if (!isNull[destIdx]) {
                    offsets[destIdx] = offsets[srcIdx];
                    srcIdx--;
                }
                else {
                    offsets[destIdx] = offsets[srcIdx + 1];
                }
                destIdx--;
            }
        }

        Slice buffer = Slices.wrappedBuffer(byteBuffer, 0, bufferSize);
        boolean hasNoNull = totalNonNullCount == nextBatchSize;
        Block block = new VariableWidthBlock(nextBatchSize, buffer, offsets, hasNoNull ? Optional.empty() : Optional.of(isNull));
        return new ColumnChunk(block, new int[0], new int[0]);
    }

    @Override
    protected ColumnChunk readNoNull()
            throws IOException
    {
        boolean[] isNull = new boolean[nextBatchSize];

        List<BinaryValuesDecoder.ReadChunk> readChunkList = new ArrayList<>();
        List<ValuesDecoderInfo> valuesDecoderInfos = new ArrayList<>();
        int bufferSize = 0;

        int remainingInBatch = nextBatchSize;
        int startOffset = 0;
        while (remainingInBatch > 0) {
            if (remainingCountInPage == 0) {
                if (!readNextPage()) {
                    break;
                }
            }

            int readChunkSize = Math.min(remainingCountInPage, remainingInBatch);

            BinaryValuesDecoder.ReadChunk readChunk = ((BinaryValuesDecoder) valuesDecoder).readNext(readChunkSize);

            bufferSize += readChunk.getBufferSize();
            readChunkList.add(readChunk);

            ValuesDecoderInfo valuesDecoderInfo = new ValuesDecoderInfo(valuesDecoder, startOffset, startOffset + readChunkSize);
            valuesDecoderInfo.setValueCount(readChunkSize);
            valuesDecoderInfo.setNonNullCount(readChunkSize);
            valuesDecoderInfos.add(valuesDecoderInfo);

            startOffset += readChunkSize;
            remainingInBatch -= readChunkSize;
            remainingCountInPage -= readChunkSize;
        }

        byte[] byteBuffer = new byte[bufferSize];
        int[] offsets = new int[nextBatchSize + 1];

        int i = 0;
        int bufferIdx = 0;
        int offsetIdx = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : valuesDecoderInfos) {
            BinaryValuesDecoder binaryValuesDecoder = (BinaryValuesDecoder) valuesDecoderInfo.getValuesDecoder();
            BinaryValuesDecoder.ReadChunk readChunk = readChunkList.get(i);
            bufferIdx = binaryValuesDecoder.readIntoBuffer(byteBuffer, bufferIdx, offsets, offsetIdx, readChunk);
            offsetIdx += valuesDecoderInfo.getValueCount();
            i++;
        }

        Slice buffer = Slices.wrappedBuffer(byteBuffer, 0, bufferSize);
        Block block = new VariableWidthBlock(nextBatchSize, buffer, offsets, Optional.of(isNull));
        return new ColumnChunk(block, new int[0], new int[0]);
    }

    @Override
    protected void skip(int skipSize)
            throws IOException
    {
        if (columnDescriptor.isRequired()) {
            int remainingInBatch = skipSize;
            while (remainingInBatch > 0) {
                if (remainingCountInPage == 0) {
                    if (!readNextPage()) {
                        break;
                    }
                }

                int readChunkSize = Math.min(remainingCountInPage, remainingInBatch);
                ((BinaryValuesDecoder) valuesDecoder).skip(readChunkSize);

                remainingInBatch -= readChunkSize;
                remainingCountInPage -= readChunkSize;
            }
        }
        else {
            boolean[] isNull = new boolean[skipSize];
            int remainingInBatch = skipSize;
            int startOffset = 0;
            while (remainingInBatch > 0) {
                if (remainingCountInPage == 0) {
                    if (!readNextPage()) {
                        break;
                    }
                }

                int readChunkSize = Math.min(remainingCountInPage, remainingInBatch);
                int nonNullCount = dlDecoder.readNext(isNull, startOffset, readChunkSize);
                ((BinaryValuesDecoder) valuesDecoder).skip(nonNullCount);

                startOffset += readChunkSize;
                remainingInBatch -= readChunkSize;
                remainingCountInPage -= readChunkSize;
            }
        }
    }
}
