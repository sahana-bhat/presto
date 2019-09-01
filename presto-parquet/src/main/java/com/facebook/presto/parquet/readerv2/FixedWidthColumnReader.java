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
import com.facebook.presto.parquet.readerv2.TemplateVariables.TemplateArrayBlock;
import com.facebook.presto.parquet.readerv2.TemplateVariables.TemplateValue;
import com.facebook.presto.parquet.readerv2.TemplateVariables.TemplateValuesDecoder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.util.Optional;

public class FixedWidthColumnReader
        extends AbstractSimpleColumnReaderV2
{
    public FixedWidthColumnReader(RichColumnDescriptor columnDescriptor)
    {
        super(columnDescriptor);
    }

    @Override
    protected ColumnChunk readWithNull()
            throws IOException
    {
        TemplateValue[] values = new TemplateValue[nextBatchSize];
        boolean[] isNull = new boolean[nextBatchSize];

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

            if (nonNullCount > 0) {
                ((TemplateValuesDecoder) valuesDecoder).readNext(values, startOffset, nonNullCount);

                int valueDestIdx = startOffset + readChunkSize - 1;
                int valueSrcIdx = startOffset + nonNullCount - 1;

                while (valueDestIdx >= startOffset) {
                    if (!isNull[valueDestIdx]) {
                        values[valueDestIdx] = values[valueSrcIdx];
                        valueSrcIdx--;
                    }
                    valueDestIdx--;
                }
            }

            startOffset += readChunkSize;
            remainingInBatch -= readChunkSize;
            remainingCountInPage -= readChunkSize;
        }

        if (remainingInBatch != 0) {
            throw new ParquetDecodingException("Still remaining to be read in current batch.");
        }

        if (totalNonNullCount == 0) {
            Block block = RunLengthEncodedBlock.create(field.getType(), null, nextBatchSize);
            return new ColumnChunk(block, new int[0], new int[0]);
        }

        boolean hasNoNull = totalNonNullCount == nextBatchSize;
        Block block = new TemplateArrayBlock(nextBatchSize, hasNoNull ? Optional.empty() : Optional.of(isNull), values);
        return new ColumnChunk(block, new int[0], new int[0]);
    }

    @Override
    protected ColumnChunk readNoNull()
            throws IOException
    {
        TemplateValue[] values = new TemplateValue[nextBatchSize];

        int remainingInBatch = nextBatchSize;
        int startOffset = 0;
        while (remainingInBatch > 0) {
            if (remainingCountInPage == 0) {
                if (!readNextPage()) {
                    break;
                }
            }

            int readChunkSize = Math.min(remainingCountInPage, remainingInBatch);

            ((TemplateValuesDecoder) valuesDecoder).readNext(values, startOffset, readChunkSize);

            startOffset += readChunkSize;
            remainingInBatch -= readChunkSize;
            remainingCountInPage -= readChunkSize;
        }

        if (remainingInBatch != 0) {
            throw new ParquetDecodingException("Still remaining to be read in current batch.");
        }

        Block block = new TemplateArrayBlock(nextBatchSize, Optional.empty(), values);
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
                ((TemplateValuesDecoder) valuesDecoder).skip(readChunkSize);

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
                ((TemplateValuesDecoder) valuesDecoder).skip(nonNullCount);

                startOffset += readChunkSize;
                remainingInBatch -= readChunkSize;
                remainingCountInPage -= readChunkSize;
            }
        }
    }
}
