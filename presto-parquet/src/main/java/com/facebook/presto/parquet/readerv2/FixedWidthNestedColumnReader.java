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

import java.io.IOException;
import java.util.Optional;

public class FixedWidthNestedColumnReader
        extends AbstractNestedColumnReaderV2
{
    public FixedWidthNestedColumnReader(RichColumnDescriptor columnDescriptor)
    {
        super(columnDescriptor);
    }

    @Override
    protected ColumnChunk readNestedWithNull()
            throws IOException
    {
        int maxDL = columnDescriptor.getMaxDefinitionLevel();
        RLDecodingInfo rlDecodingInfo = readRLs(nextBatchSize);

        DLDecodingInfo dlDecodingInfo = readDLs(rlDecodingInfo.getDLValuesDecoderInfos(), rlDecodingInfo.getRLs().length);

        int[] dls = dlDecodingInfo.getDLs();
        int newBatchSize = 0;
        int batchNonNullCount = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : dlDecodingInfo.getValuesDecoderInfos()) {
            int nonNullCount = 0;
            int valueCount = 0;
            for (int i = valuesDecoderInfo.getStart(); i < valuesDecoderInfo.getEnd(); i++) {
                nonNullCount += (dls[i] == maxDL ? 1 : 0);
                valueCount += (dls[i] >= maxDL - 1 ? 1 : 0);
            }
            batchNonNullCount += nonNullCount;
            newBatchSize += valueCount;
            valuesDecoderInfo.setNonNullCount(nonNullCount);
            valuesDecoderInfo.setValueCount(valueCount);
        }

        if (batchNonNullCount == 0) {
            Block block = RunLengthEncodedBlock.create(field.getType(), null, newBatchSize);
            return new ColumnChunk(block, dls, rlDecodingInfo.getRLs());
        }

        TemplateValue[] values = new TemplateValue[newBatchSize];
        boolean[] isNull = new boolean[newBatchSize];

        int offset = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : dlDecodingInfo.getValuesDecoderInfos()) {
            ((TemplateValuesDecoder) valuesDecoderInfo.getValuesDecoder()).readNext(values, offset, valuesDecoderInfo.getNonNullCount());

            int valueDestIdx = offset + valuesDecoderInfo.getValueCount() - 1;
            int valueSrcIdx = offset + valuesDecoderInfo.getNonNullCount() - 1;
            int dlIdx = valuesDecoderInfo.getEnd() - 1;

            while (valueDestIdx >= offset) {
                if (dls[dlIdx] == maxDL) {
                    values[valueDestIdx--] = values[valueSrcIdx--];
                }
                else if (dls[dlIdx] == maxDL - 1) {
                    isNull[valueDestIdx] = true;
                    valueDestIdx--;
                }
                dlIdx--;
            }

            offset += valuesDecoderInfo.getValueCount();
        }

        boolean hasNoNull = batchNonNullCount == newBatchSize;
        Block block = new TemplateArrayBlock(newBatchSize, hasNoNull ? Optional.empty() : Optional.of(isNull), values);
        return new ColumnChunk(block, dls, rlDecodingInfo.getRLs());
    }

    @Override
    protected ColumnChunk readNestedNoNull()
            throws IOException
    {
        int maxDL = columnDescriptor.getMaxDefinitionLevel();
        RLDecodingInfo rlDecodingInfo = readRLs(nextBatchSize);

        DLDecodingInfo dlDecodingInfo = readDLs(rlDecodingInfo.getDLValuesDecoderInfos(), rlDecodingInfo.getRLs().length);

        int[] dls = dlDecodingInfo.getDLs();
        int newBatchSize = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : dlDecodingInfo.getValuesDecoderInfos()) {
            int valueCount = 0;
            for (int i = valuesDecoderInfo.getStart(); i < valuesDecoderInfo.getEnd(); i++) {
                valueCount += (dls[i] == maxDL ? 1 : 0);
            }
            newBatchSize += valueCount;
            valuesDecoderInfo.setNonNullCount(valueCount);
            valuesDecoderInfo.setValueCount(valueCount);
        }

        TemplateValue[] values = new TemplateValue[newBatchSize];

        int offset = 0;

        for (ValuesDecoderInfo valuesDecoderInfo : dlDecodingInfo.getValuesDecoderInfos()) {
            ((TemplateValuesDecoder) valuesDecoderInfo.getValuesDecoder()).readNext(values, offset, valuesDecoderInfo.getNonNullCount());
            offset += valuesDecoderInfo.getValueCount();
        }

        Block block = new TemplateArrayBlock(newBatchSize, Optional.empty(), values);
        return new ColumnChunk(block, dls, rlDecodingInfo.getRLs());
    }

    @Override
    protected void skip(int skipSize)
            throws IOException
    {
        int maxDL = columnDescriptor.getMaxDefinitionLevel();
        RLDecodingInfo rlDecodingInfo = readRLs(skipSize);
        DLDecodingInfo dlDecodingInfo = readDLs(rlDecodingInfo.getDLValuesDecoderInfos(), rlDecodingInfo.getRLs().length);

        int[] dls = dlDecodingInfo.getDLs();
        for (ValuesDecoderInfo valuesDecoderInfo : dlDecodingInfo.getValuesDecoderInfos()) {
            int valueCount = 0;
            for (int i = valuesDecoderInfo.getStart(); i < valuesDecoderInfo.getEnd(); i++) {
                valueCount += (dls[i] == maxDL ? 1 : 0);
            }
            ((TemplateValuesDecoder) valuesDecoderInfo.getValuesDecoder()).skip(valueCount);
        }
    }
}
