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

import com.facebook.presto.parquet.ColumnReader;
import com.facebook.presto.parquet.DataPage;
import com.facebook.presto.parquet.DataPageV1;
import com.facebook.presto.parquet.DataPageV2;
import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.dictionary.Dictionary;
import com.facebook.presto.parquet.reader.ColumnChunk;
import com.facebook.presto.parquet.reader.PageReader;
import com.facebook.presto.parquet.readerv2.decoders.DLDecoder;
import com.facebook.presto.parquet.readerv2.decoders.RLDecoder;
import com.facebook.presto.parquet.readerv2.decoders.ValuesDecoder;
import com.facebook.presto.parquet.readerv2.dictionary.Dictionaries;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.facebook.presto.parquet.readerv2.decoders.Decoders.createDLDecoder;
import static com.facebook.presto.parquet.readerv2.decoders.Decoders.createRLDecoder;
import static com.facebook.presto.parquet.readerv2.decoders.Decoders.createValuesDecoder;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;

public abstract class AbstractNestedColumnReaderV2
        implements ColumnReader
{
    protected final RichColumnDescriptor columnDescriptor;

    protected Field field;
    protected int nextBatchSize;

    private Dictionary dictionary;
    private int readOffset;
    private RLDecoder rlDecoder;
    private DLDecoder dlDecoder;
    private ValuesDecoder valuesDecoder;
    private int remainingCountInPage;
    private PageReader pageReader;
    private int lastRL = -1;

    public AbstractNestedColumnReaderV2(RichColumnDescriptor columnDescriptor)
    {
        checkArgument(columnDescriptor.getPath().length > 1, "expected to read a nested column");
        this.columnDescriptor = requireNonNull(columnDescriptor, "columnDescriptor is null");
    }

    @Override
    public boolean inited()
    {
        return pageReader != null && field != null;
    }

    @Override
    public void init(PageReader pageReader, Field field)
    {
        Preconditions.checkState(!inited(), "already initialized");
        this.pageReader = requireNonNull(pageReader, "pageReader is null");
        checkArgument(pageReader.getTotalValueCount() > 0, "page is empty");

        this.field = requireNonNull(field, "field is null");

        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
        if (dictionaryPage != null) {
            dictionary = Dictionaries.createDictionary(columnDescriptor, dictionaryPage);
        }
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset = readOffset + nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public ColumnChunk readNext()
    {
        ColumnChunk columnChunk = null;
        try {
            seek();

            if (field.isRequired()) {
                columnChunk = readNestedNoNull();
            }
            else {
                columnChunk = readNestedWithNull();
            }
        }
        catch (IOException ex) {
            throw new ParquetDecodingException("Failed to decode.", ex);
        }

        readOffset = 0;
        nextBatchSize = 0;

        return columnChunk;
    }

    private void seek()
            throws IOException
    {
        if (readOffset == 0) {
            return;
        }

        skip(readOffset);
    }

    protected void readNextPage()
    {
        rlDecoder = null;
        dlDecoder = null;
        valuesDecoder = null;
        remainingCountInPage = 0;

        DataPage page = pageReader.readPage();
        if (page == null) {
            return;
        }

        try {
            if (page instanceof DataPageV1) {
                readPageV1((DataPageV1) page);
            }
            else {
                readPageV2((DataPageV2) page);
            }
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page " + page + " in column " + columnDescriptor, e);
        }

        remainingCountInPage = page.getValueCount();
    }

    private void readPageV1(DataPageV1 page)
            throws IOException
    {
        byte[] bytes = page.getSlice().getBytes();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 0, bytes.length);

        this.rlDecoder = createRLDecoder(page.getRepetitionLevelEncoding(), columnDescriptor.getMaxRepetitionLevel(), page.getValueCount(), byteBuffer);
        this.dlDecoder = createDLDecoder(page.getDefinitionLevelEncoding(), columnDescriptor.getMaxDefinitionLevel(), page.getValueCount(), byteBuffer);
        this.valuesDecoder = createValuesDecoder(columnDescriptor, dictionary, page.getValueCount(), page.getValueEncoding(),
                bytes, byteBuffer.position(), bytes.length - byteBuffer.position());
    }

    private void readPageV2(DataPageV2 pageV2)
            throws IOException
    {
        final int valueCount = pageV2.getValueCount();

        final int maxRL = columnDescriptor.getMaxRepetitionLevel();
        final int rlBitWidth = getWidthFromMaxInt(maxRL);
        if (maxRL == 0 || rlBitWidth == 0) {
            this.rlDecoder = new RLDecoder(0, valueCount);
        }
        else {
            this.rlDecoder = new RLDecoder(valueCount, rlBitWidth, new ByteArrayInputStream(pageV2.getRepetitionLevels().getBytes()));
        }

        final int maxDL = columnDescriptor.getMaxDefinitionLevel();
        final int dlBitWidth = getWidthFromMaxInt(maxDL);
        if (maxDL == 0 || dlBitWidth == 0) {
            this.dlDecoder = new DLDecoder(0, valueCount);
        }
        else {
            this.dlDecoder = new DLDecoder(valueCount, dlBitWidth, new ByteArrayInputStream(pageV2.getDefinitionLevels().getBytes()));
        }

        final byte[] dataBuffer = pageV2.getSlice().getBytes();
        this.valuesDecoder = createValuesDecoder(columnDescriptor, dictionary, pageV2.getValueCount(), pageV2.getDataEncoding(), dataBuffer, 0, dataBuffer.length);
    }

    protected abstract ColumnChunk readNestedNoNull()
            throws IOException;

    protected abstract ColumnChunk readNestedWithNull()
            throws IOException;

    protected abstract void skip(int skipSize)
            throws IOException;

    protected final RLDecodingInfo readRLs(int batchSize)
            throws IOException
    {
        IntList rlsList = new IntArrayList(batchSize);

        int remainingInBatch = batchSize + 1;

        RLDecodingInfo rlDecodingInfo = new RLDecodingInfo();

        if (remainingCountInPage == 0) {
            readNextPage();
        }

        int startOffset = 0;

        if (lastRL != -1) {
            rlsList.add(lastRL);
            lastRL = -1;
            remainingInBatch--;
        }

        while (remainingInBatch > 0) {
            int read = rlDecoder.readNext(rlsList, remainingInBatch);
            if (read == 0) {
                int endOffset = rlsList.size();
                rlDecodingInfo.add(new DLValuesDecoderInfo(dlDecoder, valuesDecoder, startOffset, endOffset));
                remainingCountInPage -= (endOffset - startOffset);
                startOffset = endOffset;
                readNextPage();
                if (remainingCountInPage == 0) {
                    break;
                }
            }
            else {
                remainingInBatch -= read;
            }
        }

        if (remainingInBatch == 0) {
            lastRL = 0;
            rlsList.remove(rlsList.size() - 1);
        }

        if (rlDecoder != null) {
            rlDecodingInfo.add(new DLValuesDecoderInfo(dlDecoder, valuesDecoder, startOffset, rlsList.size()));
        }

        rlDecodingInfo.setRLs(rlsList.toIntArray());

        return rlDecodingInfo;
    }

    protected final DLDecodingInfo readDLs(List<DLValuesDecoderInfo> decoderInfos, int batchSize)
            throws IOException
    {
        DLDecodingInfo dlDecodingInfo = new DLDecodingInfo();

        int[] dls = new int[batchSize];

        int remainingInBatch = batchSize;
        for (DLValuesDecoderInfo decoderInfo : decoderInfos) {
            int readChunkSize = decoderInfo.getEnd() - decoderInfo.getStart();
            decoderInfo.getDlDecoder().readNext(dls, decoderInfo.getStart(), readChunkSize);
            dlDecodingInfo.add(new ValuesDecoderInfo(decoderInfo.getValuesDecoder(), decoderInfo.getStart(), decoderInfo.getEnd()));
            remainingInBatch -= readChunkSize;
        }

        if (remainingInBatch != 0) {
            throw new IllegalStateException("We didn't read correct number of DLs");
        }

        dlDecodingInfo.setDLs(dls);

        return dlDecodingInfo;
    }
}
