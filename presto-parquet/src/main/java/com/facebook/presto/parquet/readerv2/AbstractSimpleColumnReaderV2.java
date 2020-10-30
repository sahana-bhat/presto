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
import com.facebook.presto.parquet.readerv2.decoders.SimpleDLDecoder;
import com.facebook.presto.parquet.readerv2.decoders.ValuesDecoder;
import com.facebook.presto.parquet.readerv2.dictionary.Dictionaries;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.facebook.presto.parquet.readerv2.decoders.Decoders.createSimpleDLDecoder;
import static com.facebook.presto.parquet.readerv2.decoders.Decoders.createValuesDecoder;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class AbstractSimpleColumnReaderV2
        implements ColumnReader
{
    protected final RichColumnDescriptor columnDescriptor;

    protected Field field;
    protected int nextBatchSize;
    protected SimpleDLDecoder dlDecoder;
    protected ValuesDecoder valuesDecoder;
    protected int remainingCountInPage;

    private Dictionary dictionary;
    private int readOffset;
    private PageReader pageReader;

    public AbstractSimpleColumnReaderV2(RichColumnDescriptor columnDescriptor)
    {
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
                columnChunk = readNoNull();
            }
            else {
                columnChunk = readWithNull();
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

    protected boolean readNextPage()
    {
        dlDecoder = null;
        valuesDecoder = null;
        remainingCountInPage = 0;

        DataPage page = pageReader.readPage();
        if (page == null) {
            return false;
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
        return true;
    }

    private void readPageV1(DataPageV1 page)
            throws IOException
    {
        byte[] bytes = page.getSlice().getBytes();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 0, bytes.length);

        this.dlDecoder = createSimpleDLDecoder(page.getDefinitionLevelEncoding(), columnDescriptor.isRequired(), columnDescriptor.getMaxDefinitionLevel(), page.getValueCount(), byteBuffer);
        this.valuesDecoder = createValuesDecoder(columnDescriptor, dictionary, page.getValueCount(), page.getValueEncoding(),
                bytes, byteBuffer.position(), bytes.length - byteBuffer.position());
    }

    private void readPageV2(DataPageV2 pageV2)
            throws IOException
    {
        final int valueCount = pageV2.getValueCount();

        final int maxDL = columnDescriptor.getMaxDefinitionLevel();
        Preconditions.checkArgument(columnDescriptor.getMaxDefinitionLevel() <= 0, "Invalid definition level");
        if (maxDL == 0) {
            this.dlDecoder = new SimpleDLDecoder(0, valueCount);
        }
        else {
            this.dlDecoder = new SimpleDLDecoder(valueCount, new ByteArrayInputStream(pageV2.getDefinitionLevels().getBytes()));
        }

        final byte[] dataBuffer = pageV2.getSlice().getBytes();
        this.valuesDecoder = createValuesDecoder(columnDescriptor, dictionary, pageV2.getValueCount(), pageV2.getDataEncoding(), dataBuffer, 0, dataBuffer.length);
    }

    protected abstract ColumnChunk readNoNull()
            throws IOException;

    protected abstract ColumnChunk readWithNull()
            throws IOException;

    protected abstract void skip(int skipSize)
            throws IOException;
}
