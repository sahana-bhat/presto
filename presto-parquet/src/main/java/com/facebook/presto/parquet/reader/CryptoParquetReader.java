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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.parquet.ParquetDataSource;
import io.airlift.units.DataSize;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.MessageColumnIO;

import java.io.IOException;
import java.util.List;

/**
 * TODO: Merge this class with ParquetReader once decryption code is baked well.
 */
public class CryptoParquetReader
        extends ParquetReader
{
    private final InternalFileDecryptor fileDecryptor;

    public CryptoParquetReader(MessageColumnIO messageColumnIO,
                               List<BlockMetaData> blocks,
                               ParquetDataSource dataSource,
                               AggregatedMemoryContext systemMemoryContext,
                               DataSize maxReadBlockSize,
                               InternalFileDecryptor fileDecryptor)
    {
        super(messageColumnIO, blocks, dataSource, systemMemoryContext, maxReadBlockSize);
        this.fileDecryptor = fileDecryptor;
    }

    @Override
    protected PageReader createPageReader(byte[] buffer, int bufferSize, ColumnChunkMetaData metadata, ColumnDescriptor columnDescriptor)
            throws IOException
    {
        ColumnChunkDescriptor descriptor = new ColumnChunkDescriptor(columnDescriptor, metadata, bufferSize);
        CryptoParquetColumnChunk columnChunk = new CryptoParquetColumnChunk(descriptor, buffer, 0, fileDecryptor);
        if (fileDecryptor == null || fileDecryptor.plaintextFile()) {
            return columnChunk.readAllPages();
        }
        else {
            short columnOrdinal = fileDecryptor.getColumnSetup(descriptor.getColumnChunkMetaData().getPath()).getOrdinal();
            return columnChunk.readAllPages(currentBlockMetadata.getOrdinal(), columnOrdinal);
        }
    }
}
