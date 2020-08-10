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

import com.facebook.presto.parquet.ParquetEncoding;
import com.facebook.presto.parquet.dictionary.Dictionary;
import com.facebook.presto.parquet.dictionary.IntegerDictionary;
import com.facebook.presto.parquet.dictionary.LongDictionary;
import com.facebook.presto.parquet.readerv2.decoders.plain.BinaryPlainValuesDecoder;
import com.facebook.presto.parquet.readerv2.decoders.plain.BooleanPlainValuesDecoder;
import com.facebook.presto.parquet.readerv2.decoders.plain.Int32PlainValuesDecoder;
import com.facebook.presto.parquet.readerv2.decoders.plain.Int64PlainValuesDecoder;
import com.facebook.presto.parquet.readerv2.decoders.plain.Int64TimestampMicrosPlainValuesDecoder;
import com.facebook.presto.parquet.readerv2.decoders.plain.TimestampPlainValuesDecoder;
import com.facebook.presto.parquet.readerv2.decoders.rle.BinaryRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.readerv2.decoders.rle.BooleanRLEValuesDecoder;
import com.facebook.presto.parquet.readerv2.decoders.rle.Int32RLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.readerv2.decoders.rle.Int64RLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.readerv2.decoders.rle.Int64TimestampMicrosRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.readerv2.decoders.rle.TimestampRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.readerv2.dictionary.BinaryDictionaryV2;
import com.facebook.presto.parquet.readerv2.dictionary.TimestampDictionary;
import com.facebook.presto.spi.PrestoException;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static com.facebook.presto.parquet.ParquetEncoding.PLAIN;
import static com.facebook.presto.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static com.facebook.presto.parquet.ParquetEncoding.RLE;
import static com.facebook.presto.parquet.ParquetEncoding.RLE_DICTIONARY;
import static com.facebook.presto.parquet.ParquetErrorCode.PARQUET_UNSUPPORTED_COLUMN_TYPE;
import static com.facebook.presto.parquet.ParquetErrorCode.PARQUET_UNSUPPORTED_ENCODING;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndianOnOneByte;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class Decoders
{
    private Decoders()
    {
    }

    public static final RLDecoder createRLDecoder(ParquetEncoding encoding, int maxLevelValue, int valueCount, ByteBuffer buffer)
            throws IOException
    {
        final int bitWidth = getWidthFromMaxInt(maxLevelValue);
        if (maxLevelValue == 0 || bitWidth == 0) {
            return new RLDecoder(0, valueCount);
        }

        if (encoding == RLE) {
            ByteBufferInputStream bufferInputStream = ByteBufferInputStream.wrap(buffer);

            final int bufferSize = BytesUtils.readIntLittleEndian(bufferInputStream);
            RLDecoder rlDecoder = new RLDecoder(valueCount, bitWidth, bufferInputStream.sliceStream(bufferSize));

            buffer.position(buffer.position() + bufferSize + 4);
            return rlDecoder;
        }

        throw new PrestoException(PARQUET_UNSUPPORTED_ENCODING, format("RL Encoding: %s", encoding));
    }

    public static final DLDecoder createDLDecoder(ParquetEncoding encoding, int maxLevelValue, int valueCount, ByteBuffer buffer)
            throws IOException
    {
        final int bitWidth = getWidthFromMaxInt(maxLevelValue);
        if (maxLevelValue == 0 || bitWidth == 0) {
            return new DLDecoder(0, valueCount);
        }

        if (encoding == RLE) {
            ByteBufferInputStream bufferInputStream = ByteBufferInputStream.wrap(buffer);

            final int bufferSize = BytesUtils.readIntLittleEndian(bufferInputStream);
            DLDecoder dlDecoder = new DLDecoder(valueCount, bitWidth, bufferInputStream.sliceStream(bufferSize));

            buffer.position(buffer.position() + bufferSize + 4);
            return dlDecoder;
        }

        throw new PrestoException(PARQUET_UNSUPPORTED_ENCODING, format("DL Encoding: %s", encoding));
    }

    public static final ValuesDecoder createValuesDecoder(ColumnDescriptor columnDescriptor, Dictionary dictionary, ParquetEncoding encoding,
            byte[] buffer, int offset, int length)
            throws IOException
    {
        final PrimitiveTypeName type = columnDescriptor.getPrimitiveType().getPrimitiveTypeName();

        if (encoding == PLAIN) {
            switch (type) {
                case BOOLEAN:
                    return new BooleanPlainValuesDecoder(buffer, offset, length);
                case INT32:
                case FLOAT:
                    return new Int32PlainValuesDecoder(buffer, offset, length);
                case INT64:
                    if (OriginalType.TIMESTAMP_MICROS.equals(columnDescriptor.getPrimitiveType().getOriginalType())) {
                        return new Int64TimestampMicrosPlainValuesDecoder(buffer, offset, length);
                    }
                case DOUBLE:
                    return new Int64PlainValuesDecoder(buffer, offset, length);
                case INT96:
                    return new TimestampPlainValuesDecoder(buffer, offset, length);
                case BINARY:
                    return new BinaryPlainValuesDecoder(buffer, offset, length);
                case FIXED_LEN_BYTE_ARRAY:
                default:
                    throw new PrestoException(PARQUET_UNSUPPORTED_COLUMN_TYPE, format("Column: %s, Encoding: %s", columnDescriptor, encoding));
            }
        }

        if (encoding == RLE && type == BOOLEAN) {
            return new BooleanRLEValuesDecoder(ByteBuffer.wrap(buffer, offset, length));
        }

        if (encoding == RLE_DICTIONARY || encoding == PLAIN_DICTIONARY) {
            switch (type) {
                case INT32:
                case FLOAT: {
                    InputStream valuesBufferInputStream = ByteBufferInputStream.wrap(ByteBuffer.wrap(buffer, offset, length));
                    int bitWidth = readIntLittleEndianOnOneByte(valuesBufferInputStream);
                    return new Int32RLEDictionaryValuesDecoder(bitWidth, valuesBufferInputStream, (IntegerDictionary) dictionary);
                }
                case INT64: {
                    if (OriginalType.TIMESTAMP_MICROS.equals(columnDescriptor.getPrimitiveType().getOriginalType())) {
                        InputStream valuesBufferInputStream = ByteBufferInputStream.wrap(ByteBuffer.wrap(buffer, offset, length));
                        int bitWidth = readIntLittleEndianOnOneByte(valuesBufferInputStream);
                        return new Int64TimestampMicrosRLEDictionaryValuesDecoder(bitWidth,
                                valuesBufferInputStream, (LongDictionary) dictionary);
                    }
                }
                case DOUBLE: {
                    InputStream valuesBufferInputStream = ByteBufferInputStream.wrap(ByteBuffer.wrap(buffer, offset, length));
                    int bitWidth = readIntLittleEndianOnOneByte(valuesBufferInputStream);
                    return new Int64RLEDictionaryValuesDecoder(bitWidth, valuesBufferInputStream, (LongDictionary) dictionary);
                }
                case INT96: {
                    InputStream valuesBufferInputStream = ByteBufferInputStream.wrap(ByteBuffer.wrap(buffer, offset, length));
                    int bitWidth = readIntLittleEndianOnOneByte(valuesBufferInputStream);
                    return new TimestampRLEDictionaryValuesDecoder(bitWidth, valuesBufferInputStream, (TimestampDictionary) dictionary);
                }
                case BINARY: {
                    InputStream inputStream = ByteBufferInputStream.wrap(ByteBuffer.wrap(buffer, offset, length));
                    int bitWidth = readIntLittleEndianOnOneByte(inputStream);
                    return new BinaryRLEDictionaryValuesDecoder(bitWidth, inputStream, (BinaryDictionaryV2) dictionary);
                }
                case FIXED_LEN_BYTE_ARRAY:
                default:
                    throw new PrestoException(PARQUET_UNSUPPORTED_COLUMN_TYPE, format("Column: %s, Encoding: %s", columnDescriptor, encoding));
            }
        }

        throw new PrestoException(PARQUET_UNSUPPORTED_ENCODING, format("Column: %s, Encoding: %s", columnDescriptor, encoding));
    }

    public static final SimpleDLDecoder createSimpleDLDecoder(ParquetEncoding encoding, boolean isRequiredType, int maxLevelValue, int valueCount, ByteBuffer buffer)
            throws IOException
    {
        checkArgument(maxLevelValue <= 1, "Can't be used for nested columns");

        if (isRequiredType) {
            return new SimpleDLDecoder(1, valueCount);
        }

        if (maxLevelValue == 0) {
            return new SimpleDLDecoder(0, valueCount);
        }

        final int bitWidth = getWidthFromMaxInt(maxLevelValue);
        if (bitWidth == 0) {
            return new SimpleDLDecoder(0, valueCount);
        }

        if (encoding == RLE) {
            ByteBufferInputStream bufferInputStream = ByteBufferInputStream.wrap(buffer);

            final int bufferSize = BytesUtils.readIntLittleEndian(bufferInputStream);
            SimpleDLDecoder dlDecoder = new SimpleDLDecoder(valueCount, bufferInputStream.sliceStream(bufferSize));

            buffer.position(buffer.position() + bufferSize + 4);
            return dlDecoder;
        }

        throw new PrestoException(PARQUET_UNSUPPORTED_ENCODING, format("DL Encoding: %s", encoding));
    }
}
