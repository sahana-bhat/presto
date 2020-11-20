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
package com.facebook.presto.parquet;

import com.facebook.presto.parquet.reader.AbstractColumnReaderV1;
import com.facebook.presto.parquet.reader.BinaryColumnReader;
import com.facebook.presto.parquet.reader.BooleanColumnReader;
import com.facebook.presto.parquet.reader.DoubleColumnReader;
import com.facebook.presto.parquet.reader.FloatColumnReader;
import com.facebook.presto.parquet.reader.IntColumnReader;
import com.facebook.presto.parquet.reader.LongColumnReader;
import com.facebook.presto.parquet.reader.LongDecimalColumnReader;
import com.facebook.presto.parquet.reader.ShortDecimalColumnReader;
import com.facebook.presto.parquet.reader.TimestampColumnReader;
import com.facebook.presto.parquet.reader.TimestampMicrosColumnReader;
import com.facebook.presto.parquet.readerv2.BinaryNestedColumnReaderV2;
import com.facebook.presto.parquet.readerv2.BinarySimpleColumnReaderV2;
import com.facebook.presto.parquet.readerv2.Int32ColumnReader;
import com.facebook.presto.parquet.readerv2.Int64ColumnReader;
import com.facebook.presto.parquet.readerv2.Int64TimestampMicrosColumnReader;
import com.facebook.presto.parquet.readerv2.NestedBooleanColumnReader;
import com.facebook.presto.parquet.readerv2.NestedInt32ColumnReader;
import com.facebook.presto.parquet.readerv2.NestedInt64ColumnReader;
import com.facebook.presto.parquet.readerv2.NestedInt64TimestampMicrosColumnReader;
import com.facebook.presto.parquet.readerv2.NestedTimestampColumnReader;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import org.apache.parquet.schema.OriginalType;

import java.util.Optional;

import static com.facebook.presto.parquet.ParquetTypeUtils.createDecimalType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.apache.parquet.schema.OriginalType.DECIMAL;

public class ColumnReaderFactory
{
    private ColumnReaderFactory()
    {
    }

    public static ColumnReader createReader(RichColumnDescriptor descriptor, boolean optimizedReaderEnabled)
    {
        // decimal is not supported in optimized reader
        if (optimizedReaderEnabled && descriptor.getPrimitiveType().getOriginalType() != DECIMAL) {
            final boolean isNested = descriptor.getPath().length > 1;
            switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
                case BOOLEAN:
                    return isNested ? new NestedBooleanColumnReader(descriptor) : new com.facebook.presto.parquet.readerv2.BooleanColumnReader(descriptor);
                case INT32:
                case FLOAT:
                    return isNested ? new NestedInt32ColumnReader(descriptor) : new Int32ColumnReader(descriptor);
                case INT64:
                    if (OriginalType.TIMESTAMP_MICROS.equals(descriptor.getPrimitiveType().getOriginalType())) {
                        return isNested ? new NestedInt64TimestampMicrosColumnReader(descriptor) : new Int64TimestampMicrosColumnReader(descriptor);
                    }
                case DOUBLE:
                    return isNested ? new NestedInt64ColumnReader(descriptor) : new Int64ColumnReader(descriptor);
                case INT96:
                    return isNested ? new NestedTimestampColumnReader(descriptor) : new com.facebook.presto.parquet.readerv2.TimestampColumnReader(descriptor);
                case BINARY:
                    return isNested ? new BinaryNestedColumnReaderV2(descriptor) : new BinarySimpleColumnReaderV2(descriptor);
            }
        }

        switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
            case BOOLEAN:
                return new BooleanColumnReader(descriptor);
            case INT32:
                return createDecimalColumnReader(descriptor).orElse(new IntColumnReader(descriptor));
            case INT64:
                if (OriginalType.TIMESTAMP_MICROS.equals(descriptor.getPrimitiveType().getOriginalType())) {
                    return new TimestampMicrosColumnReader(descriptor);
                }
                return createDecimalColumnReader(descriptor).orElse(new LongColumnReader(descriptor));
            case INT96:
                return new TimestampColumnReader(descriptor);
            case FLOAT:
                return new FloatColumnReader(descriptor);
            case DOUBLE:
                return new DoubleColumnReader(descriptor);
            case BINARY:
                return createDecimalColumnReader(descriptor).orElse(new BinaryColumnReader(descriptor));
            case FIXED_LEN_BYTE_ARRAY:
                return createDecimalColumnReader(descriptor)
                        .orElseThrow(() -> new PrestoException(NOT_SUPPORTED, " type FIXED_LEN_BYTE_ARRAY supported as DECIMAL; got " + descriptor.getPrimitiveType().getOriginalType()));
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported parquet type: " + descriptor.getType());
        }
    }

    private static Optional<AbstractColumnReaderV1> createDecimalColumnReader(RichColumnDescriptor descriptor)
    {
        Optional<Type> type = createDecimalType(descriptor);
        if (type.isPresent()) {
            DecimalType decimalType = (DecimalType) type.get();
            if (decimalType.isShort()) {
                return Optional.of(new ShortDecimalColumnReader(descriptor));
            }
            else {
                return Optional.of(new LongDecimalColumnReader(descriptor));
            }
        }
        return Optional.empty();
    }
}
