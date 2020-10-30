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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.parquet.reader.MetadataReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class ParquetMetaDataUtils
{
    static ParquetMetadataConverter.MetadataFilter filter = ParquetMetadataConverter.NO_FILTER;

    private ParquetMetaDataUtils()
    {
    }

    public static ParquetMetadata getParquetMetadata(FileDecryptionProperties fileDecryptionProperties,
                                                     InternalFileDecryptor fileDecryptor,
                                                     Configuration configuration,
                                                     Path path,
                                                     long fileSize,
                                                     FSDataInputStream fsDataInputStream) throws IOException
    {
        ParquetReadOptions options = createReadOptions(configuration, filter);
        ParquetMetadataConverter converter = new ParquetMetadataConverter(options);

        SeekableInputStream in = createStream(fsDataInputStream);
        return MetadataReader.readFooter(path, fileSize, options, in, converter, fileDecryptionProperties, fileDecryptor);
    }

    public static MessageType removeColumnsInSchema(MessageType schema, Set<ColumnPath> paths)
    {
        List<String> currentPath = new ArrayList<>();
        List<org.apache.parquet.schema.Type> prunedFields = removeColumnsInFields(schema.getFields(), currentPath, paths);
        return new MessageType(schema.getName(), prunedFields);
    }

    private static List<org.apache.parquet.schema.Type> removeColumnsInFields(List<org.apache.parquet.schema.Type> fields,
                                                                              List<String> currentPath, Set<ColumnPath> paths)
    {
        List<org.apache.parquet.schema.Type> prunedFields = new ArrayList<>();
        for (org.apache.parquet.schema.Type childField : fields) {
            org.apache.parquet.schema.Type prunedChildField = removeColumnsInField(childField, currentPath, paths);
            if (prunedChildField != null) {
                prunedFields.add(prunedChildField);
            }
        }
        return prunedFields;
    }

    private static org.apache.parquet.schema.Type removeColumnsInField(org.apache.parquet.schema.Type field,
                                                                       List<String> currentPath, Set<ColumnPath> paths)
    {
        String fieldName = field.getName();
        currentPath.add(fieldName);
        ColumnPath path = ColumnPath.get(currentPath.toArray(new String[0]));
        org.apache.parquet.schema.Type prunedField = null;
        if (!paths.contains(path)) {
            if (field.isPrimitive()) {
                prunedField = field;
            }
            else {
                List<org.apache.parquet.schema.Type> childFields = ((GroupType) field).getFields();
                List<org.apache.parquet.schema.Type> prunedFields = removeColumnsInFields(childFields, currentPath, paths);
                if (prunedFields.size() > 0) {
                    prunedField = ((GroupType) field).withNewFields(prunedFields);
                }
            }
        }

        currentPath.remove(fieldName);
        return prunedField;
    }

    private static ParquetReadOptions createReadOptions(Configuration configuration, ParquetMetadataConverter.MetadataFilter filter)
    {
        if (configuration != null) {
            return HadoopReadOptions.builder(configuration).withMetadataFilter(filter).build();
        }
        else {
            return ParquetReadOptions.builder().withMetadataFilter(filter).build();
        }
    }

    private static SeekableInputStream createStream(FSDataInputStream fsDataInputStream)
    {
        return HadoopStreams.wrap(fsDataInputStream);
    }
}
