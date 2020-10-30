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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.CryptoMetadataRetriever;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncDecryptorRetriever;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class ParquetMetaDataUtilsTest
{
    Configuration configuration;
    FileEncDecryptorRetriever fileEncDecryptorRetriever;

    @BeforeTest
    public void setUp() throws IOException
    {
        configuration = new Configuration();
        configuration.setBoolean("InMemoryKMS", true);
        fileEncDecryptorRetriever = new CryptoMetadataRetriever();
    }

    @Test
    public void testGetParquetMetadataEncrypted() throws IOException
    {
        Path path = new Path("../src/test/test-data/test-files/encrypted_footer.parquet");
        FileSystem fileSystem = path.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path);
        long fileSize = fileSystem.getFileStatus(path).getLen();
        FileDecryptionProperties fileDecryptionProperties = fileEncDecryptorRetriever.getFileDecryptionProperties(configuration);
        InternalFileDecryptor fileDecryptor = new InternalFileDecryptor(fileDecryptionProperties);
        ParquetMetadata parquetMetadata = ParquetMetaDataUtils.getParquetMetadata(fileDecryptionProperties,
                fileDecryptor, configuration, path, fileSize, inputStream);
        validate(parquetMetadata);
    }

    @Test
    public void testGetParquetMetadataPlainTextWithSignature() throws IOException
    {
        Path path = new Path("../src/test/test-data/test-files/plaintext_footer_with_signature.parquet");
        FileSystem fileSystem = path.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path);
        long fileSize = fileSystem.getFileStatus(path).getLen();
        FileDecryptionProperties fileDecryptionProperties = fileEncDecryptorRetriever.getFileDecryptionProperties(configuration);
        InternalFileDecryptor fileDecryptor = new InternalFileDecryptor(fileDecryptionProperties);
        ParquetMetadata parquetMetadata = ParquetMetaDataUtils.getParquetMetadata(fileDecryptionProperties, fileDecryptor,
                configuration, path, fileSize, inputStream);
        validate(parquetMetadata);
    }

    @Test
    public void testGetParquetMetadataPlainText() throws IOException
    {
        Path path = new Path("../src/test/test-data/test-files/plaintext_footer.parquet");
        FileSystem fileSystem = path.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path);
        long fileSize = fileSystem.getFileStatus(path).getLen();
        FileDecryptionProperties fileDecryptionProperties = fileEncDecryptorRetriever.getFileDecryptionProperties(configuration);
        InternalFileDecryptor fileDecryptor = new InternalFileDecryptor(fileDecryptionProperties);
        ParquetMetadata parquetMetadata = ParquetMetaDataUtils.getParquetMetadata(fileDecryptionProperties, fileDecryptor,
                configuration, path, fileSize, inputStream);
        validate(parquetMetadata);
    }

    @Test
    public void testRemoveColumnsInSchema()
    {
        MessageType schema = new MessageType("schema",
                new PrimitiveType(REQUIRED, INT64, "DocId"),
                new PrimitiveType(REQUIRED, BINARY, "Name"),
                new PrimitiveType(REQUIRED, BINARY, "Gender"),
                new GroupType(OPTIONAL, "Links",
                        new PrimitiveType(REPEATED, INT64, "Backward"),
                        new PrimitiveType(REPEATED, INT64, "Forward")));
        Set<ColumnPath> paths = new HashSet<>();
        paths.add(ColumnPath.fromDotString("Name"));
        paths.add(ColumnPath.fromDotString("Links.Backward"));

        MessageType newSchema = ParquetMetaDataUtils.removeColumnsInSchema(schema, paths);

        String[] docId = {"DocId"};
        assertTrue(newSchema.containsPath(docId));
        String[] gender = {"Gender"};
        assertTrue(newSchema.containsPath(gender));
        String[] linkForward = {"Links", "Forward"};
        assertTrue(newSchema.containsPath(linkForward));
        String[] name = {"Name"};
        assertFalse(newSchema.containsPath(name));
        String[] linkBackward = {"Links", "Backward"};
        assertFalse(newSchema.containsPath(linkBackward));
    }

    private void validate(ParquetMetadata parquetMetadata)
    {
        assertNotNull(parquetMetadata);
        assertTrue(parquetMetadata.getBlocks().size() > 0);
        List<String[]> paths = parquetMetadata.getFileMetaData().getSchema().getPaths();
        assertTrue(paths.size() == 2);
        assertTrue(paths.get(0).length == 1 && paths.get(0)[0].equals("price"));
        assertTrue(paths.get(1).length == 1 && paths.get(1)[0].equals("product"));
    }
}
