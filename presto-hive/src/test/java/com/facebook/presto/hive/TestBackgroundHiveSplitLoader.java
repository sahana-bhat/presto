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
package com.facebook.presto.hive;

import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.hive.HiveBucketing.HiveBucketFilter;
import com.facebook.presto.hive.HiveColumnHandle.ColumnType;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.hive.BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static com.facebook.presto.hive.HiveFileInfo.createHiveFileInfo;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveUtil.getRegularColumnHandles;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.util.HiveFileIterator.NestedDirectoryPolicy;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestBackgroundHiveSplitLoader
{
    private static final int BUCKET_COUNT = 2;

    private static final String DEFAULT_TABLE_LOCATION = "hdfs://VOL1:9000/db_name/table_name";
    private static final String SAMPLE_PATH = DEFAULT_TABLE_LOCATION + "/000000_0";
    private static final String SAMPLE_PATH_FILTERED = DEFAULT_TABLE_LOCATION + "/000000_1";
    private static final String DEFAULT_TABLE_NAME = "test_table";
    private static final String SAMPLE_LARGE_FILE_PATH = DEFAULT_TABLE_LOCATION + "/largefile";

    private static final Path RETURNED_PATH = new Path(SAMPLE_PATH);
    private static final Path FILTERED_PATH = new Path(SAMPLE_PATH_FILTERED);
    private static final Path LARGE_FILE_PATH = new Path(SAMPLE_LARGE_FILE_PATH);

    private static final Executor EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-%s"));

    private static final Domain RETURNED_PATH_DOMAIN = Domain.singleValue(VARCHAR, utf8Slice(RETURNED_PATH.toString()));

    private static final List<HiveFileInfo> TEST_FILES = ImmutableList.of(
            createHiveFileInfo(locatedFileStatus(RETURNED_PATH), Optional.empty()),
            createHiveFileInfo(locatedFileStatus(FILTERED_PATH), Optional.empty()));

    private static final List<HiveFileInfo> TEST_LARGE_FILE_LIST = ImmutableList.of(
            createHiveFileInfo(locatedFileStatus(LARGE_FILE_PATH, new DataSize(70, MEGABYTE).toBytes()), Optional.empty()));

    private static final List<Column> PARTITION_COLUMNS = ImmutableList.of(
            new Column("partitionColumn", HIVE_INT, Optional.empty()));
    private static final List<HiveColumnHandle> BUCKET_COLUMN_HANDLES = ImmutableList.of(
            new HiveColumnHandle("col1", HIVE_INT, INTEGER.getTypeSignature(), 0, ColumnType.REGULAR, Optional.empty(), Optional.empty()));

    private static final Optional<HiveBucketProperty> BUCKET_PROPERTY = Optional.of(
            new HiveBucketProperty(ImmutableList.of("col1"), BUCKET_COUNT, ImmutableList.of()));

    private static final StorageFormat DEFAULT_STORAGE_FORMAT = StorageFormat.create(
            "com.facebook.hive.orc.OrcSerde",
            "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
            "org.apache.hadoop.hive.ql.io.RCFileInputFormat");

    private static final Table SIMPLE_TABLE = table(ImmutableList.of(), Optional.empty(), DEFAULT_STORAGE_FORMAT, DEFAULT_TABLE_LOCATION, DEFAULT_TABLE_NAME);
    private static final Table PARTITIONED_TABLE = table(PARTITION_COLUMNS, BUCKET_PROPERTY, DEFAULT_STORAGE_FORMAT, DEFAULT_TABLE_LOCATION, DEFAULT_TABLE_NAME);
    private static final String RAW_TRIPS_TABLE_NAME = "raw_trips";
    private static final String HOODIE_INPUT_FORMAT_CANONICAL_NAME = HoodieParquetInputFormat.class.getCanonicalName();
    private static final StorageFormat HOODIE_STORAGE_FORMAT = StorageFormat.create(
            "any String.",
            HOODIE_INPUT_FORMAT_CANONICAL_NAME,
            HOODIE_INPUT_FORMAT_CANONICAL_NAME);

    @Test
    public void testNoPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertEquals(drain(hiveSplitSource).size(), 2);
    }

    @Test
    public void testPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                Optional.of(RETURNED_PATH_DOMAIN));

        HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testPathFilterOneBucketMatchPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                Optional.of(RETURNED_PATH_DOMAIN),
                Optional.of(new HiveBucketFilter(ImmutableSet.of(0, 1))),
                PARTITIONED_TABLE,
                Optional.of(new HiveBucketHandle(BUCKET_COLUMN_HANDLES, BUCKET_COUNT, BUCKET_COUNT)));

        HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testPathFilterBucketedPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                Optional.of(RETURNED_PATH_DOMAIN),
                Optional.empty(),
                PARTITIONED_TABLE,
                Optional.of(
                        new HiveBucketHandle(
                                getRegularColumnHandles(PARTITIONED_TABLE),
                                BUCKET_COUNT,
                                BUCKET_COUNT)));

        HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testMultiSplitsInSingleFile()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_LARGE_FILE_LIST,
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<HiveSplit> splits = drainSplits(hiveSplitSource);
        assertEquals(splits.size(), 2);
        assertEquals(splits.get(0).getStart(), 0);
        assertEquals(splits.get(0).getLength(), 33554432);
        assertEquals(splits.get(0).getFileSize(), 73400320);
        assertEquals(splits.get(1).getStart(), 33554432);
        assertEquals(splits.get(1).getLength(), 39845888);
        assertEquals(splits.get(1).getFileSize(), 73400320);
    }

    @Test
    public void testEmptyFileWithNoBlocks()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                ImmutableList.of(createHiveFileInfo(locatedFileStatusWithNoBlocks(RETURNED_PATH), Optional.empty())),
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        List<HiveSplit> splits = drainSplits(hiveSplitSource);
        assertEquals(splits.size(), 1);
        assertEquals(splits.get(0).getPath(), RETURNED_PATH.toString());
        assertEquals(splits.get(0).getLength(), 0);
    }

    @Test
    public void testNoHangIfPartitionIsOffline()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoaderOfflinePartitions();
        HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThrows(RuntimeException.class, () -> drain(hiveSplitSource));
        assertThrows(RuntimeException.class, () -> hiveSplitSource.isFinished());
    }

    @Test
    public void testHoodieFileNotFoundException()
    {
        Table hoodieTable = table(PARTITION_COLUMNS, Optional.empty(), HOODIE_STORAGE_FORMAT, "/random/path/that/doesn't/exist", RAW_TRIPS_TABLE_NAME);
        try {
            backgroundHiveSplitLoader(ImmutableList.of(), Optional.empty(), Optional.empty(), hoodieTable, Optional.empty());
            fail("shouldn't have come here");
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), HIVE_FILE_NOT_FOUND.toErrorCode());
            assertEquals(ex.getMessage(), "Hoodie table not found in path /random/path/that/doesn't/exist");
        }
    }

    @Test
    public void testLoadSplitsForHoodieTable() throws Exception
    {
        testLoadSplitsForHoodieTables(false, false);
    }

    @Test
    public void testLoadSplitsForMixedHoodieTable() throws Exception
    {
        testLoadSplitsForHoodieTables(true, false);
    }

    @Test
    public void testLoadSplitsForHoodieTableWithGloballyConsistentTimeStamp() throws Exception
    {
        testLoadSplitsForHoodieTables(false, true);
    }

    @Test
    public void testLoadSplitsForMixedHoodieTableWithGloballyConsistentTimeStamp() throws Exception
    {
        testLoadSplitsForHoodieTables(true, true);
    }

    private void testLoadSplitsForHoodieTables(boolean isMixed, boolean checkWithGloballyReplicatedTs) throws Exception
    {
        //prepare Hoodie DataSet
        ConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(new HiveClientConfig().setHoodieGloballyConsistentReadEnabled(true),
                    new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        int numFiles = 19;
        String commitnumber = "123";
        String newCommitNumber = "124";
        int numNonHoodieFiles = 5;
        File hoodieTableBasePath = Files.createTempDir();
        Table hoodieTable = table(PARTITION_COLUMNS, Optional.empty(), HOODIE_STORAGE_FORMAT,
                hoodieTableBasePath.toString(), RAW_TRIPS_TABLE_NAME);
        Map<Boolean, List<HiveFileInfo>> partitionPathFiles = prepareHoodieDataset(hoodieTableBasePath, numFiles, commitnumber, isMixed, isMixed ? numNonHoodieFiles : 0, true);
        // create the commit file in Hoodie Metadata
        new File(hoodieTableBasePath.toString() + "/.hoodie/", commitnumber + ".commit").createNewFile();
        List<HiveFileInfo> partitionFiles = partitionPathFiles.values().stream().flatMap(List::stream).collect(
                Collectors.toList());
        List<HiveFileInfo> nonHoodiePartitionFiles = partitionPathFiles.get(Boolean.FALSE);
        List<HiveSplit> hiveSplits = getHiveSplitsForHoodieTable(hoodieTable, partitionFiles, session);

        verifySplitForHoodieTable(hiveSplits, partitionFiles, numFiles);

        // add one more commit which add new files to the same partitions
        Map<Boolean, List<HiveFileInfo>> partitionPathToNewFiles =
                prepareHoodieDataset(hoodieTableBasePath, numFiles, newCommitNumber, isMixed,
                    isMixed ? numNonHoodieFiles : 0, false);
        List<HiveFileInfo> newPartitionFiles = partitionPathToNewFiles.values().stream()
                .flatMap(List::stream).collect(Collectors.toList());

        List<HiveFileInfo> allPartitionFiles = new ArrayList<>(partitionFiles);
        allPartitionFiles.addAll(partitionPathToNewFiles.get(Boolean.TRUE));

        // because commit is not yet created these should not be visible
        hiveSplits = getHiveSplitsForHoodieTable(hoodieTable, allPartitionFiles, session);

        verifySplitForHoodieTable(hiveSplits, partitionFiles, numFiles);

        if (checkWithGloballyReplicatedTs) {
            verifySplitForHoodieTableWithGloballyReplicatedTs(commitnumber, partitionFiles,
                    partitionFiles, nonHoodiePartitionFiles, allPartitionFiles,
                    numFiles, isMixed, numNonHoodieFiles, hoodieTable, session);
        }

        // now create new commit and this should make the new files visible
        new File(hoodieTableBasePath.toString() + "/.hoodie/",
            newCommitNumber + ".commit").createNewFile();

        hiveSplits = getHiveSplitsForHoodieTable(hoodieTable, allPartitionFiles, session);

        verifySplitForHoodieTable(hiveSplits, newPartitionFiles, numFiles);

        if (checkWithGloballyReplicatedTs) {
            // try with ol commit
            verifySplitForHoodieTableWithGloballyReplicatedTs(commitnumber, partitionFiles,
                    newPartitionFiles, nonHoodiePartitionFiles, allPartitionFiles,
                    numFiles, isMixed, numNonHoodieFiles, hoodieTable, session);

            // try with new commit
            verifySplitForHoodieTableWithGloballyReplicatedTs(newCommitNumber, newPartitionFiles,
                    newPartitionFiles, nonHoodiePartitionFiles, allPartitionFiles,
                    numFiles, isMixed, numNonHoodieFiles, hoodieTable, session);
        }

        // set a session level property to disable globally consistent reads.
        // for a table which does not have this property at all it should not have any effect
        hiveSplits = getHiveSplitsForHoodieTable(hoodieTable, allPartitionFiles, SESSION);

        verifySplitForHoodieTable(hiveSplits, newPartitionFiles, numFiles);

        if (checkWithGloballyReplicatedTs) {
            // because we disabled the feature with hive session property setting it to 0 should not do anything
            hoodieTable = Table.builder(hoodieTable).setParameters(
                    ImmutableMap.of(HiveTableProperties.GLOBALLY_CONSISTENT_READ_TIMESTAMP, "0")).build();

            hiveSplits = getHiveSplitsForHoodieTable(hoodieTable, allPartitionFiles, SESSION);

            verifySplitForHoodieTable(hiveSplits, newPartitionFiles, numFiles);
        }
    }

    private List<HiveSplit> getHiveSplitsForHoodieTable(Table hoodieTable,
            List<HiveFileInfo> partitionFiles, ConnectorSession session) throws Exception
    {
        BackgroundHiveSplitLoader splitLoader = backgroundHiveSplitLoader(partitionFiles,
                Optional.empty(),
                Optional.empty(),
                hoodieTable,
                Optional.empty(),
                session);
        HiveSplitSource hiveSplitSource = hiveSplitSource(hoodieTable, splitLoader);
        splitLoader.start(hiveSplitSource);
        return drainSplits(hiveSplitSource);
    }

    private void verifySplitForHoodieTableWithGloballyReplicatedTs(String commitNumber,
            List<HiveFileInfo> partitionFilesInCommit, List<HiveFileInfo> latestPartitionFiles,
            List<HiveFileInfo> numNonHoodiePartitionFiles, List<HiveFileInfo> allPartitionFiles,
            int numFiles, boolean isMixed, int numNonHoodieFiles, Table hoodieTable, ConnectorSession session) throws Exception
    {
        List<HiveSplit> hiveSplits = Collections.emptyList();

        // by setting commit to commitNumber only files created in that commit show up
        hoodieTable = Table.builder(hoodieTable).setParameters(
                ImmutableMap.of(HiveTableProperties.GLOBALLY_CONSISTENT_READ_TIMESTAMP, commitNumber)).build();
        hiveSplits = getHiveSplitsForHoodieTable(hoodieTable, allPartitionFiles, session);
        verifySplitForHoodieTable(hiveSplits, partitionFilesInCommit, numFiles);

        // by setting the globally replicated timestamp to 0 hoodie files should get filtered out
        hoodieTable = Table.builder(hoodieTable).setParameters(
                ImmutableMap.of(HiveTableProperties.GLOBALLY_CONSISTENT_READ_TIMESTAMP, "0")).build();
        hiveSplits = getHiveSplitsForHoodieTable(hoodieTable, allPartitionFiles, session);
        verifySplitForHoodieTable(hiveSplits, numNonHoodiePartitionFiles, isMixed ? numNonHoodieFiles : 0);

        // reset the property and this will only get files from commitNumber
        hoodieTable = Table.builder(hoodieTable).setParameters(
                ImmutableMap.of(HiveTableProperties.GLOBALLY_CONSISTENT_READ_TIMESTAMP, commitNumber)).build();
        hiveSplits = getHiveSplitsForHoodieTable(hoodieTable, allPartitionFiles, session);
        verifySplitForHoodieTable(hiveSplits, partitionFilesInCommit, numFiles);

        // remove the property and get latest commit's files
        hoodieTable = Table.builder(hoodieTable).setParameters(ImmutableMap.of()).build();
        hiveSplits = getHiveSplitsForHoodieTable(hoodieTable, allPartitionFiles, session);
        verifySplitForHoodieTable(hiveSplits, latestPartitionFiles, numFiles);
    }

    private void verifySplitForHoodieTable(List<HiveSplit> hiveSplits, List<HiveFileInfo> partitionFiles, int numFiles)
    {
        assertEquals(hiveSplits.size(), numFiles);

        // Check split locations match the datafiles created
        Set<String> hiveSplitpaths = new HashSet<>();
        hiveSplits.stream().map(s -> s.getPath()).forEach(hiveSplitpaths::add);
        for (HiveFileInfo hiveFileInfo : partitionFiles) {
            assertTrue(hiveSplitpaths.contains(hiveFileInfo.getPath().toString()), "Encountered unknown split!");
        }
    }

    private static List<String> drain(HiveSplitSource source)
            throws Exception
    {
        return drainSplits(source).stream()
                .map(HiveSplit::getPath)
                .collect(toImmutableList());
    }

    private static List<HiveSplit> drainSplits(HiveSplitSource source)
            throws Exception
    {
        ImmutableList.Builder<HiveSplit> splits = ImmutableList.builder();
        while (!source.isFinished()) {
            source.getNextBatch(NOT_PARTITIONED, 100).get()
                    .getSplits().stream()
                    .map(HiveSplit.class::cast)
                    .forEach(splits::add);
        }
        return splits.build();
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<HiveFileInfo> files,
            Optional<Domain> pathDomain)
    {
        return backgroundHiveSplitLoader(
                files,
                pathDomain,
                Optional.empty(),
                SIMPLE_TABLE,
                Optional.empty());
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<HiveFileInfo> files,
            Optional<Domain> pathDomain,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle,
            ConnectorSession session)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas =
                ImmutableList.of(
                        new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of()));

        return new BackgroundHiveSplitLoader(
                table,
                hivePartitionMetadatas,
                pathDomain,
                createBucketSplitInfo(bucketHandle, hiveBucketFilter),
                session,
                new TestingHdfsEnvironment(),
                new NamenodeStats(),
                new TestingDirectoryLister(files),
                EXECUTOR,
                2,
                false,
                false,
                false);
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<HiveFileInfo> files,
            Optional<Domain> pathDomain,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle)
    {
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(new HiveClientConfig().setMaxSplitSize(new DataSize(1.0, GIGABYTE)), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

        return backgroundHiveSplitLoader(files, pathDomain, hiveBucketFilter, table, bucketHandle, connectorSession);
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoaderOfflinePartitions()
    {
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(new HiveClientConfig().setMaxSplitSize(new DataSize(1.0, GIGABYTE)), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                createPartitionMetadataWithOfflinePartitions(),
                Optional.empty(),
                createBucketSplitInfo(Optional.empty(), Optional.empty()),
                connectorSession,
                new TestingHdfsEnvironment(),
                new NamenodeStats(),
                new TestingDirectoryLister(TEST_FILES),
                directExecutor(),
                2,
                false,
                false,
                false);
    }

    private static Iterable<HivePartitionMetadata> createPartitionMetadataWithOfflinePartitions()
            throws RuntimeException
    {
        return () -> new AbstractIterator<HivePartitionMetadata>()
        {
            // This iterator is crafted to return a valid partition for the first calls to
            // hasNext() and next(), and then it should throw for the second call to hasNext()
            private int position = -1;

            @Override
            protected HivePartitionMetadata computeNext()
            {
                position++;
                switch (position) {
                    case 0:
                        return new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of());
                    case 1:
                        throw new RuntimeException("OFFLINE");
                    default:
                        return endOfData();
                }
            }
        };
    }

    private static HiveSplitSource hiveSplitSource(Table table, BackgroundHiveSplitLoader backgroundHiveSplitLoader)
    {
        return HiveSplitSource.allAtOnce(
                SESSION,
                table.getDatabaseName(),
                table.getTableName(),
                1,
                1,
                new DataSize(32, MEGABYTE),
                backgroundHiveSplitLoader,
                EXECUTOR,
                new CounterStat());
    }

    private static Table table(
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            StorageFormat storageFormat,
            String location,
            String tableName)
    {
        Table.Builder tableBuilder = Table.builder();
        tableBuilder.getStorageBuilder()
                .setStorageFormat(storageFormat)
                .setLocation(location)
                .setSkewed(false)
                .setBucketProperty(bucketProperty);

        return tableBuilder
                .setDatabaseName("test_dbname")
                .setOwner("testOwner")
                .setTableName(tableName)
                .setTableType(MANAGED_TABLE)
                .setDataColumns(ImmutableList.of(new Column("col1", HIVE_STRING, Optional.empty())))
                .setParameters(ImmutableMap.of())
                .setPartitionColumns(partitionColumns)
                .build();
    }

    private static Map<Boolean, List<HiveFileInfo>> prepareHoodieDataset(File basePath, int numberOfFiles,
                                                                         String commitNumber, boolean isMixedTable, int numNonHoodieFiles,
                                                                         boolean initialize) throws IOException
    {
        if (initialize) {
            HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
            Properties properties = new Properties();
            properties.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, RAW_TRIPS_TABLE_NAME);
            properties.setProperty(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, tableType.name());
            properties.setProperty(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP_NAME, HoodieAvroPayload.class.getName());
            HoodieTableMetaClient.initTableAndGetMetaClient(new Configuration(), basePath.toString(), properties);
        }
        File partitionPath = new File(basePath, "2016/05/01");
        partitionPath.mkdirs();
        List<HiveFileInfo> filesInPartition = new ArrayList<>();
        for (int i = 0; i < numberOfFiles - numNonHoodieFiles; i++) {
            File dataFile = new File(partitionPath,
                    FSUtils.makeDataFileName(commitNumber, "1", "fileid" + i));
            dataFile.createNewFile();
            filesInPartition.add(createHiveFileInfo(locatedFileStatus(new Path(dataFile.getAbsolutePath())), Optional.empty()));
        }
        Map<Boolean, List<HiveFileInfo>> result = new HashMap<>();
        result.put(true, filesInPartition);

        if (isMixedTable) {
            File partitionPath2 = new File(basePath, "2016/04/30");
            partitionPath2.mkdirs();
            List<HiveFileInfo> filesInPartition2 = new ArrayList<>();
            for (int i = 0; i < numNonHoodieFiles; i++) {
                File dataFile = new File(partitionPath2, "000000_" + i);
                dataFile.createNewFile();
                filesInPartition2.add(createHiveFileInfo(locatedFileStatus(new Path(dataFile.getAbsolutePath())), Optional.empty()));
            }
            result.put(false, filesInPartition2);
        }
        else {
            result.put(false, new ArrayList<>());
        }

        return result;
    }

    private static LocatedFileStatus locatedFileStatus(Path path)
    {
        return new LocatedFileStatus(
                0L,
                false,
                0,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {new BlockLocation()});
    }

    private static LocatedFileStatus locatedFileStatusWithNoBlocks(Path path)
    {
        return new LocatedFileStatus(
                0L,
                false,
                0,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {});
    }

    private static LocatedFileStatus locatedFileStatus(Path path, long fileSize)
    {
        return new LocatedFileStatus(
                fileSize,
                false,
                0,
                fileSize,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {new BlockLocation(null, null, 0, fileSize)});
    }

    private static class TestingDirectoryLister
            implements DirectoryLister
    {
        private final List<HiveFileInfo> files;

        public TestingDirectoryLister(List<HiveFileInfo> files)
        {
            this.files = files;
        }

        @Override
        public Iterator<HiveFileInfo> list(FileSystem fs, Path path, NamenodeStats namenodeStats, NestedDirectoryPolicy nestedDirectoryPolicy, PathFilter pathFilter)
        {
            return files.iterator();
        }
    }

    public static class TestingHdfsEnvironment
            extends HdfsEnvironment
    {
        public TestingHdfsEnvironment()
        {
            super(
                    new HiveHdfsConfiguration(new HdfsConfigurationInitializer(new HiveClientConfig(), new MetastoreClientConfig()), ImmutableSet.of()),
                    new MetastoreClientConfig(),
                    new NoHdfsAuthentication());
        }

        @Override
        public FileSystem getFileSystem(String user, Path path, Configuration configuration)
        {
            return new TestingHdfsFileSystem();
        }
    }

    private static class TestingHdfsFileSystem
            extends FileSystem
    {
        @Override
        public boolean delete(Path f, boolean recursive)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rename(Path src, Path dst)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWorkingDirectory(Path dir)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream create(
                Path f,
                FsPermission permission,
                boolean overwrite,
                int bufferSize,
                short replication,
                long blockSize,
                Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mkdirs(Path f, FsPermission permission)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Path getWorkingDirectory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI getUri()
        {
            throw new UnsupportedOperationException();
        }
    }
}
