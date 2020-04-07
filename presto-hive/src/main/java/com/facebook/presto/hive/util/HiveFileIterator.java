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
package com.facebook.presto.hive.util;

import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.hive.HiveFileInfo;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;

import static com.facebook.presto.hive.HiveErrorCode.Constants.DATA_ARCHIVED_PREFIX;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_DATA_ARCHIVED_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PERMISSION_ERROR;
import static com.facebook.presto.hive.MetastoreErrorCode.HIVE_FILESYSTEM_ERROR;
import static java.util.Objects.requireNonNull;

public class HiveFileIterator
        extends AbstractIterator<HiveFileInfo>
{
    public enum NestedDirectoryPolicy
    {
        IGNORED,
        RECURSE,
        FAIL
    }

    private final Deque<Path> paths = new ArrayDeque<>();
    private final ListDirectoryOperation listDirectoryOperation;
    private final NamenodeStats namenodeStats;
    private final NestedDirectoryPolicy nestedDirectoryPolicy;
    private final PathFilter pathFilter;

    private Iterator<HiveFileInfo> remoteIterator = Collections.emptyIterator();

    public HiveFileIterator(
            Path path,
            ListDirectoryOperation listDirectoryOperation,
            NamenodeStats namenodeStats,
            NestedDirectoryPolicy nestedDirectoryPolicy,
            PathFilter pathFilter)
    {
        paths.addLast(requireNonNull(path, "path is null"));
        this.listDirectoryOperation = requireNonNull(listDirectoryOperation, "listDirectoryOperation is null");
        this.namenodeStats = requireNonNull(namenodeStats, "namenodeStats is null");
        this.nestedDirectoryPolicy = requireNonNull(nestedDirectoryPolicy, "nestedDirectoryPolicy is null");
        this.pathFilter = requireNonNull(pathFilter, "pathFilter is null");
    }

    @Override
    protected HiveFileInfo computeNext()
    {
        while (true) {
            while (remoteIterator.hasNext()) {
                HiveFileInfo fileInfo = getLocatedFileStatus(remoteIterator);

                // Ignore hidden files and directories. Hive ignores files starting with _ and . as well.
                String fileName = fileInfo.getPath().getName();
                if (fileName.startsWith("_") || fileName.startsWith(".")) {
                    continue;
                }

                if (fileInfo.isDirectory()) {
                    switch (nestedDirectoryPolicy) {
                        case IGNORED:
                            continue;
                        case RECURSE:
                            paths.add(fileInfo.getPath());
                            continue;
                        case FAIL:
                            throw new NestedDirectoryNotAllowedException();
                    }
                }

                return fileInfo;
            }

            if (paths.isEmpty()) {
                return endOfData();
            }
            remoteIterator = getLocatedFileStatusRemoteIterator(paths.removeFirst(), pathFilter);
        }
    }

    private Iterator<HiveFileInfo> getLocatedFileStatusRemoteIterator(Path path, PathFilter pathFilter)
    {
        try (TimeStat.BlockTimer ignored = namenodeStats.getListLocatedStatus().time()) {
            return Iterators.filter(new FileStatusIterator(path, listDirectoryOperation, namenodeStats), input -> pathFilter.accept(input.getPath()));
        }
    }

    private HiveFileInfo getLocatedFileStatus(Iterator<HiveFileInfo> iterator)
    {
        try (TimeStat.BlockTimer ignored = namenodeStats.getRemoteIteratorNext().time()) {
            return iterator.next();
        }
    }

    private static class FileStatusIterator
            implements Iterator<HiveFileInfo>
    {
        private final Path path;
        private final NamenodeStats namenodeStats;
        private final RemoteIterator<HiveFileInfo> fileStatusIterator;

        private FileStatusIterator(Path path, ListDirectoryOperation listDirectoryOperation, NamenodeStats namenodeStats)
        {
            this.path = path;
            this.namenodeStats = namenodeStats;
            try {
                this.fileStatusIterator = listDirectoryOperation.list(path);
            }
            catch (IOException e) {
                throw processException(e);
            }
        }

        @Override
        public boolean hasNext()
        {
            try {
                return fileStatusIterator.hasNext();
            }
            catch (IOException e) {
                throw processException(e);
            }
        }

        @Override
        public HiveFileInfo next()
        {
            try {
                return fileStatusIterator.next();
            }
            catch (IOException e) {
                throw processException(e);
            }
        }

        private PrestoException processException(IOException exception)
        {
            namenodeStats.getRemoteIteratorNext().recordException(exception);
            if (exception instanceof FileNotFoundException) {
                if (exception.getMessage().contains(DATA_ARCHIVED_PREFIX)) {
                    return new PrestoException(HIVE_DATA_ARCHIVED_ERROR, "The part of the data you are looking for is archived for cost efficiency reasons", exception);
                }

                return new PrestoException(HIVE_FILE_NOT_FOUND, "Partition location does not exist: " + path);
            }
            else if (exception instanceof AccessControlException) {
                throw new PrestoException(HIVE_PERMISSION_ERROR, "Seems like a security problem, " +
                        "please try to follow the FAQ http://t.uber.com/uaccess_getting_started to resolve the issue first. " +
                        "If it persists, please ask in https://uchat.uberinternal.com/uber/channels/data-security-community." + exception.getMessage());
            }

            return new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed to list directory: " + path +
                    ". If you believe you have permissions check if the directory corresponds to a recent partition which could be in " +
                    "the process of data ingestion at this moment. Please retry your query after sometime. " + exception.getMessage(), exception);
        }
    }

    public static class NestedDirectoryNotAllowedException
            extends RuntimeException
    {
        public NestedDirectoryNotAllowedException()
        {
            super("Nested sub-directories are not allowed");
        }
    }

    public interface ListDirectoryOperation
    {
        RemoteIterator<HiveFileInfo> list(Path path)
                throws IOException;
    }
}
