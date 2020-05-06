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

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.Constants.DATA_ARCHIVED_ERROR_MESSAGE;
import static com.facebook.presto.hive.HiveErrorCode.Constants.DEFAULT_ERROR_MESSAGE;
import static com.facebook.presto.hive.HiveErrorCode.Constants.UACCESS_ERROR_MESSAGE;
import static com.facebook.presto.hive.MetastoreErrorCode.ERROR_CODE_MASK;
import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum HiveErrorCode
        implements ErrorCodeSupplier
{
    // HIVE_METASTORE_ERROR(0) moved to MetastoreErrorCode
    HIVE_CURSOR_ERROR(1, EXTERNAL),
    // HIVE_TABLE_OFFLINE(2) moved to MetastoreErrorCode
    HIVE_CANNOT_OPEN_SPLIT(3, EXTERNAL),
    HIVE_FILE_NOT_FOUND(4, EXTERNAL),
    HIVE_UNKNOWN_ERROR(5, EXTERNAL),
    // HIVE_PARTITION_OFFLINE(6) moved to MetastoreErrorCode
    HIVE_BAD_DATA(7, EXTERNAL),
    HIVE_PARTITION_SCHEMA_MISMATCH(8, EXTERNAL),
    HIVE_MISSING_DATA(9, EXTERNAL),
    // HIVE_INVALID_PARTITION_VALUE(10, EXTERNAL) moved to MetastoreErrorCode
    HIVE_TIMEZONE_MISMATCH(11, EXTERNAL),
    // HIVE_INVALID_METADATA(12) moved to MetastoreErrorCode
    HIVE_INVALID_VIEW_DATA(13, EXTERNAL),
    HIVE_DATABASE_LOCATION_ERROR(14, EXTERNAL),
    // HIVE_PATH_ALREADY_EXISTS(15, EXTERNAL) moved to MetastoreErrorCode
    // HIVE_FILESYSTEM_ERROR(16) moved to MetastoreErrorCode
    // code HIVE_WRITER_ERROR(17) is deprecated
    HIVE_SERDE_NOT_FOUND(18, EXTERNAL),
    // HIVE_UNSUPPORTED_FORMAT(19) moved to MetastoreErrorCode
    HIVE_PARTITION_READ_ONLY(20, USER_ERROR),
    HIVE_TOO_MANY_OPEN_PARTITIONS(21, USER_ERROR),
    HIVE_CONCURRENT_MODIFICATION_DETECTED(22, EXTERNAL),
    HIVE_COLUMN_ORDER_MISMATCH(23, USER_ERROR),
    HIVE_FILE_MISSING_COLUMN_NAMES(24, EXTERNAL),
    HIVE_WRITER_OPEN_ERROR(25, EXTERNAL),
    HIVE_WRITER_CLOSE_ERROR(26, EXTERNAL),
    HIVE_WRITER_DATA_ERROR(27, EXTERNAL),
    HIVE_INVALID_BUCKET_FILES(28, EXTERNAL, DEFAULT_ERROR_MESSAGE),
    HIVE_EXCEEDED_PARTITION_LIMIT(29, USER_ERROR),
    HIVE_WRITE_VALIDATION_FAILED(30, INTERNAL_ERROR),
    // HIVE_PARTITION_DROPPED_DURING_QUERY(31, EXTERNAL) moved to MetastoreErrorCode
    HIVE_TABLE_READ_ONLY(32, USER_ERROR),
    HIVE_PARTITION_NOT_READABLE(33, USER_ERROR),
    HIVE_TABLE_NOT_READABLE(34, USER_ERROR),
    // HIVE_TABLE_DROPPED_DURING_QUERY(35, EXTERNAL) moded to MetastoreErrorCode
    // HIVE_TOO_MANY_BUCKET_SORT_FILES(36) is deprecated
    // HIVE_CORRUPTED_COLUMN_STATISTICS(37, EXTERNAL) moved to MetastoreErrorCode
    HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT(38, USER_ERROR),
    // HIVE_UNKNOWN_COLUMN_STATISTIC_TYPE(39, INTERNAL_ERROR) moved to MetastoreErrorCode
    HIVE_TABLE_BUCKETING_IS_IGNORED(40, USER_ERROR),
    HIVE_TRANSACTION_NOT_FOUND(41, INTERNAL_ERROR),
    HIVE_PERMISSION_ERROR(90, EXTERNAL, UACCESS_ERROR_MESSAGE),
    HIVE_DATA_ARCHIVED_ERROR(91, EXTERNAL, DATA_ARCHIVED_ERROR_MESSAGE),
    /**/;

    private final ErrorCode errorCode;
    private final Optional<String> guidance;

    HiveErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + ERROR_CODE_MASK, name(), type);
        guidance = Optional.empty();
    }

    HiveErrorCode(int code, ErrorType type, String guidance)
    {
        errorCode = new ErrorCode(code + ERROR_CODE_MASK, name(), type);
        this.guidance = Optional.of(guidance);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }

    @Override
    public Optional<String> getGuidance()
    {
        return guidance;
    }

    public class Constants
    {
        public static final String DATA_ARCHIVED_PREFIX = "_DATA_ARCHIVED_CONTACT_hdfs-dev-group@uber.com";

        static final String DATA_ARCHIVED_ERROR_MESSAGE = "Please check http://t.uber.com/data-archived for more information.";
        static final String DEFAULT_ERROR_MESSAGE = "Please find table owner through https://databook.uberinternal.com/ and report the issue.";
        static final String UACCESS_ERROR_MESSAGE = "Please follow the FAQ http://t.uber.com/uaccess_getting_started to get access to this dataset. "
                + "If unresolved, please ask in https://uchat.uberinternal.com/uber/channels/data-security-community.";

        private Constants() {}
    }
}
