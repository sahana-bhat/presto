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
package com.facebook.presto.aresdb;

import com.facebook.presto.aresdb.AresDbQueryGenerator.AresDbQueryGeneratorResult;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AresDbTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final String tableName;
    private final Optional<String> timeColumnName;
    private final Optional<Type> timeStampType;
    private final Optional<Duration> retention;
    private final Optional<Boolean> isQueryShort;
    private final Optional<AresDbQueryGeneratorResult> generatedResult;
    private final AresDbMuttleyConfig muttleyConfig;

    @JsonCreator
    public AresDbTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("timeColumnName") Optional<String> timeColumnName,
            @JsonProperty("timeStampType") Optional<Type> timeStampType,
            @JsonProperty("retention") Optional<Duration> retention,
            Optional<Boolean> isQueryShort,
            Optional<AresDbQueryGeneratorResult> generatedResult,
            @JsonProperty("muttleyConfig") AresDbMuttleyConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.timeColumnName = requireNonNull(timeColumnName, "timeColumnName is null");
        this.timeStampType = requireNonNull(timeStampType, "timeStampType is null");
        this.retention = requireNonNull(retention, "retention is null");
        this.isQueryShort = requireNonNull(isQueryShort, "isQueryShort is null");
        this.generatedResult = requireNonNull(generatedResult, "generatedResult is null");
        this.muttleyConfig = requireNonNull(config, "config is null");
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("tableName", tableName)
                .add("timeColumnName", timeColumnName)
                .add("timeStampType", timeStampType)
                .add("retention", retention)
                .add("isQueryShort", isQueryShort)
                .add("generatedResult", generatedResult)
                .add("muttleyConfig", muttleyConfig)
                .toString();
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Optional<String> getTimeColumnName()
    {
        return timeColumnName;
    }

    @JsonProperty
    public Optional<Duration> getRetention()
    {
        return retention;
    }

    @JsonProperty
    public Optional<Type> getTimeStampType()
    {
        return timeStampType;
    }

    public Optional<Boolean> getIsQueryShort()
    {
        return isQueryShort;
    }

    public Optional<AresDbQueryGeneratorResult> getGeneratedResult()
    {
        return generatedResult;
    }

    @JsonProperty
    public AresDbMuttleyConfig getMuttleyConfig()
    {
        return muttleyConfig;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AresDbTableHandle that = (AresDbTableHandle) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(timeColumnName, that.timeColumnName) &&
                Objects.equals(timeStampType, that.timeStampType) &&
                Objects.equals(retention, that.retention) &&
                Objects.equals(isQueryShort, that.isQueryShort) &&
                Objects.equals(generatedResult, that.generatedResult) &&
                Objects.equals(muttleyConfig, that.muttleyConfig);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, tableName, timeColumnName, timeStampType, retention, isQueryShort, generatedResult, muttleyConfig);
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(connectorId, tableName);
    }
}
