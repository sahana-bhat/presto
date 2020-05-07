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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbColumnHandle.AresDbColumnType.REGULAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AresDbMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(AresDbMetadata.class);

    private final String connectorId;
    private final AresDbConnection aresDbConnection;

    @Inject
    public AresDbMetadata(ConnectorId connectorId, AresDbConnection aresDbConnection)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.aresDbConnection = requireNonNull(aresDbConnection, "aresDbConnection is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(connectorId);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return new AresDbTableHandle(connectorId, tableName.getTableName(), aresDbConnection.getTimeColumn(tableName.getTableName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), new AresDbMuttleyConfig("", ImmutableMap.of()));
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        AresDbTableHandle aresDbTableHandle = (AresDbTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new AresDbTableLayoutHandle(aresDbTableHandle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        AresDbTableHandle aresDbTableHandle = (AresDbTableHandle) table;
        checkArgument(aresDbTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = aresDbTableHandle.toSchemaTableName();
        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return aresDbConnection.getTables().stream()
                .map(t -> schemaTableName(t))
                .collect(Collectors.toList());
    }

    private SchemaTableName schemaTableName(String table)
    {
        return new SchemaTableName(connectorId, table);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        AresDbTableHandle aresDbTableHandle = (AresDbTableHandle) tableHandle;
        checkArgument(aresDbTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        AresDbTable aresDbTable = aresDbConnection.getTable(aresDbTableHandle.getTableName());
        if (aresDbTable == null) {
            throw new TableNotFoundException(aresDbTableHandle.toSchemaTableName());
        }
        ImmutableMap.Builder<String, ColumnHandle> columnHandleMap = ImmutableMap.builder();
        for (ColumnMetadata column : aresDbTable.getColumnsMetadata()) {
            columnHandleMap.put(column.getName().toLowerCase(ENGLISH),
                    new AresDbColumnHandle(((AresDbColumnMetadata) column).getAresDbName(), column.getType(), REGULAR));
        }
        return columnHandleMap.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((AresDbColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> output = ImmutableMap.builder();

        for (String table : aresDbConnection.getTables()) {
            AresDbTable aresDbTable = aresDbConnection.getTable(table);
            output.put(schemaTableName(table), aresDbTable.getColumnsMetadata());
        }

        return output.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        AresDbTable table = aresDbConnection.getTable(tableName.getTableName());
        if (table == null) {
            return null;
        }
        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }
}
