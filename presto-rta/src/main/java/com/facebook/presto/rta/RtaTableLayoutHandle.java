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
package com.facebook.presto.rta;

import com.facebook.presto.aresdb.AresDbTableHandle;
import com.facebook.presto.aresdb.AresDbTableLayoutHandle;
import com.facebook.presto.pinot.PinotTableHandle;
import com.facebook.presto.pinot.PinotTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class RtaTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final RtaTableHandle table;

    @JsonCreator
    public RtaTableLayoutHandle(
            @JsonProperty("table") RtaTableHandle table)
    {
        this.table = requireNonNull(table, "table is null");
    }

    @JsonProperty
    public RtaTableHandle getTable()
    {
        return table;
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
        RtaTableLayoutHandle that = (RtaTableLayoutHandle) o;
        return Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table);
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();
        result.append(table.toString());
        return result.toString();
    }

    public ConnectorTableLayoutHandle createConnectorSpecificTableLayoutHandle()
    {
        switch (table.getKey().getType()) {
            case PINOT:
                return new PinotTableLayoutHandle((PinotTableHandle) table.getHandle());
            case ARESDB:
                return new AresDbTableLayoutHandle((AresDbTableHandle) table.getHandle());
            default:
                throw new IllegalStateException("Unknown connector type " + table.getKey().getType());
        }
    }
}
