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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PinotPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final String connectorId;
    private final PinotConfig pinotConfig;
    private final PinotScatterGatherQueryClient pinotQueryClient;
    private final PinotClusterInfoFetcher clusterInfoFetcher;
    private final ObjectMapper objectMapper;

    @Inject
    public PinotPageSourceProvider(
            ConnectorId connectorId,
            PinotConfig pinotConfig,
            PinotClusterInfoFetcher clusterInfoFetcher,
            ObjectMapper objectMapper)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pinotConfig = requireNonNull(pinotConfig, "pinotConfig is null");
        this.pinotQueryClient = new PinotScatterGatherQueryClient(new PinotScatterGatherQueryClient.Config(
                pinotConfig.getIdleTimeout().toMillis(),
                pinotConfig.getThreadPoolSize(),
                pinotConfig.getMinConnectionsPerServer(),
                pinotConfig.getMaxBacklogPerServer(),
                pinotConfig.getMaxConnectionsPerServer()));
        this.clusterInfoFetcher = requireNonNull(clusterInfoFetcher, "cluster info fetcher is null");
        this.objectMapper = requireNonNull(objectMapper, "object mapper is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle tableLayoutHandle,
            List<ColumnHandle> columns)
    {
        requireNonNull(split, "split is null");

        PinotSplit pinotSplit = (PinotSplit) split;
        checkArgument(pinotSplit.getTableHandle().getConnectorId().equals(connectorId), "split is not for this connector");

        List<PinotColumnHandle> handles = new ArrayList<>();
        for (ColumnHandle handle : columns) {
            handles.add((PinotColumnHandle) handle);
        }

        switch (pinotSplit.getSplitType()) {
            case SEGMENT:
                return new PinotSegmentPageSource(
                        session,
                        this.pinotConfig,
                        this.pinotQueryClient,
                        pinotSplit,
                        handles);
            case BROKER:
                return new PinotBrokerPageSource(
                        this.pinotConfig,
                        session,
                        pinotSplit.getBrokerPql().get(),
                        handles,
                        pinotSplit.getExpectedColumnHandles(),
                        clusterInfoFetcher,
                        objectMapper,
                        pinotSplit.getTableHandle());
            default:
                throw new UnsupportedOperationException("Unknown Pinot split type: " + pinotSplit.getSplitType());
        }
    }
}
