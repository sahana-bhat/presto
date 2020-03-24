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

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class AresDbConnector
        implements Connector
{
    private static final Logger log = Logger.get(AresDbConnector.class);
    private final LifeCycleManager lifeCycleManager;
    private final AresDbMetadata metadata;
    private final AresDbSplitManager splitManager;
    private final AresDbPageSourceProvider pageSourceProvider;
    private final AresDbNodePartitioningProvider partitioningProvider;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final ConnectorPlanOptimizer planOptimizer;

    @Inject
    public AresDbConnector(LifeCycleManager lifeCycleManager,
                           AresDbMetadata metadata,
                           AresDbSplitManager splitManager,
                           AresDbPageSourceProvider pageSourceProvider,
                           AresDbNodePartitioningProvider partitioningProvider,
                           AresDbSessionProperties sessionProperties,
                           AresDbConnectorPlanOptimizer planOptimizer)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.partitioningProvider = partitioningProvider;
        this.sessionProperties = ImmutableList.copyOf(requireNonNull(sessionProperties, "sessionProperties is null").getSessionProperties());
        this.planOptimizer = requireNonNull(planOptimizer, "plan optimizer is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return AresDbTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return partitioningProvider;
    }

    @Override
    public ConnectorPlanOptimizerProvider getConnectorPlanOptimizerProvider()
    {
        return new ConnectorPlanOptimizerProvider()
        {
            @Override
            public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers()
            {
                return ImmutableSet.of(planOptimizer);
            }

            @Override
            public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers()
            {
                return ImmutableSet.of();
            }
        };
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
