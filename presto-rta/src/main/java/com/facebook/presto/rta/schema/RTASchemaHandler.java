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

package com.facebook.presto.rta.schema;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.rta.RtaConfig;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;

/**
 * This class is responsble for top level schema/deployments operations and has a unified view of all info regarding query table, such as
 * - Creating namespace->table->deployments hierarchy
 * - Preloading/making all RTAMS calls
 * - Caching
 */
public class RTASchemaHandler
{
    private static final Logger log = Logger.get(RTASchemaHandler.class);
    private static final Object CACHE_KEY = new Object();
    private final RtaConfig config;
    private final RTAMSClient client;
    private final LoadingCache<Object, State> stateSupplier;
    private final TimeStat stateSupplierTimer = new TimeStat(TimeUnit.MILLISECONDS);

    private static class CaseInsensitiveString
            implements Supplier<String>
    {
        private final String string;
        private final String lowerCaseString;

        public CaseInsensitiveString(String string)
        {
            this.string = string;
            this.lowerCaseString = this.string.toLowerCase(ENGLISH);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            CaseInsensitiveString that = o instanceof CaseInsensitiveString ? (CaseInsensitiveString) o : (o instanceof String) ? new CaseInsensitiveString((String) o) : null;
            return that != null && Objects.equals(lowerCaseString, that.lowerCaseString);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(lowerCaseString);
        }

        @Override
        public String get()
        {
            return string;
        }

        public String getLowerCaseString()
        {
            return lowerCaseString;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("lowerCaseString", lowerCaseString)
                    .toString();
        }
    }

    private static class State
    {
        private Map<String, Map<CaseInsensitiveString, RTATableEntity>> namespaceToTablesMap;

        public State(Map<String, Map<CaseInsensitiveString, RTATableEntity>> namespaceToTablesMap)
        {
            ImmutableMap.Builder<String, Map<CaseInsensitiveString, RTATableEntity>> namespaceToTablesMapBuilder = ImmutableMap.builder();
            namespaceToTablesMap.forEach((namespace, tableMap) -> namespaceToTablesMapBuilder.put(namespace, ImmutableMap.copyOf(tableMap)));
            this.namespaceToTablesMap = namespaceToTablesMapBuilder.build();
        }

        public Map<String, Map<CaseInsensitiveString, RTATableEntity>> getNamespaceToTablesMap()
        {
            return namespaceToTablesMap;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("namespaceToTablesMap", namespaceToTablesMap)
                    .toString();
        }
    }

    private State getState()
    {
        // Return stale if present
        try {
            return stateSupplierTimer.time(() -> {
                State state = stateSupplier.getIfPresent(CACHE_KEY);
                if (state != null) {
                    return state;
                }
                return stateSupplier.getUnchecked(CACHE_KEY);
            });
        }
        catch (Exception e) {
            throw new RuntimeException("Error when fetching state", e);
        }
    }

    private static class TableInNamespaceDetails
    {
        private final RTADefinition definition;
        private List<RTADeployment> deployments;

        public TableInNamespaceDetails(RTADefinition definition, List<RTADeployment> deployments)
        {
            this.definition = definition;
            this.deployments = deployments;
        }
    }

    private State populate()
    {
        State ret;
        try {
            List<RTADefinition> extraDefinitions = client.getExtraDefinitions(config.getExtraDefinitionFiles());
            List<List<RTADeployment>> extraDeployments = client.getExtraDeployments(config.getExtraDeploymentFiles());
            Map<String, Map<CaseInsensitiveString, RTATableEntity>> namespaceToTablesMap = new HashMap<>();
            Set<String> allNamespaces = new HashSet<>(client.getNamespaces().stream().map(s -> s.toLowerCase(ENGLISH)).collect(toImmutableList()));
            Map<SchemaTableName, TableInNamespaceDetails> schemaTableNameMap = new HashMap<>();
            for (String namespace : allNamespaces) {
                for (String table : client.getTables(namespace)) {
                    List<RTADeployment> deployments = client.getDeployments(namespace, table);
                    RTADefinition definition = client.getDefinition(namespace, table);
                    schemaTableNameMap.put(new SchemaTableName(namespace, table), new TableInNamespaceDetails(definition, deployments));
                }
            }

            extraDefinitions.forEach(definition -> {
                schemaTableNameMap.put(new SchemaTableName(definition.getNamespace(), definition.getName()), new TableInNamespaceDetails(definition, null));
            });
            extraDeployments.forEach(deployments -> {
                TableInNamespaceDetails tableInNamespaceDetails = schemaTableNameMap.get(new SchemaTableName(deployments.get(0).getNamespace(), deployments.get(0).getName()));
                if (tableInNamespaceDetails != null) {
                    tableInNamespaceDetails.deployments = deployments;
                }
            });
            schemaTableNameMap.forEach((schemaTableName, detail) -> {
                RTATableEntity entity = new RTATableEntity(schemaTableName.getTableName(), detail.deployments, detail.definition);
                CaseInsensitiveString nonCasedTable = new CaseInsensitiveString(schemaTableName.getTableName());
                namespaceToTablesMap.computeIfAbsent(schemaTableName.getSchemaName(), (ignored) -> new HashMap<>()).put(nonCasedTable, entity);
            });
            allNamespaces.forEach(namespace -> namespaceToTablesMap.putIfAbsent(namespace, ImmutableMap.of()));
            ret = new State(namespaceToTablesMap);
        }
        catch (IOException e) {
            throw new RuntimeException("Error when preloading RTA schema state", e);
        }
        if (log.isDebugEnabled()) {
            log.debug("Created Rta schema state " + ret);
        }
        return ret;
    }

    // Note that this is a counter
    @Managed
    public long getCacheLoadTime()
    {
        return stateSupplier.stats().totalLoadTime();
    }

    // Note that this is a counter
    @Managed
    public long getCacheLoadCount()
    {
        return stateSupplier.stats().loadCount();
    }

    @Managed
    @Nested
    public TimeStat getStateSupplierTimer()
    {
        return stateSupplierTimer;
    }

    @Inject
    public RTASchemaHandler(RTAMSClient client, RtaConfig config, @ForRTAMS Executor executor)
    {
        this.client = client;
        this.config = config;
        this.stateSupplier = CacheBuilder.newBuilder()
                .refreshAfterWrite((long) config.getMetadataCacheExpiryTime().getValue(TimeUnit.SECONDS), TimeUnit.SECONDS)
                .recordStats()
                .build(asyncReloading(CacheLoader.from(this::populate), executor));
        executor.execute(() -> {
            this.stateSupplier.refresh(CACHE_KEY);
        });
    }

    public List<String> getAllNamespaces()
    {
        return ImmutableList.copyOf(getState().getNamespaceToTablesMap().keySet());
    }

    public List<String> getTablesInNamespace(String namespace)
    {
        Map<CaseInsensitiveString, RTATableEntity> tablesInNamespace = getState().getNamespaceToTablesMap().get(namespace);
        if (tablesInNamespace == null) {
            return ImmutableList.of();
        }
        else {
            return tablesInNamespace.keySet().stream().map(CaseInsensitiveString::getLowerCaseString).collect(toImmutableList());
        }
    }

    public Optional<RTATableEntity> getEntity(SchemaTableName schemaTableName)
    {
        return getEntity(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    public Optional<RTATableEntity> getEntity(String namespace, String table)
    {
        Map<CaseInsensitiveString, RTATableEntity> tablesInNamespace = getState().getNamespaceToTablesMap().get(namespace);
        if (tablesInNamespace == null) {
            throw new SchemaNotFoundException(namespace);
        }
        return Optional.ofNullable(tablesInNamespace.get(new CaseInsensitiveString(table)));
    }
}
