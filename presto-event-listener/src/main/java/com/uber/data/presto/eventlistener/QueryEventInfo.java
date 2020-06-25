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
package com.uber.data.presto.eventlistener;

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import hpw_shaded.com.uber.m3.client.Scope;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class QueryEventInfo
{
    private static final String createEvent = "create";
    private static final String completeEvent = "complete";

    // text is like `"JanusId": "ec84b317-1a53-4cfb-bd3f-3e77df79bd90"`
    private static final Pattern JANUS_ID_PATTERN = Pattern.compile("\"JanusId\": \"(.+?)\"");

    // text is like `"TranslationId": "ec84b317-1a53-4cfb-bd3f-3e77df79bd90"`
    private static final Pattern TRANSLATION_ID_PATTERN = Pattern.compile("\"TranslationId\": \"(.+?)\"");

    // text is like `"Benchmark": "00000001-0000-0000-0000-000000000003"`
    private static final Pattern BENCHMARK_ID_PATTERN = Pattern.compile("\"Benchmark\": \"(.+?)\"");

    private final String queryId;
    private final String state;
    private final String eventType;
    private final String janusId;
    private final String transactionId;
    private final String translationId;
    private final String benchmarkId;
    private final String user;
    private final String engine;  // eg. presto or jarvis
    private final String cluster; // eg. sjc1_secure or dca1_nonsec
    private final String source;
    private final String catalog;
    private final String resourceGroup;
    private final String schema;
    private final String query;
    private final long createTimeMs;
    private final String remoteClientAddress;
    private final String userAgent;
    private final String sessionProperties;
    private final Optional<Completed> completed;

    private class Completed
    {
        private final long scanBlockTimeMs;
        private final long nonScanBlockTimeMs;
        private final int totalTasks;
        private final int totalStages;
        private final long endTimeMs;
        private final long elapsedTimeMs;
        private final long queuedTime;
        private final double memory;
        private final long cpuTimeMs; // aggregate across all tasks
        private final long wallTimeMs; // aggregate across all tasks
        private final long blockedTimeMs; // aggregate across all tasks
        private final long analysisTime;
        private final long totalDrivers;          // aka completedSplits
        private final long peakMemoryReservation; // aka peakMemoryBytes
        private final long rawInputPositions;     // aka totalRows
        private final long rawInputDataSize;      // aka totalBytes
        private final long errorCode;
        private final String errorType;
        private final String errorName;
        private final String failureJson;
        private final List<ColumnAccessEntry> columnAccess;
        private final List<String> operatorSummaries;
        private final List<String> sessionLogEntries;
        private final String memoryPool;

        public Completed(QueryCompletedEvent queryCompletedEvent)
        {
            QueryIOMetadata queryIOMetadata = queryCompletedEvent.getIoMetadata();
            QueryStatistics queryStatistics = queryCompletedEvent.getStatistics();

            this.endTimeMs = queryCompletedEvent.getEndTime().toEpochMilli();
            this.elapsedTimeMs = endTimeMs - createTimeMs;

            // Query Statistics
            this.scanBlockTimeMs = queryStatistics.getScanBlockTime().toMillis();
            this.totalTasks = queryStatistics.getPeakRunningTasks();
            this.totalStages = queryStatistics.getStageGcStatistics().size();
            this.queuedTime = queryStatistics.getQueuedTime().toMillis();
            this.memory = queryStatistics.getCumulativeMemory();
            this.cpuTimeMs = queryStatistics.getCpuTime().toMillis();
            this.wallTimeMs = queryStatistics.getWallTime().toMillis();
            this.blockedTimeMs = queryStatistics.getBlockedTime().toMillis();
            this.analysisTime = queryStatistics.getAnalysisTime().orElse(Duration.ZERO).toMillis();
            this.totalDrivers = queryStatistics.getCompletedSplits();
            this.peakMemoryReservation = queryStatistics.getPeakTotalNonRevocableMemoryBytes();
            this.rawInputPositions = queryStatistics.getTotalRows();
            this.rawInputDataSize = queryStatistics.getTotalBytes();
            this.nonScanBlockTimeMs = Math.max(0, elapsedTimeMs - scanBlockTimeMs);

            // Query IOMetadata
            this.columnAccess = queryIOMetadata.getInputs().stream().map(ColumnAccessEntry::new).collect(toImmutableList());

            this.operatorSummaries = queryStatistics.getOperatorSummaries();

            this.sessionLogEntries = queryStatistics.getSessionLogEntries();
            this.memoryPool = queryStatistics.getMemoryPoolId().isPresent() ? queryStatistics.getMemoryPoolId().get().getId() : "";

            // Failure related
            if (queryCompletedEvent.getFailureInfo().isPresent()) {
                QueryFailureInfo queryFailureInfo = queryCompletedEvent.getFailureInfo().get();

                this.errorCode = queryFailureInfo.getErrorCode().getCode();
                this.errorType = queryFailureInfo.getErrorCode().getType().toString();
                this.errorName = queryFailureInfo.getErrorCode().getName();
                this.failureJson = queryFailureInfo.getFailuresJson();
            }
            else {
                this.errorCode = 0;
                this.errorType = "";
                this.errorName = "";
                this.failureJson = "";
            }
        }

        public void populateMap(Map<String, Object> map)
        {
            map.put("endTime", TimeUnit.SECONDS.convert(this.endTimeMs, TimeUnit.MILLISECONDS));
            map.put("endTimeMs", this.endTimeMs);
            map.put("elapsedTime", TimeUnit.SECONDS.convert(this.elapsedTimeMs, TimeUnit.MILLISECONDS));
            map.put("elapsedTimeMs", this.elapsedTimeMs);
            map.put("queuedTime", this.queuedTime);
            map.put("memory", this.memory);
            map.put("cpuTime", TimeUnit.SECONDS.convert(this.cpuTimeMs, TimeUnit.MILLISECONDS));
            map.put("cpuTimeMs", this.cpuTimeMs);
            map.put("wallTime", TimeUnit.SECONDS.convert(this.wallTimeMs, TimeUnit.MILLISECONDS));
            map.put("wallTimeMs", this.wallTimeMs);
            map.put("blockedTime", TimeUnit.SECONDS.convert(this.blockedTimeMs, TimeUnit.MILLISECONDS));
            map.put("blockedTimeMs", this.blockedTimeMs);
            map.put("analysisTime", this.analysisTime);
            map.put("totalDrivers", this.totalDrivers);
            map.put("peakMemoryReservation", this.peakMemoryReservation);
            map.put("rawInputPositions", this.rawInputPositions);
            map.put("rawInputDataSize", this.rawInputDataSize);
            map.put("columnAccess", this.columnAccess.stream().map(c -> c.toMap()).collect(toImmutableList()));
            map.put("remoteClientAddress", remoteClientAddress);
            map.put("sessionProperties", sessionProperties);
            map.put("errorCode", this.errorCode);
            map.put("failureJson", this.failureJson);
            map.put("totalTasks", this.totalTasks);
            map.put("totalStages", this.totalStages);
            map.put("operatorSummaries", this.operatorSummaries);
            map.put("sessionLogEntries", this.sessionLogEntries);
            map.put("scanBlockTimeMs", this.scanBlockTimeMs);
            map.put("nonScanBlockTimeMs", this.nonScanBlockTimeMs);
        }

        private void populateCommonTagsForM3AndKafka(Map<String, String> tags)
        {
            tags.put("errorType", this.errorType);
            tags.put("errorName", this.errorName);
            tags.put("memorypool", this.memoryPool);
        }

        public void sendToM3(Scope scope)
        {
            Map<String, String> tags = new HashMap<>(getCommonTagsForM3AndKafka());
            tags.put("errorCode", Long.toString(this.errorCode));
            tags.put("tableAccess", columnAccess.stream().map(ColumnAccessEntry::getM3Tag).collect(Collectors.joining(",")));
            publishTimerToM3(scope, "elapsedTime", elapsedTimeMs, tags);
            publishTimerToM3(scope, "queuedTime", queuedTime, tags);
            publishTimerToM3(scope, "cpuTime", cpuTimeMs, tags);
            publishTimerToM3(scope, "wallTime", wallTimeMs, tags);
            publishTimerToM3(scope, "blockedTime", blockedTimeMs, tags);
            publishTimerToM3(scope, "analysisTime", analysisTime, tags);
            publishTimerToM3(scope, "scanBlockTimeMs", scanBlockTimeMs, tags);
            publishTimerToM3(scope, "nonScanBlockTime", nonScanBlockTimeMs, tags);
            scope.gauge("memory", this.memory, tags);
            scope.gauge("totalDrivers", this.totalDrivers, tags);
            scope.gauge("peakMemoryReservation", this.peakMemoryReservation, tags);
            scope.gauge("rawInputPositions", this.rawInputPositions, tags);
            scope.gauge("rawInputDataSize", this.rawInputDataSize, tags);
            scope.gauge("totalTasks", this.totalTasks, tags);
            scope.gauge("totalStages", this.totalStages, tags);
        }
    }

    private class ColumnAccessEntry
    {
        private final String catalog;
        private final String database;
        private final String table;
        private final List<String> columns;
        private final Optional<Object> connectorInfo;
        private final boolean sampleReplaced;

        ColumnAccessEntry(
                String catalog,
                String database,
                String table,
                List<String> columns,
                Optional<Object> connectorInfo,
                boolean sampleReplaced)
        {
            this.catalog = catalog;
            this.database = database;
            this.table = table;
            this.columns = columns;
            this.connectorInfo = connectorInfo;
            this.sampleReplaced = sampleReplaced;
        }

        public ColumnAccessEntry(QueryInputMetadata m)
        {
            this(m.getCatalogName(),
                    m.getSchema(),
                    m.getTable(),
                    m.getColumns(),
                    m.getConnectorInfo(),
                    m.isSampleReplaced());
        }

        public Map<String, Object> toMap()
        {
            List<String> columnNames = new ArrayList<>();
            for (String column : columns) {
                // Change "Column{driver_flow, varchar}" to "driver_flow"
                int start = column.indexOf("{");
                int end = column.indexOf(",");
                if (end == -1) {
                    end = column.length();
                }
                String columnName = column.substring(start + 1, end);

                columnNames.add(columnName);
            }

            Map<String, Object> map = new HashMap<>();
            map.put("catalog", this.catalog);
            map.put("database", this.database);
            map.put("table", this.table);
            map.put("sampleReplaced", this.sampleReplaced);
            map.put("columns", columnNames);
            connectorInfo.ifPresent(info -> map.put("connector_info", info.toString()));
            return map;
        }

        public String getM3Tag()
        {
            if (connectorInfo.isPresent()) {
                return String.format("%s.%s.%s.%s", catalog, database, table, connectorInfo.get().toString());
            }
            else {
                return String.format("%s.%s.%s", catalog, database, table);
            }
        }
    }

    QueryEventInfo(QueryCreatedEvent queryCreatedEvent, String engine, String cluster)
    {
        QueryContext queryContext = queryCreatedEvent.getContext();
        QueryMetadata queryMetadata = queryCreatedEvent.getMetadata();

        this.cluster = cluster;
        this.engine = engine;
        this.eventType = createEvent;

        // Query Medatada
        this.queryId = queryMetadata.getQueryId();
        this.state = queryMetadata.getQueryState();
        this.transactionId = queryMetadata.getTransactionId().orElse("");
        this.query = queryMetadata.getQuery();

        // Query Context
        this.user = queryContext.getUser();
        this.source = queryContext.getSource().orElse("");
        this.catalog = queryContext.getCatalog().orElse("");
        this.resourceGroup = queryContext.getResourceGroupId().isPresent() ? queryContext.getResourceGroupId().get().toString() : "";
        this.schema = queryContext.getSchema().orElse("");
        this.janusId = getJanusId(this.query);
        this.translationId = getTranslationId(this.query);
        this.benchmarkId = getBenchmarkId(this.query);
        this.remoteClientAddress = queryContext.getRemoteClientAddress().orElse("");
        this.userAgent = queryContext.getUserAgent().orElse("");
        this.sessionProperties = getSessionPropertiesString(queryContext.getSessionProperties());

        this.createTimeMs = queryCreatedEvent.getCreateTime().toEpochMilli();
        this.completed = Optional.empty();
    }

    QueryEventInfo(QueryCompletedEvent queryCompletedEvent, String engine, String cluster)
    {
        QueryMetadata queryMetadata = queryCompletedEvent.getMetadata();
        QueryContext queryContext = queryCompletedEvent.getContext();

        this.engine = engine;
        this.cluster = cluster;
        this.eventType = completeEvent;

        // Query Metadata
        this.queryId = queryMetadata.getQueryId();
        this.state = queryMetadata.getQueryState();
        this.transactionId = queryMetadata.getTransactionId().orElse("");
        this.query = queryMetadata.getQuery();

        // Query Context
        this.user = queryContext.getUser();
        this.source = queryContext.getSource().orElse("");
        this.catalog = queryContext.getCatalog().orElse("");
        this.resourceGroup = queryContext.getResourceGroupId().isPresent() ? queryContext.getResourceGroupId().get().toString() : "";
        this.schema = queryContext.getSchema().orElse("");
        this.janusId = getJanusId(this.query);
        this.translationId = getTranslationId(this.query);
        this.benchmarkId = getBenchmarkId(this.query);
        this.remoteClientAddress = queryContext.getRemoteClientAddress().orElse("");
        this.userAgent = queryContext.getUserAgent().orElse("");
        this.sessionProperties = getSessionPropertiesString(queryContext.getSessionProperties());

        this.createTimeMs = queryCompletedEvent.getCreateTime().toEpochMilli();
        this.completed = Optional.of(new Completed(queryCompletedEvent));
    }

    String getQueryId()
    {
        return this.queryId;
    }

    String getState()
    {
        return this.state;
    }

    String getJanusId(String query)
    {
        Matcher matcher = JANUS_ID_PATTERN.matcher(query);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    String getTranslationId(String query)
    {
        Matcher matcher = TRANSLATION_ID_PATTERN.matcher(query);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    String getBenchmarkId(String query)
    {
        Matcher matcher = BENCHMARK_ID_PATTERN.matcher(query);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    String getSessionPropertiesString(Map<String, String> sessionProperties)
    {
        String strSessionProperties = "";
        for (Map.Entry<String, String> entry : sessionProperties.entrySet()) {
            strSessionProperties += ("\"" + entry.getKey() + "\":\"" + entry.getValue() + "\",");
        }
        return strSessionProperties;
    }

    private Map<String, String> getCommonTagsForM3AndKafka()
    {
        Map<String, String> tags = new HashMap<>();
        tags.put("state", this.state);
        tags.put("eventType", this.eventType);
        tags.put("user", this.user);
        tags.put("resourceGroup", this.resourceGroup);
        tags.put("userAgent", this.userAgent);
        completed.ifPresent(c -> c.populateCommonTagsForM3AndKafka(tags));
        return tags;
    }

    private static void publishTimerToM3(Scope scope, String name, long valueInMs, Map<String, String> tags)
    {
        // https://stack.uberinternal.com/questions/17213/does-m3-timer-expect-values-reported-in-nanoseconds
        scope.timer(name, TimeUnit.NANOSECONDS.convert(valueInMs, TimeUnit.MILLISECONDS), tags);
    }

    public void sendToM3(Scope scope)
    {
        completed.ifPresent(c -> c.sendToM3(scope));
    }

    public Map<String, Object> toMap()
    {
        Map<String, Object> map = new HashMap<>(getCommonTagsForM3AndKafka());
        map.put("engine", this.engine);
        map.put("cluster", this.cluster);
        map.put("catalog", this.catalog);
        map.put("source", this.source);
        map.put("queryId", this.queryId);
        map.put("transactionId", this.transactionId);
        map.put("janusId", this.janusId);
        map.put("translationId", this.translationId);
        map.put("benchmarkId", this.benchmarkId);
        map.put("schema", this.schema);
        map.put("query", this.query);
        map.put("createTime", TimeUnit.SECONDS.convert(this.createTimeMs, TimeUnit.MILLISECONDS));
        map.put("createTimeMs", this.createTimeMs);
        completed.ifPresent(c -> c.populateMap(map));

        return map;
    }
}
