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
package com.facebook.presto.event;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.airlift.stats.Distribution;
import com.facebook.airlift.stats.Distribution.DistributionSnapshot;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.Column;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.Input;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.TableFinishInfo;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryOutputMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.facebook.presto.spi.eventlistener.ResourceDistribution;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.SessionLogger;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.transaction.TransactionId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import org.joda.time.DateTime;

import javax.inject.Inject;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textDistributedPlan;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static java.util.Objects.requireNonNull;

public class QueryMonitor
{
    private static final Logger log = Logger.get(QueryMonitor.class);
    private static final JsonCodec<Map<PlanFragmentId, JsonPlanFragment>> PLAN_MAP_CODEC = JsonCodec.mapJsonCodec(PlanFragmentId.class, JsonPlanFragment.class);

    private final JsonCodec<StageInfo> stageInfoCodec;
    private final JsonCodec<OperatorStats> operatorStatsCodec;
    private final JsonCodec<SessionLogger.Entry> sessionLogEntryCodec;
    private final JsonCodec<ExecutionFailureInfo> executionFailureInfoCodec;
    private final EventListenerManager eventListenerManager;
    private final String serverVersion;
    private final String serverAddress;
    private final String environment;
    private final SessionPropertyManager sessionPropertyManager;
    private final FunctionManager functionManager;
    private final int maxJsonLimit;

    @Inject
    public QueryMonitor(
            JsonCodec<StageInfo> stageInfoCodec,
            JsonCodec<OperatorStats> operatorStatsCodec,
            JsonCodec<SessionLogger.Entry> sessionLogEntryCodec,
            JsonCodec<ExecutionFailureInfo> executionFailureInfoCodec,
            EventListenerManager eventListenerManager,
            NodeInfo nodeInfo,
            NodeVersion nodeVersion,
            SessionPropertyManager sessionPropertyManager,
            Metadata metadata,
            QueryMonitorConfig config)
    {
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.stageInfoCodec = requireNonNull(stageInfoCodec, "stageInfoCodec is null");
        this.operatorStatsCodec = requireNonNull(operatorStatsCodec, "operatorStatsCodec is null");
        this.sessionLogEntryCodec = requireNonNull(sessionLogEntryCodec, "session log entry codec is null");
        this.executionFailureInfoCodec = requireNonNull(executionFailureInfoCodec, "executionFailureInfoCodec is null");
        this.serverVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        this.serverAddress = requireNonNull(nodeInfo, "nodeInfo is null").getExternalAddress();
        this.environment = requireNonNull(nodeInfo, "nodeInfo is null").getEnvironment();
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.functionManager = requireNonNull(metadata, "metadata is null").getFunctionManager();
        this.maxJsonLimit = toIntExact(requireNonNull(config, "config is null").getMaxOutputStageJsonSize().toBytes());
    }

    public void queryCreatedEvent(BasicQueryInfo queryInfo)
    {
        eventListenerManager.queryCreated(
                new QueryCreatedEvent(
                        queryInfo.getQueryStats().getCreateTime().toDate().toInstant(),
                        createQueryContext(queryInfo.getSession(), queryInfo.getResourceGroupId()),
                        new QueryMetadata(
                                queryInfo.getQueryId().toString(),
                                queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                                queryInfo.getQuery(),
                                QUEUED.toString(),
                                queryInfo.getSelf(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty())));
    }

    public void queryImmediateFailureEvent(BasicQueryInfo queryInfo, ExecutionFailureInfo failure)
    {
        eventListenerManager.queryCompleted(new QueryCompletedEvent(
                new QueryMetadata(
                        queryInfo.getQueryId().toString(),
                        queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                        queryInfo.getQuery(),
                        queryInfo.getState().toString(),
                        queryInfo.getSelf(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                new QueryStatistics(
                        0,
                        ofMillis(0),
                        ofMillis(0),
                        ofMillis(0),
                        ofMillis(0),
                        Optional.empty(),
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        ImmutableList.of(),
                        0,
                        true,
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        Optional.empty(),
                        ImmutableList.of()),
                createQueryContext(queryInfo.getSession(), queryInfo.getResourceGroupId()),
                new QueryIOMetadata(ImmutableList.of(), Optional.empty()),
                createQueryFailureInfo(failure, Optional.empty()),
                ImmutableList.of(),
                queryInfo.getQueryType(),
                ImmutableList.of(),
                ofEpochMilli(queryInfo.getQueryStats().getCreateTime().getMillis()),
                ofEpochMilli(queryInfo.getQueryStats().getCreateTime().getMillis()),
                ofEpochMilli(queryInfo.getQueryStats().getEndTime().getMillis())));

        logQueryTimeline(queryInfo);
    }

    public void queryCompletedEvent(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();
        eventListenerManager.queryCompleted(
                new QueryCompletedEvent(
                        createQueryMetadata(queryInfo),
                        createQueryStatistics(queryInfo),
                        createQueryContext(queryInfo.getSession(), queryInfo.getResourceGroupId()),
                        getQueryIOMetadata(queryInfo),
                        createQueryFailureInfo(queryInfo.getFailureInfo(), queryInfo.getOutputStage()),
                        queryInfo.getWarnings(),
                        queryInfo.getQueryType(),
                        queryInfo.getFailedTasks().orElse(ImmutableList.of()).stream()
                                .map(TaskId::toString)
                                .collect(toImmutableList()),
                        ofEpochMilli(queryStats.getCreateTime().getMillis()),
                        ofEpochMilli(queryStats.getExecutionStartTime().getMillis()),
                        ofEpochMilli(queryStats.getEndTime() != null ? queryStats.getEndTime().getMillis() : 0)));

        logQueryTimeline(queryInfo);
    }

    public static ResourceDistribution createResourceDistribution(
            int stageId,
            int tasks,
            DistributionSnapshot distributionSnapshot)
    {
        return new ResourceDistribution(
                stageId,
                tasks,
                distributionSnapshot.getP25(),
                distributionSnapshot.getP50(),
                distributionSnapshot.getP75(),
                distributionSnapshot.getP90(),
                distributionSnapshot.getP95(),
                distributionSnapshot.getP99(),
                distributionSnapshot.getMin(),
                distributionSnapshot.getMax(),
                (long) distributionSnapshot.getTotal(),
                distributionSnapshot.getTotal() / distributionSnapshot.getCount());
    }

    private QueryMetadata createQueryMetadata(QueryInfo queryInfo)
    {
        return new QueryMetadata(
                queryInfo.getQueryId().toString(),
                queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                queryInfo.getQuery(),
                queryInfo.getState().toString(),
                queryInfo.getSelf(),
                createTextQueryPlan(queryInfo),
                createJsonQueryPlan(queryInfo),
                queryInfo.getOutputStage().flatMap(stage -> stageInfoCodec.toJsonWithLengthLimit(stage, maxJsonLimit)));
    }

    private QueryStatistics createQueryStatistics(QueryInfo queryInfo)
    {
        ImmutableList.Builder<String> operatorSummaries = ImmutableList.builder();
        for (OperatorStats summary : queryInfo.getQueryStats().getOperatorSummaries()) {
            operatorSummaries.add(operatorStatsCodec.toJson(summary));
        }
        ImmutableList.Builder<String> sessionLogEntries = ImmutableList.builder();
        if (queryInfo.getSessionLogEntries().isPresent()) {
            queryInfo.getSessionLogEntries().get().forEach(entry -> sessionLogEntries.add(sessionLogEntryCodec.toJson(entry)));
        }

        QueryStats queryStats = queryInfo.getQueryStats();

        ImmutableList.Builder<ResourceDistribution> cpuDistributionBuilder = ImmutableList.builder();
        ImmutableList.Builder<ResourceDistribution> memoryDistributionBuilder = ImmutableList.builder();
        if (queryInfo.getOutputStage().isPresent()) {
            computeCpuAndMemoryDistributions(queryInfo.getOutputStage().get(), cpuDistributionBuilder, memoryDistributionBuilder);
        }

        return new QueryStatistics(
                queryStats.getTotalTasks(),
                ofMillis(queryStats.getTotalCpuTime().toMillis()),
                ofMillis(queryStats.getTotalScheduledTime().toMillis()),
                ofMillis(queryStats.getQueuedTime().toMillis()),
                ofMillis(queryStats.getTotalBlockedTime().toMillis()),
                Optional.of(ofMillis(queryStats.getAnalysisTime().toMillis())),
                queryStats.getPeakRunningTasks(),
                queryStats.getPeakUserMemoryReservation().toBytes(),
                queryStats.getPeakTotalMemoryReservation().toBytes(),
                queryStats.getPeakTaskUserMemory().toBytes(),
                queryStats.getPeakTaskTotalMemory().toBytes(),
                queryStats.getRawInputDataSize().toBytes(),
                queryStats.getRawInputPositions(),
                queryStats.getOutputDataSize().toBytes(),
                queryStats.getOutputPositions(),
                queryStats.getWrittenOutputLogicalDataSize().toBytes(),
                queryStats.getWrittenOutputPositions(),
                queryStats.getWrittenIntermediatePhysicalDataSize().toBytes(),
                queryStats.getCumulativeUserMemory(),
                queryStats.getStageGcStatistics(),
                queryStats.getCompletedDrivers(),
                queryInfo.isCompleteInfo(),
                cpuDistributionBuilder.build(),
                memoryDistributionBuilder.build(),
                operatorSummaries.build(),
                Optional.of(queryInfo.getMemoryPool()),
                sessionLogEntries.build());
    }

    private QueryContext createQueryContext(SessionRepresentation session, Optional<ResourceGroupId> resourceGroup)
    {
        return new QueryContext(
                session.getUser(),
                session.getPrincipal(),
                session.getRemoteUserAddress(),
                session.getUserAgent(),
                session.getClientInfo(),
                session.getClientTags(),
                session.getSource(),
                session.getCatalog(),
                session.getSchema(),
                resourceGroup,
                mergeSessionAndCatalogProperties(session),
                session.getResourceEstimates(),
                serverAddress,
                serverVersion,
                environment);
    }

    private Optional<String> createTextQueryPlan(QueryInfo queryInfo)
    {
        try {
            if (queryInfo.getOutputStage().isPresent()) {
                return Optional.of(textDistributedPlan(
                        queryInfo.getOutputStage().get(),
                        functionManager,
                        queryInfo.getSession().toSession(sessionPropertyManager),
                        false));
            }
        }
        catch (Exception e) {
            // Sometimes it is expected to fail. For example if generated plan is too long.
            // Don't fail to create event if the plan can not be created.
            log.warn(e, "Error creating explain plan for query %s", queryInfo.getQueryId());
        }
        return Optional.empty();
    }

    private Optional<String> createJsonQueryPlan(QueryInfo queryInfo)
    {
        try {
            if (queryInfo.getOutputStage().isPresent()) {
                ImmutableSortedMap.Builder<PlanFragmentId, JsonPlanFragment> fragmentJsonMap = ImmutableSortedMap.naturalOrder();
                for (StageInfo stage : getAllStages(queryInfo.getOutputStage())) {
                    PlanFragmentId fragmentId = stage.getPlan().get().getId();
                    JsonPlanFragment jsonPlanFragment = new JsonPlanFragment(stage.getPlan().get().getJsonRepresentation().get());
                    fragmentJsonMap.put(fragmentId, jsonPlanFragment);
                }
                return Optional.of(PLAN_MAP_CODEC.toJson(fragmentJsonMap.build()));
            }
        }
        catch (Exception e) {
            // Don't fail to create event if the plan can not be created
            log.warn(e, "Error creating json plan for query %s: %s", queryInfo.getQueryId(), e);
        }
        return Optional.empty();
    }

    private static QueryIOMetadata getQueryIOMetadata(QueryInfo queryInfo)
    {
        ImmutableList.Builder<QueryInputMetadata> inputs = ImmutableList.builder();
        for (Input input : queryInfo.getInputs()) {
            inputs.add(new QueryInputMetadata(
                    input.getConnectorId().getCatalogName(),
                    input.getSchema(),
                    input.getTable(),
                    input.getColumns().stream()
                            .map(Column::getName).collect(Collectors.toList()),
                    input.getConnectorInfo(),
                    input.getStatistics()));
        }

        Optional<QueryOutputMetadata> output = Optional.empty();
        Optional<String> plan = Optional.empty();
        ImmutableList.Builder<String> predicates = ImmutableList.builder();
        if (queryInfo.getOutput().isPresent()) {
            Optional<TableFinishInfo> tableFinishInfo = queryInfo.getQueryStats().getOperatorSummaries().stream()
                    .map(OperatorStats::getInfo)
                    .filter(TableFinishInfo.class::isInstance)
                    .map(TableFinishInfo.class::cast)
                    .findFirst();

            output = Optional.of(
                    new QueryOutputMetadata(
                            queryInfo.getOutput().get().getConnectorId().getCatalogName(),
                            queryInfo.getOutput().get().getSchema(),
                            queryInfo.getOutput().get().getTable(),
                            tableFinishInfo.map(TableFinishInfo::getSerializedConnectorOutputMetadata),
                            tableFinishInfo.map(TableFinishInfo::isJsonLengthLimitExceeded)));
        }
        return new QueryIOMetadata(inputs.build(), output);
    }

    private Optional<QueryFailureInfo> createQueryFailureInfo(ExecutionFailureInfo failureInfo, Optional<StageInfo> outputStage)
    {
        if (failureInfo == null) {
            return Optional.empty();
        }

        Optional<TaskInfo> failedTask = outputStage.flatMap(QueryMonitor::findFailedTask);

        return Optional.of(new QueryFailureInfo(
                failureInfo.getErrorCode(),
                Optional.ofNullable(failureInfo.getType()),
                Optional.ofNullable(failureInfo.getMessage()),
                failedTask.map(task -> task.getTaskStatus().getTaskId().toString()),
                failedTask.map(task -> task.getTaskStatus().getSelf().getHost()),
                executionFailureInfoCodec.toJson(failureInfo)));
    }

    private static Optional<TaskInfo> findFailedTask(StageInfo stageInfo)
    {
        for (StageInfo subStage : stageInfo.getSubStages()) {
            Optional<TaskInfo> task = findFailedTask(subStage);
            if (task.isPresent()) {
                return task;
            }
        }
        return stageInfo.getLatestAttemptExecutionInfo().getTasks().stream()
                .filter(taskInfo -> taskInfo.getTaskStatus().getState() == TaskState.FAILED)
                .findFirst();
    }

    private static Map<String, String> mergeSessionAndCatalogProperties(SessionRepresentation session)
    {
        Map<String, String> mergedProperties = new LinkedHashMap<>(session.getSystemProperties());

        // Either processed or unprocessed catalog properties, but not both.  Instead of trying to enforces this while
        // firing events, allow both to be set and if there is a duplicate favor the processed properties.
        for (Map.Entry<String, Map<String, String>> catalogEntry : session.getUnprocessedCatalogProperties().entrySet()) {
            for (Map.Entry<String, String> entry : catalogEntry.getValue().entrySet()) {
                mergedProperties.put(catalogEntry.getKey() + "." + entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<ConnectorId, Map<String, String>> catalogEntry : session.getCatalogProperties().entrySet()) {
            for (Map.Entry<String, String> entry : catalogEntry.getValue().entrySet()) {
                mergedProperties.put(catalogEntry.getKey().getCatalogName() + "." + entry.getKey(), entry.getValue());
            }
        }
        return ImmutableMap.copyOf(mergedProperties);
    }

    private static void logQueryTimeline(QueryInfo queryInfo)
    {
        try {
            QueryStats queryStats = queryInfo.getQueryStats();
            DateTime queryStartTime = queryStats.getCreateTime();
            DateTime queryEndTime = queryStats.getEndTime();

            // query didn't finish cleanly
            if (queryStartTime == null || queryEndTime == null) {
                return;
            }

            // planning duration -- start to end of planning
            long planning = queryStats.getTotalPlanningTime().toMillis();

            List<StageInfo> stages = getAllStages(queryInfo.getOutputStage());
            // long lastSchedulingCompletion = 0;
            long firstTaskStartTime = queryEndTime.getMillis();
            long lastTaskStartTime = queryStartTime.getMillis() + planning;
            long lastTaskEndTime = queryStartTime.getMillis() + planning;
            for (StageInfo stage : stages) {
                // only consider leaf stages
                if (!stage.getSubStages().isEmpty()) {
                    continue;
                }

                for (TaskInfo taskInfo : stage.getLatestAttemptExecutionInfo().getTasks()) {
                    TaskStats taskStats = taskInfo.getStats();

                    DateTime firstStartTime = taskStats.getFirstStartTime();
                    if (firstStartTime != null) {
                        firstTaskStartTime = Math.min(firstStartTime.getMillis(), firstTaskStartTime);
                    }

                    DateTime lastStartTime = taskStats.getLastStartTime();
                    if (lastStartTime != null) {
                        lastTaskStartTime = max(lastStartTime.getMillis(), lastTaskStartTime);
                    }

                    DateTime endTime = taskStats.getEndTime();
                    if (endTime != null) {
                        lastTaskEndTime = max(endTime.getMillis(), lastTaskEndTime);
                    }
                }
            }

            long elapsed = max(queryEndTime.getMillis() - queryStartTime.getMillis(), 0);
            long scheduling = max(firstTaskStartTime - queryStartTime.getMillis() - planning, 0);
            long running = max(lastTaskEndTime - firstTaskStartTime, 0);
            long finishing = max(queryEndTime.getMillis() - lastTaskEndTime, 0);

            logQueryTimeline(
                    queryInfo.getQueryId(),
                    queryInfo.getSession().getTransactionId().map(TransactionId::toString).orElse(""),
                    elapsed,
                    planning,
                    scheduling,
                    running,
                    finishing,
                    queryStartTime,
                    queryEndTime);
        }
        catch (Exception e) {
            log.error(e, "Error logging query timeline");
        }
    }

    private static void logQueryTimeline(BasicQueryInfo queryInfo)
    {
        DateTime queryStartTime = queryInfo.getQueryStats().getCreateTime();
        DateTime queryEndTime = queryInfo.getQueryStats().getEndTime();

        // query didn't finish cleanly
        if (queryStartTime == null || queryEndTime == null) {
            return;
        }

        long elapsed = max(queryEndTime.getMillis() - queryStartTime.getMillis(), 0);

        logQueryTimeline(
                queryInfo.getQueryId(),
                queryInfo.getSession().getTransactionId().map(TransactionId::toString).orElse(""),
                elapsed,
                elapsed,
                0,
                0,
                0,
                queryStartTime,
                queryEndTime);
    }

    private static void logQueryTimeline(
            QueryId queryId,
            String transactionId,
            long elapsedMillis,
            long planningMillis,
            long schedulingMillis,
            long runningMillis,
            long finishingMillis,
            DateTime queryStartTime,
            DateTime queryEndTime)
    {
        log.info("TIMELINE: Query %s :: Transaction:[%s] :: elapsed %sms :: planning %sms :: scheduling %sms :: running %sms :: finishing %sms :: begin %s :: end %s",
                queryId,
                transactionId,
                elapsedMillis,
                planningMillis,
                schedulingMillis,
                runningMillis,
                finishingMillis,
                queryStartTime,
                queryEndTime);
    }

    private static void computeCpuAndMemoryDistributions(
            StageInfo stageInfo,
            ImmutableList.Builder<ResourceDistribution> cpuDistributionBuilder,
            ImmutableList.Builder<ResourceDistribution> memoryDistributionBuilder)
    {
        Distribution cpuDistribution = new Distribution();
        Distribution memoryDistribution = new Distribution();

        for (TaskInfo taskInfo : stageInfo.getLatestAttemptExecutionInfo().getTasks()) {
            cpuDistribution.add(taskInfo.getStats().getTotalCpuTime().toMillis());
            memoryDistribution.add(taskInfo.getStats().getPeakTotalMemoryInBytes());
        }

        cpuDistributionBuilder.add(createResourceDistribution(
                stageInfo.getStageId().getId(),
                stageInfo.getLatestAttemptExecutionInfo().getTasks().size(),
                cpuDistribution.snapshot()));

        memoryDistributionBuilder.add(createResourceDistribution(
                stageInfo.getStageId().getId(),
                stageInfo.getLatestAttemptExecutionInfo().getTasks().size(),
                memoryDistribution.snapshot()));

        for (StageInfo subStage : stageInfo.getSubStages()) {
            computeCpuAndMemoryDistributions(subStage, cpuDistributionBuilder, memoryDistributionBuilder);
        }
    }

    public static class JsonPlanFragment
    {
        @JsonRawValue
        private final String plan;

        @JsonCreator
        public JsonPlanFragment(String plan)
        {
            this.plan = plan;
        }

        @JsonProperty
        public String getPlan()
        {
            return this.plan;
        }
    }
}
