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

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAresDbSplitManager
        extends TestAresDbQueryBase
{
    private List<AresDbSplit> getSplits(AresDbTableHandle aresDbTableHandle, Map<String, Object> properties)
    {
        AresDbConfig aresDbConfig = new AresDbConfig();
        ConnectorSession session = new TestingConnectorSession("user", Optional.of("test"), Optional.empty(), UTC_KEY, ENGLISH, System.currentTimeMillis(), new AresDbSessionProperties(aresDbConfig).getSessionProperties(), properties, new FeaturesConfig().isLegacyTimestamp(), Optional.empty());
        AresDbSplitManager aresDbSplitManager = new AresDbSplitManager(new ConnectorId("aresdb"), typeManager, functionMetadataManager, standardFunctionResolution, rowExpressionService);
        AresDbTableLayoutHandle aresDbTableLayoutHandle = new AresDbTableLayoutHandle(aresDbTableHandle);
        return getSplits(aresDbSplitManager.getSplits(new TestingTransactionHandle(UUID.randomUUID()), session, aresDbTableLayoutHandle, null));
    }

    private List<AresDbSplit> getSplits(ConnectorSplitSource splitSource)
    {
        List<AresDbSplit> splits = new ArrayList<>();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits().stream().map(s -> (AresDbSplit) s).collect(toList()));
        }
        return splits;
    }

    private List<AresDbSplit> getSplitsHelper(boolean isPartial, int maxNumOfSplits, String singleSplitLimit)
    {
        PlanBuilder planBuilder = createPlanBuilder(sessionHolder);
        TableScanNode scanNode = tableScan(planBuilder, aresDbTableHandle, regionId, fare);
        PlanNode planNode = agg(planBuilder, scanNode, isPartial, sessionHolder);
        AresDbQueryGenerator.AresDbQueryGeneratorResult generateResult = new AresDbQueryGenerator(typeManager, functionMetadataManager, standardFunctionResolution, logicalRowExpressions, rowExpressionService).generate(planNode, sessionHolder.getConnectorSession()).get();
        AresDbTableHandle newTableHandle = new AresDbTableHandle(
                aresDbTableHandle.getConnectorId(),
                aresDbTableHandle.getTableName(),
                aresDbTableHandle.getTimeColumnName(),
                aresDbTableHandle.getTimeStampType(),
                aresDbTableHandle.getRetention(),
                Optional.of(generateResult.isQueryShort()),
                Optional.of(generateResult),
                aresDbTableHandle.getMuttleyConfig());
        return getSplits(newTableHandle, ImmutableMap.of("max_number_of_splits", maxNumOfSplits, "single_split_limit", singleSplitLimit, "unsafe_to_cache_interval", "0d"));
    }

    @Test
    public void testPartialAggregationSplittingWithOneAqlPerSplit()
    {
        List<AresDbSplit> splits = getSplitsHelper(true, 100, "1d");
        assertEquals(splits.size(), 91, "Default retention is 90 days as stated above");
        splits.forEach(s -> {
            List<AresDbSplit.AresQL> aqls = s.getAqls();
            assertEquals(aqls.size(), 1, "Should have one AQL per split");
        });
        long numNonCachedAqls = splits.stream().flatMap(s -> s.getAqls().stream()).filter(aql -> !aql.isCacheable()).count();
        assertTrue(numNonCachedAqls <= 2, "Only the beginning and final aql should be uncacheable");
    }

    @Test
    public void testPartialAggregationSplittingWithManyAqlSingleSplit()
    {
        List<AresDbSplit> splits = getSplitsHelper(true, 1, "1d");
        assertEquals(splits.size(), 1, "All aqls should be packed into single split");
        List<AresDbSplit.AresQL> aqls = splits.get(0).getAqls();
        assertEquals(aqls.size(), 91, "All aqls should be in here, one per day");
        long numNonCachedAqls = aqls.stream().filter(aql -> !aql.isCacheable()).count();
        assertTrue(numNonCachedAqls <= 2, "Only the beginning and final aql should be uncacheable");
    }

    @Test
    public void testFinalAggregationNotSplit()
    {
        List<AresDbSplit> splits = getSplitsHelper(false, 100, "1000d");
        assertEquals(splits.size(), 1, "Final aggregation not split");
        List<AresDbSplit.AresQL> aqls = splits.get(0).getAqls();
        assertEquals(aqls.size(), 1, "No splitting");
        assertFalse(aqls.get(0).isCacheable(), "Single aql not cached");
    }

    @Test
    public void testNonAggregateQueryNotSplit()
    {
        PlanBuilder planBuilder = createPlanBuilder(sessionHolder);
        TableScanNode scanNode = tableScan(planBuilder, aresDbTableHandle, regionId, fare);
        FilterNode filterNode = filter(planBuilder, scanNode, getRowExpression("fare > 10 AND regionid != 3", sessionHolder));
        PlanNode planNode = limit(planBuilder, 10, filterNode);
        AresDbQueryGenerator.AresDbQueryGeneratorResult generateResult = new AresDbQueryGenerator(typeManager, functionMetadataManager, standardFunctionResolution, logicalRowExpressions, rowExpressionService).generate(planNode, sessionHolder.getConnectorSession()).get();
        AresDbTableHandle newTableHandle = new AresDbTableHandle(
                aresDbTableHandle.getConnectorId(),
                aresDbTableHandle.getTableName(),
                aresDbTableHandle.getTimeColumnName(),
                aresDbTableHandle.getTimeStampType(),
                aresDbTableHandle.getRetention(),
                Optional.of(generateResult.isQueryShort()),
                Optional.of(generateResult),
                aresDbTableHandle.getMuttleyConfig());
        List<AresDbSplit> splits = getSplits(newTableHandle, ImmutableMap.of("max_number_of_splits", 100, "single_split_limit", "1d", "unsafe_to_cache_interval", "0d"));
        assertEquals(splits.size(), 1, "Non aggregate query shouldn't be split");
        List<AresDbSplit.AresQL> aqls = splits.get(0).getAqls();
        assertEquals(aqls.size(), 1, "No splitting");
        assertFalse(aqls.get(0).isCacheable(), "Single aql not cached");
    }
}
