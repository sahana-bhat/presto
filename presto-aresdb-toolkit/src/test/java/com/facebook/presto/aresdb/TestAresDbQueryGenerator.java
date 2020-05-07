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

import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.aresdb.AresDbQueryGenerator.AresDbQueryGeneratorResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbQueryGenerator.isBooleanTrue;
import static com.facebook.presto.spi.plan.AggregationNode.GroupingSetDescriptor;
import static com.facebook.presto.spi.plan.AggregationNode.Step;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;

public class TestAresDbQueryGenerator
        extends TestAresDbQueryBase
{
    // Test table and related info
    private static AresDbTableHandle aresdbTable = new AresDbTableHandle("connId", "tbl", Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), new AresDbMuttleyConfig("", ImmutableMap.of()));
    //private static AresDbTableHandle joinTable = new AresDbTableHandle(new AresDbConnectorId("connId"), "dim", Optional.empty(), Optional.empty(), Optional.empty());

    private void testAQL(PlanNode planNode, String expectedAQL, ConnectorSession connectorSession)
    {
        AresDbQueryGeneratorResult augmentedAQL = new AresDbQueryGenerator(typeManager, functionMetadataManager, standardFunctionResolution, logicalRowExpressions, rowExpressionService).generate(planNode, connectorSession).get();
        Object expected = JSONObject.parse(expectedAQL);
        Object actual = JSONObject.parse(augmentedAQL.getGeneratedAql().getAql());
        //System.out.println("expected: " + expected.toString() + "\nactual: " + actual.toString());
        assertEquals(actual, expected, augmentedAQL.getGeneratedAql().getAql());
    }

    private void unaryTestHelper(RowExpression unaryFunction, String param)
    {
        ConnectorSession session = sessionHolder.getConnectorSession();

        // `select agg from tbl group by regionId`
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder);
        TableScanNode source1 = tableScan(planBuilder1, aresdbTable, regionId, fare);
        GroupingSetDescriptor groupingSetDescriptor = new GroupingSetDescriptor(Collections.singletonList(v("regionid")), 1, new HashSet<Integer>());
        boolean isPartial = false;
        PlanNode planNode = planBuilder1.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder1.variable("fare_total", DOUBLE), unaryFunction)
                        .source(source1)
                        .groupingSets(groupingSetDescriptor));
        String expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"}],\"measures\":[{\"sqlExpression\":\"%s\"}],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, format(expectedAggOutputFormat, param), session);

        // `select regionid, agg from tbl group by regionId`
        PlanBuilder planBuilder2 = createPlanBuilder(sessionHolder);
        TableScanNode source2 = tableScan(planBuilder2, aresdbTable, fare, regionId);
        planNode = planBuilder2.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder2.variable("fare_total", DOUBLE), unaryFunction)
                        .source(source2)
                        .groupingSets(groupingSetDescriptor));
        testAQL(planNode, format(expectedAggOutputFormat, param), session);

        // `select regionid, agg, city from tbl group by regionId, city`
        PlanBuilder planBuilder3 = createPlanBuilder(sessionHolder);
        TableScanNode source3 = tableScan(planBuilder3, aresdbTable, regionId, fare, city);
        planNode = planBuilder3.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder3.variable("fare_total", DOUBLE), unaryFunction)
                        .source(source3)
                        .singleGroupingSet(v("regionid"), v("city")));
        expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"%s\"}],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, format(expectedAggOutputFormat, param), session);

        // `select regionid, agg from tbl group by regionId where regionid > 20`
        PlanBuilder planBuilder4 = createPlanBuilder(sessionHolder);
        TableScanNode source4 = tableScan(planBuilder4, aresdbTable, regionId, fare, city);
        FilterNode filterNode1 = filter(planBuilder4, source4, getRowExpression("regionid > 20", sessionHolder));
        planNode = planBuilder4.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder4.variable("fare_total", DOUBLE), unaryFunction)
                        .source(filterNode1)
                        .groupingSets(groupingSetDescriptor));
        expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"}],\"measures\":[{\"sqlExpression\":\"%s\"}],\"rowFilters\":[\"(regionId > 20)\"],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, format(expectedAggOutputFormat, param), session);

        // `select regionid, agg, city from tbl group by regionId, city where regionid > 20`
        PlanBuilder planBuilder5 = createPlanBuilder(sessionHolder);
        TableScanNode source5 = tableScan(planBuilder5, aresdbTable, regionId, fare, city);
        FilterNode filterNode2 = filter(planBuilder5, source5, getRowExpression("regionid > 20", sessionHolder));
        planNode = planBuilder5.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder5.variable("fare_total", DOUBLE), unaryFunction)
                        .source(filterNode2)
                        .singleGroupingSet(v("regionid"), v("city")));
        expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"%s\"}],\"rowFilters\":[\"(regionId > 20)\"],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, format(expectedAggOutputFormat, param), session);

        // `select regionid, agg, city from tbl group by regionId, city where secondssinceepoch between 200 and 300 and regionid >= 40`
        PlanBuilder planBuilder6 = createPlanBuilder(sessionHolder);
        TableScanNode source6 = tableScan(planBuilder6, aresdbTable, regionId, city, secondsSinceEpoch, fare);
        FilterNode filterNode3 = filter(planBuilder6, source6, getRowExpression("secondssinceepoch between 200 and 300 and regionid >= 40", sessionHolder));
        planNode = planBuilder6.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder6.variable("fare_total", DOUBLE), unaryFunction)
                        .source(filterNode3)
                        .singleGroupingSet(v("regionid"), v("city")));
        expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"%s\"}],\"rowFilters\":[\"(((secondsSinceEpoch >= 200) AND (secondsSinceEpoch <= 300)) AND (regionId >= 40))\"],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, format(expectedAggOutputFormat, param), session);
    }

    @Test
    public void testSimpleSelectStar()
    {
        ConnectorSession session = sessionHolder.getConnectorSession();
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder);
        PlanNode planNode = tableScan(planBuilder1, aresdbTable, regionId, city, fare, secondsSinceEpoch);
        testAQL(planNode, "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"fare\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":-1,\"measures\":[{\"sqlExpression\":\"1\"}],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}", session);
    }

    @Test
    public void testSimplePartialColumnSelection()
    {
        ConnectorSession session = sessionHolder.getConnectorSession();
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder);
        PlanNode planNode = tableScan(planBuilder1, aresdbTable, regionId, secondsSinceEpoch);
        testAQL(planNode, "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":-1,\"measures\":[{\"sqlExpression\":\"1\"}],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}", session);
    }

    @Test
    public void testSimpleSelectWithLimit()
    {
        ConnectorSession session = sessionHolder.getConnectorSession();
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode = tableScan(planBuilder1, aresdbTable, regionId, city, fare);
        PlanNode planNode = limit(planBuilder1, 50, scanNode);
        testAQL(planNode, "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"fare\"}],\"limit\":50,\"measures\":[{\"sqlExpression\":\"1\"}],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}", session);
    }

    @Test
    public void testMultipleFilters()
    {
        ConnectorSession session = sessionHolder.getConnectorSession();
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode = tableScan(planBuilder1, aresdbTable, cityId, secondsSinceEpoch);
        FilterNode filterNode1 = filter(planBuilder1, scanNode, getRowExpression("secondssinceepoch > 20", sessionHolder));
        PlanNode planNode = filter(planBuilder1, filterNode1, getRowExpression("cityid < 200 AND cityid > 10", sessionHolder));
        testAQL(planNode, "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"cityId\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":-1,\"measures\":[{\"sqlExpression\":\"1\"}],\"rowFilters\":[\"(secondsSinceEpoch > 20)\",\"((cityId < 200) AND (cityId > 10))\"],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}", session);
    }

    @Test
    public void testSimpleSelectWithFilter()
    {
        ConnectorSession session = sessionHolder.getConnectorSession();
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode = tableScan(planBuilder1, aresdbTable, city, secondsSinceEpoch);
        PlanNode planNode = filter(planBuilder1, scanNode, getRowExpression("secondssinceepoch > 20", sessionHolder));
        testAQL(planNode, "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":-1,\"measures\":[{\"sqlExpression\":\"1\"}],\"rowFilters\":[\"(secondsSinceEpoch > 20)\"],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}", session);
    }

    @Test
    public void testSimpleSelectWithFilterLimit()
    {
        ConnectorSession session = sessionHolder.getConnectorSession();
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode = tableScan(planBuilder1, aresdbTable, city, secondsSinceEpoch);
        FilterNode filterNode = filter(planBuilder1, scanNode, getRowExpression("secondssinceepoch > 20", sessionHolder));
        PlanNode planNode = limit(planBuilder1, 50, filterNode);
        testAQL(planNode, "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":50,\"measures\":[{\"sqlExpression\":\"1\"}],\"rowFilters\":[\"(secondsSinceEpoch > 20)\"],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}", session);
    }

    private FilterNode createFilterForExpression(String expr, SessionHolder sessionHolder, PlanBuilder planBuilder, PlanNode source)
    {
        RowExpression predicateExpr = getRowExpression(expr, sessionHolder);
        DomainTranslator.ExtractionResult<VariableReferenceExpression> extractionResult = domainTranslator.fromPredicate(sessionHolder.getConnectorSession(), predicateExpr, DomainTranslator.BASIC_COLUMN_EXTRACTOR);
        HashMap<VariableReferenceExpression, Domain> domainMap = (HashMap<VariableReferenceExpression, Domain>) extractionResult.getTupleDomain().getDomains().get().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ImmutableList.Builder<RowExpression> remainingPredicateBuilder = ImmutableList.builder();
        if (extractionResult.getRemainingExpression() != null && !isBooleanTrue(extractionResult.getRemainingExpression())) {
            remainingPredicateBuilder.add(extractionResult.getRemainingExpression());
        }
        remainingPredicateBuilder.add(domainTranslator.toPredicate(TupleDomain.withColumnDomains(domainMap)));
        return filter(planBuilder, source, logicalRowExpressions.combineConjuncts(remainingPredicateBuilder.build()));
    }

    @Test
    public void testTimeFilterDefaultRetentionBounds()
    {
        long currentTime = System.currentTimeMillis() + 100; // 100 to make sure it is not aligned to second boundary;
        ConnectorSession session = new TestingConnectorSession("user", Optional.of("test"), Optional.empty(), UTC_KEY, ENGLISH, currentTime, new AresDbSessionProperties(aresDbConfig).getSessionProperties(), ImmutableMap.of(), new FeaturesConfig().isLegacyTimestamp(), Optional.empty());
        Duration retention = new Duration(2, TimeUnit.DAYS);
        AresDbTableHandle tableWithRetention = new AresDbTableHandle("connId", "tbl", Optional.of("secondsSinceEpoch"), Optional.of(BIGINT), Optional.of(retention), Optional.empty(), Optional.empty(), new AresDbMuttleyConfig("", ImmutableMap.of()));
        long retentionTime = currentTime - retention.toMillis();
        long highSecondsExpected = (currentTime + 999) / 1000;
        long lowSecondsExpected = retentionTime / 1000;

        SessionHolder sessionHolder1 = new SessionHolder(session);
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder1);
        TableScanNode scanNode1 = tableScan(planBuilder1, tableWithRetention, regionId, city, secondsSinceEpoch);
        FilterNode filterNode1 = createFilterForExpression("regionid in (3, 4)", sessionHolder1, planBuilder1, scanNode1);
        PlanNode planNode = limit(planBuilder1, 50, filterNode1);
        String expectedAql = String.format("{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":50,\"measures\":[{\"sqlExpression\":\"1\"}],\"rowFilters\":[\"(regionId IN (3, 4))\"],\"table\":\"tbl\",\"timeFilter\":{\"column\":\"secondsSinceEpoch\",\"from\":\"%d\",\"to\":\"%d\"},\"timeZone\":\"UTC\"}]}", lowSecondsExpected, highSecondsExpected);
        testAQL(planNode, expectedAql, sessionHolder1.getConnectorSession());

        PlanBuilder planBuilder2 = createPlanBuilder(sessionHolder1);
        TableScanNode scanNode2 = tableScan(planBuilder2, tableWithRetention, regionId, city, secondsSinceEpoch);
        FilterNode filterNode2 = createFilterForExpression(String.format("regionid in (3, 4) and (secondssinceepoch = %d or secondssinceepoch = (%d + 1))", highSecondsExpected - 2, highSecondsExpected - 4), sessionHolder1, planBuilder2, scanNode2);
        planNode = limit(planBuilder2, 50, filterNode2);
        expectedAql = String.format("{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":50,\"measures\":[{\"sqlExpression\":\"1\"}],\"rowFilters\":[\"(regionId IN (3, 4))\"],\"table\":\"tbl\",\"timeFilter\":{\"column\":\"secondsSinceEpoch\",\"from\":\"%d\",\"to\":\"%d\"},\"timeZone\":\"UTC\"}]}", highSecondsExpected - 3, highSecondsExpected - 2);
        testAQL(planNode, expectedAql, sessionHolder1.getConnectorSession());
    }

    /*
    @Test
    public void testTimeFilter()
    {
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode1 = tableScan(planBuilder1, aresDbTableHandle, regionId, city, secondsSinceEpoch);
        FilterNode filterNode1 = createFilterForExpression("regionid in (3, 4) and ((secondssinceepoch between 100 and 200) and (secondssinceepoch >= 150 or secondssinceepoch < 300))", sessionHolder, planBuilder1, scanNode1);
        PlanNode planNode = limit(planBuilder1, 50, filterNode1);
        String expectedAql = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":50,\"measures\":[{\"sqlExpression\":\"1\"}],\"rowFilters\":[\"(regionId IN (3, 4))\"],\"table\":\"tbl\",\"timeFilter\":{\"column\":\"secondsSinceEpoch\",\"from\":\"100\",\"to\":\"200\"},\"timeZone\":\"" + sessionHolder.getConnectorSession().getTimeZoneKey() + "\"}]}";
        testAQL(planNode, expectedAql, sessionHolder.getConnectorSession());

        PlanBuilder planBuilder2 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode2 = tableScan(planBuilder2, aresDbTableHandle, regionId, city, secondsSinceEpoch);
        FilterNode filterNode2 = createFilterForExpression("regionid in (3, 4) and ((secondssinceepoch between 100 and 125) or (secondssinceepoch >= 150 and secondssinceepoch < 200))", sessionHolder, planBuilder2, scanNode2);
        planNode = limit(planBuilder2, 50, filterNode2);
        expectedAql = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":50,\"measures\":[{\"sqlExpression\":\"1\"}],\"rowFilters\":[\"(regionId IN (3, 4))\",\"(((secondsSinceEpoch >= 100) AND (secondsSinceEpoch <= 125)) OR ((secondsSinceEpoch >= 150) AND (secondsSinceEpoch < 200)))\"],\"table\":\"tbl\",\"timeFilter\":{\"column\":\"secondsSinceEpoch\",\"from\":\"100\",\"to\":\"200\"},\"timeZone\":\"" + sessionHolder.getConnectorSession().getTimeZoneKey() + "\"}]}";
        testAQL(planNode, expectedAql, sessionHolder.getConnectorSession());

        PlanBuilder planBuilder3 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode3 = tableScan(planBuilder3, aresDbTableHandle, regionId, city, secondsSinceEpoch);
        FilterNode filterNode3 = createFilterForExpression("secondssinceepoch >= 100", sessionHolder, planBuilder3, scanNode3);
        planNode = limit(planBuilder3, 50, filterNode3);
        expectedAql = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"},{\"sqlExpression\":\"secondsSinceEpoch\"}],\"limit\":50,\"measures\":[{\"sqlExpression\":\"1\"}],\"table\":\"tbl\",\"timeFilter\":{\"column\":\"secondsSinceEpoch\",\"from\":\"100\"},\"timeZone\":\"" + sessionHolder.getConnectorSession().getTimeZoneKey() + "\"}]}";
        testAQL(planNode, expectedAql, sessionHolder.getConnectorSession());
    }
    */

    @Test
    public void testCountStarAggregation()
    {
        ConnectorSession session = sessionHolder.getConnectorSession();

        // `select count(*) from tbl group by regionId`
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder);
        TableScanNode source1 = tableScan(planBuilder1, aresdbTable, regionId);
        GroupingSetDescriptor groupingSetDescriptor = new GroupingSetDescriptor(Collections.singletonList(v("regionid")), 1, new HashSet<Integer>());
        boolean isPartial = false;
        RowExpression countStar = getRowExpression("count(*)", sessionHolder);
        PlanNode planNode = planBuilder1.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder1.variable("count_all", BIGINT), countStar)
                        .source(source1)
                        .groupingSets(groupingSetDescriptor));
        String expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"}],\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, format(expectedAggOutputFormat, "count(*)"), session);

        // `select regionid, count(*) from tbl group by regionId`
        PlanBuilder planBuilder2 = createPlanBuilder(sessionHolder);
        TableScanNode source2 = tableScan(planBuilder2, aresdbTable, regionId);
        planNode = planBuilder2.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder2.variable("count_all", BIGINT), countStar)
                        .source(source2)
                        .groupingSets(groupingSetDescriptor));
        expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"}],\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, format(expectedAggOutputFormat, "count(*)"), session);

        // `select regionid, count(*), city from tbl group by regionId`
        PlanBuilder planBuilder3 = createPlanBuilder(sessionHolder);
        TableScanNode source3 = tableScan(planBuilder3, aresdbTable, regionId, city);
        planNode = planBuilder3.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder3.variable("count_all", BIGINT), countStar)
                        .source(source3)
                        .singleGroupingSet(v("regionid"), v("city")));
        expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, format(expectedAggOutputFormat, "count(*)"), session);

        // `select regionid, count(*), city from tbl group by regionId where regionid > 20`
        PlanBuilder planBuilder4 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode4 = tableScan(planBuilder4, aresdbTable, regionId, city);
        FilterNode filterNode4 = filter(planBuilder4, scanNode4, getRowExpression("regionid > 20", sessionHolder));
        planNode = planBuilder4.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder4.variable("count_all", BIGINT), countStar)
                        .source(filterNode4)
                        .singleGroupingSet(v("regionid"), v("city")));
        expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"rowFilters\":[\"(regionId > 20)\"],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, format(expectedAggOutputFormat, "count(*)"), session);

        // `select regionid, count(*), city from tbl group by regionId where secondssinceepoch between 200 and 300 and regionid >= 40`
        PlanBuilder planBuilder5 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode5 = tableScan(planBuilder5, aresdbTable, regionId, city, secondsSinceEpoch);
        FilterNode filterNode5 = filter(planBuilder5, scanNode5, getRowExpression("secondssinceepoch between 200 and 300 and regionid >= 40", sessionHolder));
        planNode = planBuilder5.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder5.variable("count_all", BIGINT), countStar)
                        .source(filterNode5)
                        .singleGroupingSet(v("regionid"), v("city")));
        expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"rowFilters\":[\"(((secondsSinceEpoch >= 200) AND (secondsSinceEpoch <= 300)) AND (regionId >= 40))\"],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, format(expectedAggOutputFormat, "count(*)"), session);

        // `select count(*) from tbl;`
        PlanBuilder planBuilder6 = createPlanBuilder(sessionHolder);
        TableScanNode source6 = tableScan(planBuilder6, aresdbTable);
        planNode = planBuilder6.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .globalGrouping()
                        .addAggregation(planBuilder6.variable("count_all", BIGINT), countStar)
                        .source(source6));
        expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"1\"}],\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, format(expectedAggOutputFormat, "count(*)"), session);

        // `select count(*) from tbl limit 3;`
        PlanBuilder planBuilder7 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode7 = tableScan(planBuilder7, aresdbTable);
        AggregationNode aggregationNode7 = planBuilder7.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .globalGrouping()
                        .addAggregation(planBuilder7.variable("count_all", BIGINT), countStar)
                        .source(scanNode7));
        planNode = limit(planBuilder7, 3, aggregationNode7);
        expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"1\"}],\"limit\":3,\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, format(expectedAggOutputFormat, "count(*)"), session);
    }

    @Test
    public void testDistinctSelection()
    {
        // `select regionId, city from tbl group by 1, 2;`
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder);
        TableScanNode source1 = tableScan(planBuilder1, aresdbTable, regionId, city);
        PlanNode planNode = planBuilder1.aggregation(
                af -> af.step(Step.FINAL)
                        .source(source1)
                        .singleGroupingSet(v("regionid"), v("city")));
        String expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"table\":\"tbl\",\"timeZone\":\"" + sessionHolder.getConnectorSession().getTimeZoneKey() + "\"}]}";
        testAQL(planNode, expectedAggOutputFormat, sessionHolder.getConnectorSession());

        // `select regionId, city from tbl group by 1, 2 limit 5;`
        PlanBuilder planBuilder2 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode2 = tableScan(planBuilder2, aresdbTable, regionId, city);
        AggregationNode aggregationNode2 = planBuilder2.aggregation(
                af -> af.step(Step.FINAL)
                        .source(scanNode2)
                        .singleGroupingSet(v("regionid"), v("city")));
        planNode = limit(planBuilder2, 5, aggregationNode2);
        expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"},{\"sqlExpression\":\"city\"}],\"limit\":5,\"measures\":[{\"sqlExpression\":\"count(*)\"}],\"table\":\"tbl\",\"timeZone\":\"" + sessionHolder.getConnectorSession().getTimeZoneKey() + "\"}]}";
        testAQL(planNode, expectedAggOutputFormat, sessionHolder.getConnectorSession());
    }

    @Test
    public void testUnaryAggregation()
    {
        RowExpression countFare = getRowExpression("count(fare)", sessionHolder);
        unaryTestHelper(countFare, "count(fare)");

        RowExpression sumFare = getRowExpression("sum(fare)", sessionHolder);
        unaryTestHelper(sumFare, "sum(fare)");

        RowExpression minFare = getRowExpression("min(fare)", sessionHolder);
        unaryTestHelper(minFare, "min(fare)");

        RowExpression maxFare = getRowExpression("max(fare)", sessionHolder);
        unaryTestHelper(maxFare, "max(fare)");

        RowExpression avgFare = getRowExpression("avg(fare)", sessionHolder);
        unaryTestHelper(avgFare, "avg(fare)");

        RowExpression aprxDistinctFare = getRowExpression("approx_distinct(fare)", sessionHolder);
        unaryTestHelper(aprxDistinctFare, "countdistincthll(fare)");
    }

    @Test
    public void testApproxDistinct()
    {
        ConnectorSession session = sessionHolder.getConnectorSession();

        // `select agg from tbl group by regionId`
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode1 = tableScan(planBuilder1, aresdbTable, regionId, fare);
        ProjectNode project1 = project(planBuilder1, scanNode1, ImmutableList.of("regionid", "fare"));
        GroupingSetDescriptor groupingSetDescriptor = new GroupingSetDescriptor(Collections.singletonList(v("regionid")), 1, new HashSet<Integer>());
        boolean isPartial = false;
        RowExpression aprxDistinctFare = getRowExpression("approx_distinct(fare)", sessionHolder);
        PlanNode planNode = planBuilder1.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder1.variable("fare_total", DOUBLE), aprxDistinctFare)
                        .source(project1)
                        .groupingSets(groupingSetDescriptor));
        String expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"}],\"measures\":[{\"sqlExpression\":\"countdistincthll(fare)\"}],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, expectedAggOutputFormat, session);

        // `select regionid, agg, city from tbl group by regionId where regionid > 20`
        PlanBuilder planBuilder2 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode2 = tableScan(planBuilder2, aresdbTable, regionId, fare, city);
        FilterNode filterNode2 = filter(planBuilder2, scanNode2, getRowExpression("regionid > 20", sessionHolder));
        ProjectNode project2 = project(planBuilder1, filterNode2, ImmutableList.of("regionid", "fare", "city"));
        planNode = planBuilder2.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder2.variable("fare_total", DOUBLE), aprxDistinctFare)
                        .source(project2)
                        .groupingSets(groupingSetDescriptor));
        expectedAggOutputFormat = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"regionId\"}],\"measures\":[{\"sqlExpression\":\"countdistincthll(fare)\"}],\"rowFilters\":[\"(regionId > 20)\"],\"table\":\"tbl\",\"timeZone\":\"" + session.getTimeZoneKey() + "\"}]}";
        testAQL(planNode, expectedAggOutputFormat, session);
    }

    @Test
    public void testAggWithUDFInGroupBy()
    {
        ConnectorSession session = sessionHolder.getConnectorSession();
        boolean isPartial = false;
        RowExpression aprxDistinctFare = getRowExpression("approx_distinct(fare)", sessionHolder);
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode1 = tableScan(planBuilder1, aresdbTable, city, fare, secondsSinceEpoch);
        LinkedHashMap<String, String> toProject = new LinkedHashMap<>();
        toProject.put("city", "city");
        toProject.put("fare", "fare");
        toProject.put("dateagg", "date_trunc('day', cast(from_unixtime(secondssinceepoch - 50) AS TIMESTAMP))");
        ProjectNode projectNode1 = project(planBuilder1, scanNode1, toProject, sessionHolder);
        PlanNode planNode = planBuilder1.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .source(projectNode1)
                        .singleGroupingSet(new VariableReferenceExpression("dateagg", TIMESTAMP), v("city"))
                        .addAggregation(planBuilder1.variable("fare_total", DOUBLE), aprxDistinctFare));
        String expectedOutputAQL = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"secondsSinceEpoch - 50\",\"timeBucketizer\":\"day\",\"timeUnit\":\"second\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"countdistincthll(fare)\"}],\"table\":\"tbl\",\"timeZone\":\"UTC\"}]}";
        testAQL(planNode, expectedOutputAQL, session);

        PlanBuilder planBuilder2 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode2 = tableScan(planBuilder2, aresdbTable, city, fare, secondsSinceEpoch);
        toProject = new LinkedHashMap<>();
        toProject.put("city", "city");
        toProject.put("fare", "fare");
        toProject.put("dateagg", "date_trunc('week', cast(from_unixtime(secondssinceepoch - 50, 'America/New_York') AS TIMESTAMP WITH TIME ZONE))");
        ProjectNode projectNode2 = project(planBuilder2, scanNode2, toProject, sessionHolder);
        planNode = planBuilder2.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .source(projectNode2)
                        .singleGroupingSet(new VariableReferenceExpression("dateagg", TIMESTAMP_WITH_TIME_ZONE), v("city"))
                        .addAggregation(planBuilder2.variable("fare_total", DOUBLE), aprxDistinctFare));
        expectedOutputAQL = "{\"queries\":[{\"dimensions\":[{\"sqlExpression\":\"secondsSinceEpoch - 50\",\"timeBucketizer\":\"week\",\"timeUnit\":\"second\"},{\"sqlExpression\":\"city\"}],\"measures\":[{\"sqlExpression\":\"countdistincthll(fare)\"}],\"table\":\"tbl\",\"timeZone\":\"America/New_York\"}]}";
        testAQL(planNode, expectedOutputAQL, session);
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testMultipleAggregates()
    {
        ConnectorSession session = sessionHolder.getConnectorSession();
        boolean isPartial = false;
        RowExpression minFare = getRowExpression("min(fare)", sessionHolder);
        RowExpression countFare = getRowExpression("count(fare)", sessionHolder);
        RowExpression maxFare = getRowExpression("max(fare)", sessionHolder);
        RowExpression aprxDistinctFare = getRowExpression("approx_distinct(fare)", sessionHolder);
        PlanBuilder planBuilder1 = createPlanBuilder(sessionHolder);
        TableScanNode scanNode1 = tableScan(planBuilder1, aresdbTable, city, fare, secondsSinceEpoch);
        LinkedHashMap<String, String> toProject = new LinkedHashMap<>();
        toProject.put("city", "city");
        toProject.put("fare", "fare");
        toProject.put("dateagg", "date_trunc('day', cast(from_unixtime(secondssinceepoch - 50) AS TIMESTAMP))");
        ProjectNode projectNode1 = project(planBuilder1, scanNode1, toProject, sessionHolder);
        PlanNode planNode = planBuilder1.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .source(projectNode1)
                        .addAggregation(planBuilder1.variable("fare_total", DOUBLE), aprxDistinctFare)
                        .addAggregation(planBuilder1.variable("min_fare", DOUBLE), minFare)
                        .addAggregation(planBuilder1.variable("max_fare", DOUBLE), maxFare)
                        .addAggregation(planBuilder1.variable("count_fare", DOUBLE), countFare)
                        .singleGroupingSet(new VariableReferenceExpression("dateagg", TIMESTAMP), v("city")));
        testAQL(planNode, null, session);
    }
}
