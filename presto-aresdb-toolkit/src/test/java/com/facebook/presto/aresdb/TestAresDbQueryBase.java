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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;
import com.facebook.presto.sql.relational.ConnectorRowExpressionService;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.facebook.presto.aresdb.AresDbColumnHandle.AresDbColumnType.REGULAR;
import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Origin.TABLE;
import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Selection;
import static com.facebook.presto.spi.plan.AggregationNode.GroupingSetDescriptor;
import static com.facebook.presto.spi.plan.AggregationNode.Step;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.LimitNode.Step.FINAL;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TestAresDbQueryBase
{
    protected static final TypeManager typeManager = new TypeRegistry();
    protected static final FunctionManager functionMetadataManager = new FunctionManager(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
    protected static final StandardFunctionResolution standardFunctionResolution = new FunctionResolution(functionMetadataManager);

    protected static ConnectorId aresDbConnectorId = new ConnectorId("id");
    protected static AresDbColumnHandle regionId = new AresDbColumnHandle("regionId", BIGINT, REGULAR);
    protected static AresDbColumnHandle fare = new AresDbColumnHandle("fare", DOUBLE, REGULAR);
    protected static AresDbColumnHandle city = new AresDbColumnHandle("city", VARCHAR, REGULAR);
    protected static AresDbColumnHandle cityId = new AresDbColumnHandle("cityId", BIGINT, REGULAR);
    protected static AresDbColumnHandle cityZipCode = new AresDbColumnHandle("zip", BIGINT, REGULAR);
    protected static AresDbColumnHandle cityPopulation = new AresDbColumnHandle("population", BIGINT, REGULAR);
    protected static AresDbColumnHandle secondsSinceEpoch = new AresDbColumnHandle("secondsSinceEpoch", BIGINT, REGULAR);
    protected static AresDbTableHandle aresDbTableHandle = new AresDbTableHandle(aresDbConnectorId.getCatalogName(), "tbl", Optional.of("secondsSinceEpoch"), Optional.of(BIGINT), Optional.of(new Duration(90, TimeUnit.DAYS)), Optional.empty(), Optional.empty());

    protected static final Metadata metadata = MetadataManager.createTestMetadataManager();
    protected static final DomainTranslator domainTranslator = new RowExpressionDomainTranslator(metadata);
    protected static final PredicateCompiler predicateCompiler = new RowExpressionPredicateCompiler(metadata);
    protected static final DeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata);
    protected static final RowExpressionService rowExpressionService = new ConnectorRowExpressionService(
            domainTranslator,
            new RowExpressionOptimizer(metadata),
            predicateCompiler,
            determinismEvaluator,
            new RowExpressionFormatter(functionMetadataManager));
    protected static final LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(rowExpressionService.getDeterminismEvaluator(), standardFunctionResolution, functionMetadataManager);
    protected final AresDbConfig aresDbConfig = new AresDbConfig();
    protected SessionHolder sessionHolder = new SessionHolder(aresDbConfig);

    protected static final Map<VariableReferenceExpression, Selection> testInput = ImmutableMap.<VariableReferenceExpression, Selection>builder()
            .put(new VariableReferenceExpression("regionid", BIGINT), new Selection("regionId", TABLE, Optional.empty()))
            .put(new VariableReferenceExpression("fare", DOUBLE), new Selection("fare", TABLE, Optional.empty()))
            .put(new VariableReferenceExpression("city", VARCHAR), new Selection("city", TABLE, Optional.empty()))
            .put(new VariableReferenceExpression("cityid", BIGINT), new Selection("cityId", TABLE, Optional.empty()))
            .put(new VariableReferenceExpression("zip", BIGINT), new Selection("zip", TABLE, Optional.empty()))
            .put(new VariableReferenceExpression("population", BIGINT), new Selection("population", TABLE, Optional.empty()))
            .put(new VariableReferenceExpression("secondssinceepoch", BIGINT), new Selection("secondsSinceEpoch", TABLE, Optional.empty()))
            .build();

    protected final TypeProvider typeProvider = TypeProvider.fromVariables(testInput.keySet());

    protected static class SessionHolder
    {
        private final ConnectorSession connectorSession;
        private final Session session;

        public SessionHolder(AresDbConfig aresDbConfig)
        {
            connectorSession = new TestingConnectorSession(new AresDbSessionProperties(aresDbConfig).getSessionProperties());
            session = TestingSession.testSessionBuilder(new SessionPropertyManager(new SystemSessionProperties().getSessionProperties())).build();
        }

        public SessionHolder(ConnectorSession connectorSession)
        {
            this.connectorSession = connectorSession;
            session = TestingSession.testSessionBuilder(new SessionPropertyManager(new SystemSessionProperties().getSessionProperties())).build();
        }

        public ConnectorSession getConnectorSession()
        {
            return connectorSession;
        }

        public Session getSession()
        {
            return session;
        }
    }

    protected VariableReferenceExpression v(String name)
    {
        return testInput.keySet().stream().filter(v -> v.getName().equals(name)).findFirst().orElseThrow(() -> new IllegalArgumentException("Cannot find variable " + name));
    }

    protected TableScanNode tableScan(PlanBuilder planBuilder, AresDbTableHandle connectorTableHandle, AresDbColumnHandle... columnHandles)
    {
        List<VariableReferenceExpression> variables = Arrays.stream(columnHandles).map(ch -> new VariableReferenceExpression(ch.getColumnName().toLowerCase(ENGLISH), ch.getDataType())).collect(toImmutableList());
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.builder();
        for (int i = 0; i < variables.size(); ++i) {
            assignments.put(variables.get(i), columnHandles[i]);
        }
        TableHandle tableHandle = new TableHandle(
                aresDbConnectorId,
                connectorTableHandle,
                TestingTransactionHandle.create(),
                Optional.empty());
        return planBuilder.tableScan(
                tableHandle,
                variables,
                assignments.build());
    }

    protected FilterNode filter(PlanBuilder planBuilder, PlanNode source, RowExpression predicate)
    {
        return planBuilder.filter(predicate, source);
    }

    protected ProjectNode project(PlanBuilder planBuilder, PlanNode source, List<String> columnNames)
    {
        Map<String, VariableReferenceExpression> incomingColumns = source.getOutputVariables().stream().collect(toMap(VariableReferenceExpression::getName, identity()));
        Assignments.Builder assignmentsBuilder = Assignments.builder();
        columnNames.forEach(columnName -> {
            VariableReferenceExpression variable = requireNonNull(incomingColumns.get(columnName), "Couldn't find the incoming column " + columnName);
            assignmentsBuilder.put(variable, variable);
        });
        return planBuilder.project(assignmentsBuilder.build(), source);
    }

    protected ProjectNode project(PlanBuilder planBuilder, PlanNode source, LinkedHashMap<String, String> toProject, SessionHolder sessionHolder)
    {
        Assignments.Builder assignmentsBuilder = Assignments.builder();
        toProject.forEach((columnName, expression) -> {
            RowExpression rowExpression = getRowExpression(expression, sessionHolder);
            VariableReferenceExpression variable = new VariableReferenceExpression(columnName, rowExpression.getType());
            assignmentsBuilder.put(variable, rowExpression);
        });
        return planBuilder.project(assignmentsBuilder.build(), source);
    }

    protected AggregationNode agg(PlanBuilder planBuilder, PlanNode source, boolean isPartial, SessionHolder sessionHolder)
    {
        GroupingSetDescriptor groupingSetDescriptor = new GroupingSetDescriptor(Collections.singletonList(v("regionid")), 1, new HashSet<Integer>());
        return planBuilder.aggregation(
                af -> af.step(isPartial ? PARTIAL : Step.FINAL)
                        .addAggregation(planBuilder.variable("fare_total", DOUBLE), getRowExpression("count(fare)", sessionHolder))
                        .source(source)
                        .groupingSets(groupingSetDescriptor));
    }

    public static Expression expression(String sql)
    {
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql, new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL)));
    }

    protected RowExpression toRowExpression(Expression expression, Session session)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                session,
                metadata,
                new SqlParser(),
                typeProvider,
                expression,
                ImmutableList.of(),
                WarningCollector.NOOP);
        return SqlToRowExpressionTranslator.translate(expression, expressionTypes, ImmutableMap.of(), functionMetadataManager, typeManager, session);
    }

    protected LimitNode limit(PlanBuilder pb, long count, PlanNode source)
    {
        return new LimitNode(pb.getIdAllocator().getNextId(), source, count, FINAL);
    }

    protected TopNNode topN(PlanBuilder pb, long count, List<String> orderingColumns, List<Boolean> ascending, PlanNode source)
    {
        ImmutableList<Ordering> ordering = IntStream.range(0, orderingColumns.size()).boxed().map(i -> new Ordering(v(orderingColumns.get(i)), ascending.get(i) ? SortOrder.ASC_NULLS_FIRST : SortOrder.DESC_NULLS_FIRST)).collect(toImmutableList());
        return new TopNNode(pb.getIdAllocator().getNextId(), source, count, new OrderingScheme(ordering), TopNNode.Step.SINGLE);
    }

    protected RowExpression getRowExpression(String sqlExpression, SessionHolder sessionHolder)
    {
        return toRowExpression(expression(sqlExpression), sessionHolder.getSession());
    }

    protected PlanBuilder createPlanBuilder(SessionHolder sessionHolder)
    {
        return new PlanBuilder(sessionHolder.getSession(), new PlanNodeIdAllocator(), metadata);
    }
}
