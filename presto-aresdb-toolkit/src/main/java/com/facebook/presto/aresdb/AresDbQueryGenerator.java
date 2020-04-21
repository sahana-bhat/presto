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
import com.facebook.presto.aresdb.AresDbUtils.AggregationColumnNode;
import com.facebook.presto.aresdb.AresDbUtils.AggregationFunctionColumnNode;
import com.facebook.presto.aresdb.AresDbUtils.GroupByColumnNode;
import com.facebook.presto.aresdb.query.AQLExpression;
import com.facebook.presto.aresdb.query.AresDbExpression;
import com.facebook.presto.aresdb.query.AresDbFilterExpressionConverter;
import com.facebook.presto.aresdb.query.AresDbProjectExpressionConverter;
import com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_FINAL_AGGREGATION_TOO_BIG_ERROR;
import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.aresdb.AresDbUtils.checkSupported;
import static com.facebook.presto.aresdb.AresDbUtils.computeAggregationNodes;
import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Origin.DERIVED;
import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Origin.LITERAL;
import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Origin.TABLE;
import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Selection;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AresDbQueryGenerator
{
    private static final Logger log = Logger.get(AresDbQueryGenerator.class);
    private final LogicalRowExpressions logicalRowExpressions;
    private final AresDbFilterExpressionConverter aresDbFilterExpressionConverter;
    private final AresDbProjectExpressionConverter aresDbProjectExpressionConverter;
    private final DomainTranslator domainTranslator;
    private static Map<String, String> unaryAggregationMap = ImmutableMap.of(
            "min", "min",
            "max", "max",
            "avg", "avg",
            "sum", "sum",
            "approx_distinct", "countdistincthll");
    private static final Set<String> notSupportedPartialAggregationFunctions = ImmutableSet.of("approx_distinct", "avg");

    @Inject
    public AresDbQueryGenerator(TypeManager typeManager, FunctionMetadataManager functionMetadataManager, StandardFunctionResolution standardFunctionResolution, LogicalRowExpressions logicalRowExpressions, RowExpressionService rowExpressionService)
    {
        this.logicalRowExpressions = requireNonNull(logicalRowExpressions, "logicalRowExpressions is null");
        this.aresDbFilterExpressionConverter = new AresDbFilterExpressionConverter(typeManager, functionMetadataManager, standardFunctionResolution);
        this.aresDbProjectExpressionConverter = new AresDbProjectExpressionConverter(typeManager, functionMetadataManager, standardFunctionResolution);
        this.domainTranslator = requireNonNull(rowExpressionService, "rowExpressionService is null").getDomainTranslator();
    }

    public static class AresDbQueryGeneratorResult
    {
        private final AugmentedAQL generatedAql;
        private final AresDbQueryGeneratorContext context;
        private final boolean isQueryShort;

        @JsonCreator
        public AresDbQueryGeneratorResult(
                @JsonProperty("augmentedAql") AugmentedAQL generatedAql,
                @JsonProperty("context") AresDbQueryGeneratorContext context,
                @JsonProperty("queryShort") boolean isQueryShort)
        {
            this.generatedAql = requireNonNull(generatedAql, "generatedPql is null");
            this.context = requireNonNull(context, "context is null");
            this.isQueryShort = isQueryShort;
        }

        @JsonProperty
        public AugmentedAQL getGeneratedAql()
        {
            return generatedAql;
        }

        @JsonProperty
        public AresDbQueryGeneratorContext getContext()
        {
            return context;
        }

        @JsonProperty
        public boolean isQueryShort()
        {
            return isQueryShort;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("generatedAql", generatedAql)
                    .add("context", context)
                    .add("isQueryShort", isQueryShort)
                    .toString();
        }
    }

    public Optional<AresDbQueryGeneratorResult> generate(PlanNode plan, ConnectorSession session)
    {
        try {
            AresDbQueryGeneratorContext context = requireNonNull(plan.accept(new AresDbQueryPlanVisitor(session), new AresDbQueryGeneratorContext()), "Resulting context is null");
            boolean isQueryShort = context.isQueryShort(AresDbSessionProperties.getMaxLimitWithoutAggregates(session));
            return Optional.of(new AresDbQueryGeneratorResult(context.toQuery(Optional.of(session), aresDbFilterExpressionConverter, domainTranslator), context, isQueryShort));
        }
        catch (AresDbException e) {
            log.debug(e, "Possibly benign error when pushing plan into scan node %s", plan);
            return Optional.empty();
        }
    }

    private static boolean isPartialAggregationNotSupported(AggregationFunctionColumnNode aggregation)
    {
        return notSupportedPartialAggregationFunctions.contains(aggregation.getCallExpression().getDisplayName().toLowerCase(ENGLISH));
    }

    public static class AugmentedAQL
    {
        final String aql;
        final List<AQLExpression> expressions;

        @JsonCreator
        public AugmentedAQL(
                @JsonProperty("aql") String aql,
                @JsonProperty("expressions") List<AQLExpression> expressions)
        {
            this.aql = aql;
            this.expressions = ImmutableList.copyOf(expressions);
        }

        @JsonProperty
        public String getAql()
        {
            return aql;
        }

        @JsonProperty
        public List<AQLExpression> getExpressions()
        {
            return expressions;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("aql", aql)
                    .add("expressions", expressions)
                    .toString();
        }
    }

    public static boolean isBooleanTrue(RowExpression expression)
    {
        if (!(expression instanceof ConstantExpression)) {
            return false;
        }
        ConstantExpression constantExpression = (ConstantExpression) expression;
        if (constantExpression.getValue() == null || !(constantExpression.getType() instanceof BooleanType)) {
            return false;
        }
        return Boolean.parseBoolean(String.valueOf(((ConstantExpression) expression).getValue()));
    }

    class AresDbQueryPlanVisitor
            extends PlanVisitor<AresDbQueryGeneratorContext, AresDbQueryGeneratorContext>
    {
        //TODO visitJoin needs to be implemented
        private final ConnectorSession session;

        protected AresDbQueryPlanVisitor(ConnectorSession session)
        {
            this.session = session;
        }
        @Override
        public AresDbQueryGeneratorContext visitPlan(PlanNode node, AresDbQueryGeneratorContext context)
        {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, "Don't know how to handle plan node of type " + node);
        }

        @Override
        public AresDbQueryGeneratorContext visitTableScan(TableScanNode node, AresDbQueryGeneratorContext contextIn)
        {
            AresDbTableHandle tableHandle = (AresDbTableHandle) node.getTable().getConnectorHandle();
            checkSupported(!tableHandle.getGeneratedResult().isPresent(), "Expect to see no existing query generator result");
            checkSupported(!tableHandle.getIsQueryShort().isPresent(), "Expect to see no existing Aql");
            LinkedHashMap<VariableReferenceExpression, Selection> selections = new LinkedHashMap<>();
            node.getOutputVariables().forEach(outputColumn -> {
                AresDbColumnHandle aresDbColumn = (AresDbColumnHandle) (node.getAssignments().get(outputColumn));
                checkSupported(aresDbColumn.getType().equals(AresDbColumnHandle.AresDbColumnType.REGULAR), "Unexpected aresDb column handle that is not regular: %s", aresDbColumn);
                selections.put(outputColumn, new Selection(aresDbColumn.getColumnName(), TABLE, Optional.empty()));
            });
            TupleDomain<String> constraints = node.getCurrentConstraint().transform(col -> ((AresDbColumnHandle) col).getColumnName());
            return new AresDbQueryGeneratorContext(selections, Optional.of(tableHandle), Optional.of(constraints));
        }

        @Override
        public AresDbQueryGeneratorContext visitProject(ProjectNode node, AresDbQueryGeneratorContext contextIn)
        {
            AresDbQueryGeneratorContext context = node.getSource().accept(this, contextIn);
            requireNonNull(context, "context is null");
            LinkedHashMap<VariableReferenceExpression, Selection> newSelections = new LinkedHashMap<>();
            // TODO Like Pinot, does this need to tc of variables that are in aggregation to be handled differently?
            node.getOutputVariables().forEach(variable -> {
                RowExpression expression = node.getAssignments().get(variable);
                AresDbExpression aresDbExpression = expression.accept(aresDbProjectExpressionConverter, context.getSelections());
                newSelections.put(
                        variable,
                        new Selection(aresDbExpression.getDefinition(), aresDbExpression.getOrigin(), aresDbExpression.getTimeBucketizer()));
            });
            return context.withProject(newSelections);
        }

        @Override
        public AresDbQueryGeneratorContext visitFilter(FilterNode node, AresDbQueryGeneratorContext context)
        {
            context = node.getSource().accept(this, context);
            requireNonNull(context, "context is null");
            Optional<Domain> timeFilterMap = Optional.empty();
            Optional<RowExpression> predicate = Optional.of(node.getPredicate());

            DomainTranslator.ExtractionResult<VariableReferenceExpression> extractionResult = domainTranslator.fromPredicate(session, node.getPredicate(), DomainTranslator.BASIC_COLUMN_EXTRACTOR);

            if (context.getTimeColumn().isPresent() && context.getCurrentConstraints().isPresent()) {
                String timeColumn = context.getTimeColumn().get().toLowerCase(ENGLISH);
                Set<Entry<VariableReferenceExpression, Domain>> tempDomainMap = extractionResult.getTupleDomain().getDomains().get().entrySet();
                HashMap<VariableReferenceExpression, Domain> domainMap = (HashMap<VariableReferenceExpression, Domain>) tempDomainMap.stream()
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
                VariableReferenceExpression timeExpression = null;
                for (Entry<VariableReferenceExpression, Domain> entry : domainMap.entrySet()) {
                    if (entry.getKey().getName().toLowerCase(ENGLISH).equals(timeColumn)) {
                        timeExpression = entry.getKey();
                        break;
                    }
                }
                if (timeExpression != null) {
                    timeFilterMap = Optional.of(domainMap.remove(timeExpression));
                    ImmutableList.Builder<RowExpression> remainingPredicateBuilder = ImmutableList.builder();
                    if (extractionResult.getRemainingExpression() != null && !isBooleanTrue(extractionResult.getRemainingExpression())) {
                        remainingPredicateBuilder.add(extractionResult.getRemainingExpression());
                    }
                    //TODO using the toPredicate from RowExpressionDomainTranslator instead of custom methods.
                    remainingPredicateBuilder.add(domainTranslator.toPredicate(TupleDomain.withColumnDomains(domainMap)));
                    predicate = Optional.of(logicalRowExpressions.combineConjuncts(remainingPredicateBuilder.build()));
                }
            }
            List<String> filters;
            if (!predicate.isPresent() || isBooleanTrue(predicate.get())) {
                filters = ImmutableList.of();
            }
            else {
                String filterPredicate = predicate.get().accept(aresDbFilterExpressionConverter, context.getSelections()::get).getDefinition();
                if (filterPredicate == null) {
                    throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, String.format("Cannot convert the filter %s", predicate));
                }
                filters = ImmutableList.of(filterPredicate);
            }
            return context.withFilters(filters, timeFilterMap).withOutputColumns(node.getOutputVariables());
        }

        private String handleAggregationFunction(CallExpression aggregation, Map<VariableReferenceExpression, Selection> inputSelections)
        {
            String prestoAgg = aggregation.getDisplayName().toLowerCase(ENGLISH);
            List<RowExpression> params = aggregation.getArguments();
            switch (prestoAgg) {
                case "count":
                    if (params.size() <= 1) {
                        return format("count(%s)", params.isEmpty() ? "*" : inputSelections.get(getVariableReference(params.get(0))).getDefinition());
                    }
                    break;
                default:
                    if (unaryAggregationMap.containsKey(prestoAgg) && aggregation.getArguments().size() == 1) {
                        return format("%s(%s)", unaryAggregationMap.get(prestoAgg), inputSelections.get(getVariableReference(params.get(0))).getDefinition());
                    }
            }

            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, format("aggregation function '%s' not supported yet", aggregation));
        }

        @Override
        public AresDbQueryGeneratorContext visitAggregation(AggregationNode node, AresDbQueryGeneratorContext contextIn)
        {
            List<AggregationColumnNode> aggregationColumnNodes = computeAggregationNodes(node);

            // Make two passes over the aggregatinColumnNodes: In the first pass identify all the variables that will be used
            // Then pass that context to the source
            // And finally, in the second pass actually generate the AQL

            // 1st pass
            Set<VariableReferenceExpression> variablesInAggregation = new HashSet<>();
            for (AggregationColumnNode expression : aggregationColumnNodes) {
                switch (expression.getExpressionType()) {
                    case GROUP_BY: {
                        GroupByColumnNode groupByColumn = (GroupByColumnNode) expression;
                        VariableReferenceExpression groupByInputColumn = getVariableReference(groupByColumn.getInputColumn());
                        variablesInAggregation.add(groupByInputColumn);
                        break;
                    }
                    case AGGREGATE: {
                        AggregationFunctionColumnNode aggregationNode = (AggregationFunctionColumnNode) expression;
                        variablesInAggregation.addAll(
                                aggregationNode
                                        .getCallExpression()
                                        .getArguments()
                                        .stream()
                                        .filter(argument -> argument instanceof VariableReferenceExpression)
                                        .map(argument -> (VariableReferenceExpression) argument)
                                        .collect(Collectors.toList()));
                        break;
                    }
                    default:
                        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, "unknown aggregation expression: " + expression.getExpressionType());
                }
            }

            // now visit the child project node
            AresDbQueryGeneratorContext context = node.getSource().accept(this, contextIn.withVariablesInAggregation(variablesInAggregation));
            requireNonNull(context, "context is null");

            // 2nd pass
            LinkedHashMap<VariableReferenceExpression, Selection> newSelections = new LinkedHashMap<>();
            LinkedHashSet<VariableReferenceExpression> groupByColumns = new LinkedHashSet<>();
            Set<VariableReferenceExpression> hiddenColumnSet = new HashSet<>(context.getHiddenColumnSet());
            int aggregations = 0;
            boolean groupByExists = false;
            boolean partialAggregationSupported = true;

            for (AggregationColumnNode expression : aggregationColumnNodes) {
                switch (expression.getExpressionType()) {
                    case GROUP_BY: {
                        GroupByColumnNode groupByColumn = (GroupByColumnNode) expression;
                        VariableReferenceExpression groupByInputColumn = getVariableReference(groupByColumn.getInputColumn());
                        VariableReferenceExpression outputColumn = getVariableReference(groupByColumn.getOutputColumn());
                        Selection aresDbColumn = requireNonNull(context.getSelections().get(groupByInputColumn), "Group By column " + groupByInputColumn + " doesn't exist in input " + context.getSelections());

                        newSelections.put(outputColumn, new Selection(aresDbColumn.getDefinition(), aresDbColumn.getOrigin(), aresDbColumn.getTimeTokenizer()));
                        groupByColumns.add(outputColumn);
                        groupByExists = true;
                        break;
                    }
                    case AGGREGATE: {
                        AggregationFunctionColumnNode aggregationNode = (AggregationFunctionColumnNode) expression;
                        if (isPartialAggregationNotSupported(aggregationNode)) {
                            partialAggregationSupported = false;
                        }
                        String aresDbAggFunction = handleAggregationFunction(aggregationNode.getCallExpression(), context.getSelections());
                        newSelections.put(getVariableReference(aggregationNode.getOutputColumn()), new Selection(aresDbAggFunction, DERIVED, Optional.empty()));
                        aggregations++;
                        break;
                    }
                    default:
                        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, "unknown aggregation expression: " + expression.getExpressionType());
                }
            }

            // Handling non-aggregated group by
            if (groupByExists && aggregations == 0) {
                partialAggregationSupported = !notSupportedPartialAggregationFunctions.contains("count");
                VariableReferenceExpression hidden = new VariableReferenceExpression(getNewUniqueSymbol(), BigintType.BIGINT);
                newSelections.put(hidden, Selection.of("count(*)", DERIVED, Optional.empty()));
                hiddenColumnSet.add(hidden);
                aggregations++;
            }

            // Handling non-group-by aggregation
            if (!groupByExists) {
                VariableReferenceExpression hidden = new VariableReferenceExpression(getNewUniqueSymbol(), IntegerType.INTEGER);
                Selection selection = Selection.of("1", LITERAL, Optional.empty());
                context.getSelections().put(hidden, selection);
                groupByColumns.add(hidden);
                newSelections.put(hidden, selection);
                hiddenColumnSet.add(hidden);
                groupByExists = true;
            }

            if (aggregations > 1 && groupByExists) {
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "AresDB doesn't support multiple aggregations with GROUP BY in a query");
            }
            if (!node.getStep().isOutputPartial() && context.isInputTooBig(Optional.of(session), domainTranslator) && partialAggregationSupported) {
                throw new AresDbException(ARESDB_FINAL_AGGREGATION_TOO_BIG_ERROR, "Partial aggregation preferred in favor of final because input is too big");
            }
            return context.withAggregation(newSelections, groupByColumns, hiddenColumnSet, node.getStep().isOutputPartial());
        }

        protected VariableReferenceExpression getVariableReference(RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression) {
                return ((VariableReferenceExpression) expression);
            }
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, "Expected a variable reference but got " + expression);
        }

        private String getNewUniqueSymbol()
        {
            return UUID.randomUUID().toString();
        }

        @Override
        public AresDbQueryGeneratorContext visitLimit(LimitNode node, AresDbQueryGeneratorContext context)
        {
            context = node.getSource().accept(this, context);
            requireNonNull(context, "context is null");
            return context.withLimit(OptionalInt.of((int) node.getCount())).withOutputColumns(node.getOutputVariables());
        }
    }
}
