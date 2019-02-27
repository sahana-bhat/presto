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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.ExpressionExtractor.extractExpressionsNonRecursive;
import static com.facebook.presto.sql.planner.optimizations.MergeNestedColumn.prefixExists;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.tree.ExpressionTreeRewriter.rewriteWith;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class PushDownDereferenceExpression
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public PushDownDereferenceExpression(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlparser is null");
    }

    private static RowExpression replaceDereferences(RowExpression rowExpression, Map<DereferenceExpression, DereferenceInfo> dereferences)
    {
        return castToRowExpression(rewriteWith(new DereferenceReplacer(dereferences), castToExpression(rowExpression)));
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        Map<DereferenceExpression, DereferenceInfo> expressionInfoMap = new HashMap<>();
        return SimplePlanRewriter.rewriteWith(new Optimizer(session, metadata, sqlParser, variableAllocator, idAllocator, warningCollector), plan, expressionInfoMap);
    }

    private static class DereferenceReplacer
            extends ExpressionRewriter<Void>
    {
        private final Map<DereferenceExpression, DereferenceInfo> map;

        DereferenceReplacer(Map<DereferenceExpression, DereferenceInfo> map)
        {
            this.map = map;
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (map.containsKey(node) && map.get(node).isFromValidSource()) {
                return new SymbolReference(map.get(node).getVariable().getName());
            }
            return treeRewriter.defaultRewrite(node, context);
        }
    }

    private static class Optimizer
            extends SimplePlanRewriter<Map<DereferenceExpression, DereferenceInfo>>
    {
        private final Session session;
        private final SqlParser sqlParser;
        private final PlanVariableAllocator variableAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final WarningCollector warningCollector;

        private Optimizer(Session session, Metadata metadata, SqlParser sqlParser, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
        {
            this.session = session;
            this.sqlParser = sqlParser;
            this.metadata = metadata;
            this.variableAllocator = variableAllocator;
            this.idAllocator = idAllocator;
            this.warningCollector = warningCollector;
        }

        private static List<DereferenceExpression> extractDereferenceExpressions(Expression expression)
        {
            ImmutableList.Builder<DereferenceExpression> builder = ImmutableList.builder();
            new DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<DereferenceExpression>>()
            {
                @Override
                protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableList.Builder<DereferenceExpression> context)
                {
                    context.add(node);
                    return null;
                }
            }.process(expression, builder);
            return builder.build();
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Map<DereferenceExpression, DereferenceInfo>> context)
        {
            Map<DereferenceExpression, DereferenceInfo> expressionInfoMap = context.get();
            extractDereferenceInfos(node).forEach(expressionInfoMap::putIfAbsent);

            PlanNode child = context.rewrite(node.getSource(), expressionInfoMap);

            Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = new HashMap<>();
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> variableAggregationEntry : node.getAggregations().entrySet()) {
                AggregationNode.Aggregation oldAggregation = variableAggregationEntry.getValue();
                AggregationNode.Aggregation newAggregation = new AggregationNode.Aggregation(
                        new CallExpression(
                                oldAggregation.getCall().getDisplayName(),
                                oldAggregation.getCall().getFunctionHandle(),
                                oldAggregation.getCall().getType(),
                                oldAggregation.getCall().getArguments().stream()
                                        .map(arg -> replaceDereferences(arg, expressionInfoMap))
                                        .collect(Collectors.toList())),
                        oldAggregation.getFilter(),
                        oldAggregation.getOrderBy(),
                        oldAggregation.isDistinct(),
                        oldAggregation.getMask());
                aggregations.put(variableAggregationEntry.getKey(), newAggregation);
            }

            return new AggregationNode(
                    idAllocator.getNextId(),
                    child,
                    aggregations,
                    node.getGroupingSets(),
                    node.getPreGroupedVariables(),
                    node.getStep(),
                    node.getHashVariable(),
                    node.getGroupIdVariable());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Map<DereferenceExpression, DereferenceInfo>> context)
        {
            Map<DereferenceExpression, DereferenceInfo> expressionInfoMap = context.get();
            extractDereferenceInfos(node).forEach(expressionInfoMap::putIfAbsent);

            PlanNode child = context.rewrite(node.getSource(), expressionInfoMap);

            RowExpression predicate = replaceDereferences(node.getPredicate(), expressionInfoMap);
            return new FilterNode(idAllocator.getNextId(), child, predicate);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Map<DereferenceExpression, DereferenceInfo>> context)
        {
            Map<DereferenceExpression, DereferenceInfo> expressionInfoMap = context.get();
            //  parentDereferenceInfos is used to find out passThroughSymbol. we will only pass those symbols that are needed by upstream
            List<DereferenceInfo> parentDereferenceInfos = expressionInfoMap.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
            Map<DereferenceExpression, DereferenceInfo> newDereferences = extractDereferenceInfos(node);
            newDereferences.forEach(expressionInfoMap::putIfAbsent);

            PlanNode child = context.rewrite(node.getSource(), expressionInfoMap);

            List<VariableReferenceExpression> passThroughVariables = getUsedDereferenceInfo(node.getOutputVariables(), parentDereferenceInfos).stream()
                    .filter(DereferenceInfo::isFromValidSource)
                    .map(DereferenceInfo::getVariable)
                    .collect(Collectors.toList());

            Assignments.Builder assignmentsBuilder = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().entrySet()) {
                assignmentsBuilder.put(entry.getKey(), replaceDereferences(entry.getValue(), expressionInfoMap));
            }
            for (VariableReferenceExpression passThroughVariable : passThroughVariables) {
                assignmentsBuilder.put(passThroughVariable, castToRowExpression(new SymbolReference(passThroughVariable.getName())));
            }
            ProjectNode newProjectNode = new ProjectNode(idAllocator.getNextId(), child, assignmentsBuilder.build());
            newDereferences.forEach(expressionInfoMap::remove);
            return newProjectNode;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Map<DereferenceExpression, DereferenceInfo>> context)
        {
            Map<DereferenceExpression, DereferenceInfo> expressionInfoMap = context.get();
            List<DereferenceInfo> usedDereferenceInfo = getUsedDereferenceInfo(node.getOutputVariables(), expressionInfoMap.values());
            if (!usedDereferenceInfo.isEmpty()) {
                usedDereferenceInfo.forEach(DereferenceInfo::doesFromValidSource);
                Assignments.Builder assignmentsBuilder = Assignments.builder();
                for (DereferenceInfo dereferenceInfo : usedDereferenceInfo) {
                    assignmentsBuilder.put(dereferenceInfo.getVariable(), castToRowExpression(dereferenceInfo.getDereference()));
                }
                assignmentsBuilder.putAll(identityAssignmentsAsSymbolReferences(node.getOutputVariables()));
                return new ProjectNode(idAllocator.getNextId(), node, assignmentsBuilder.build());
            }
            return node;
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<Map<DereferenceExpression, DereferenceInfo>> context)
        {
            Map<DereferenceExpression, DereferenceInfo> expressionInfoMap = context.get();
            List<DereferenceInfo> usedDereferenceInfo = getUsedDereferenceInfo(node.getOutputVariables(), expressionInfoMap.values());
            if (!usedDereferenceInfo.isEmpty()) {
                usedDereferenceInfo.forEach(DereferenceInfo::doesFromValidSource);
                Assignments.Builder assignmentsBuilder = Assignments.builder();
                for (DereferenceInfo dereferenceInfo : usedDereferenceInfo) {
                    assignmentsBuilder.put(dereferenceInfo.getVariable(), castToRowExpression(dereferenceInfo.getDereference()));
                }
                assignmentsBuilder.putAll(identityAssignmentsAsSymbolReferences(node.getOutputVariables()));
                return new ProjectNode(idAllocator.getNextId(), node, assignmentsBuilder.build());
            }
            return node;
        }

        @Override
        public PlanNode visitJoin(JoinNode joinNode, RewriteContext<Map<DereferenceExpression, DereferenceInfo>> context)
        {
            Map<DereferenceExpression, DereferenceInfo> expressionInfoMap = context.get();
            extractDereferenceInfos(joinNode).forEach(expressionInfoMap::putIfAbsent);

            PlanNode leftNode = context.rewrite(joinNode.getLeft(), expressionInfoMap);
            PlanNode rightNode = context.rewrite(joinNode.getRight(), expressionInfoMap);

            Optional<RowExpression> joinFilter = joinNode.getFilter().map(expression -> replaceDereferences(expression, expressionInfoMap));

            return new JoinNode(
                    joinNode.getId(),
                    joinNode.getType(),
                    leftNode,
                    rightNode,
                    joinNode.getCriteria(),
                    ImmutableList.<VariableReferenceExpression>builder().addAll(leftNode.getOutputVariables()).addAll(rightNode.getOutputVariables()).build(),
                    joinFilter,
                    joinNode.getLeftHashVariable(),
                    joinNode.getRightHashVariable(),
                    joinNode.getDistributionType());
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Map<DereferenceExpression, DereferenceInfo>> context)
        {
            Map<DereferenceExpression, DereferenceInfo> expressionInfoMap = context.get();
            List<DereferenceInfo> parentDereferenceInfos = expressionInfoMap.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());

            PlanNode child = context.rewrite(node.getSource(), expressionInfoMap);

            List<VariableReferenceExpression> passThroughSymbols = getUsedDereferenceInfo(child.getOutputVariables(), parentDereferenceInfos).stream()
                    .filter(DereferenceInfo::isFromValidSource)
                    .map(DereferenceInfo::getVariable)
                    .collect(Collectors.toList());

            UnnestNode unnestNode = new UnnestNode(
                    idAllocator.getNextId(),
                    child,
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(node.getReplicateVariables())
                            .addAll(passThroughSymbols).build(),
                    node.getUnnestVariables(),
                    node.getOrdinalityVariable());

            List<VariableReferenceExpression> unnestSymbols = unnestNode.getUnnestVariables().entrySet().stream()
                    .flatMap(entry -> entry.getValue().stream())
                    .collect(Collectors.toList());

            List<DereferenceInfo> dereferenceExpressionInfos = getUsedDereferenceInfo(unnestSymbols, expressionInfoMap.values());
            if (!dereferenceExpressionInfos.isEmpty()) {
                dereferenceExpressionInfos.forEach(DereferenceInfo::doesFromValidSource);
                Assignments.Builder assignmentsBuilder = Assignments.builder();
                for (DereferenceInfo dereferenceInfo : dereferenceExpressionInfos) {
                    assignmentsBuilder.put(dereferenceInfo.getVariable(), castToRowExpression(dereferenceInfo.getDereference()));
                }
                assignmentsBuilder.putAll(identityAssignmentsAsSymbolReferences(unnestNode.getOutputVariables()));
                return new ProjectNode(idAllocator.getNextId(), unnestNode, assignmentsBuilder.build());
            }
            return unnestNode;
        }

        private List<DereferenceInfo> getUsedDereferenceInfo(List<VariableReferenceExpression> variables, Collection<DereferenceInfo> dereferenceExpressionInfos)
        {
            Set<VariableReferenceExpression> variableSet = variables.stream().collect(Collectors.toSet());
            return dereferenceExpressionInfos.stream()
                    .filter(dereferenceExpressionInfo -> variableSet.contains(dereferenceExpressionInfo.getBaseVariable()))
                    .collect(Collectors.toList());
        }

        private DereferenceInfo getDereferenceInfo(DereferenceExpression expression)
        {
            VariableReferenceExpression variable = newSymbol(expression);
            VariableReferenceExpression base = Iterables.getOnlyElement(VariablesExtractor.extractAll(expression, variableAllocator.getTypes()));
            return new DereferenceInfo(expression, variable, base);
        }

        private VariableReferenceExpression newSymbol(Expression expression)
        {
            Type type = getExpressionTypes(session, metadata, sqlParser, variableAllocator.getTypes(), expression, emptyList(), WarningCollector.NOOP).get(NodeRef.of(expression));
            verify(type != null);
            return variableAllocator.newVariable(expression, type);
        }

        private Map<DereferenceExpression, DereferenceInfo> extractDereferenceInfos(PlanNode node)
        {
            Collection<RowExpression> rowExpressions = extractExpressionsNonRecursive(node);
            Set<Expression> dereferences = rowExpressions.stream()
                    .filter(OriginalExpressionUtils::isExpression)
                    .map(OriginalExpressionUtils::castToExpression)
                    .flatMap(expression -> extractDereferenceExpressions(expression).stream())
                    .map(MergeNestedColumn::validDereferenceExpression)
                    .filter(Objects::nonNull)
                    .collect(toImmutableSet());

            return dereferences.stream()
                    .filter(expression -> !prefixExists(expression, dereferences))
                    .filter(expression -> expression instanceof DereferenceExpression)
                    .map(expression -> ((DereferenceExpression) expression))
                    .distinct()
                    .map(this::getDereferenceInfo)
                    .collect(Collectors.toMap(DereferenceInfo::getDereference, Function.identity()));
        }
    }

    private static class DereferenceInfo
    {
        // e.g. for dereference expression msg.foo[1].bar, base is "msg", newSymbol is new assigned symbol to replace this dereference expression
        private final DereferenceExpression dereferenceExpression;
        private final VariableReferenceExpression variable;
        private final VariableReferenceExpression baseVariable;

        // fromValidSource is used to check whether the dereference expression is from either TableScan or Unnest
        // it will be false for following node therefore we won't rewrite:
        // Project[expr_1 := "max_by"."field1"]
        // - Aggregate[max_by := "max_by"("expr", "app_rating")] => [max_by:row(field0 varchar, field1 varchar)]
        private boolean fromValidSource;

        public DereferenceInfo(DereferenceExpression dereferenceExpression, VariableReferenceExpression variable, VariableReferenceExpression baseVariable)
        {
            this.dereferenceExpression = requireNonNull(dereferenceExpression);
            this.variable = requireNonNull(variable);
            this.baseVariable = requireNonNull(baseVariable);
            this.fromValidSource = false;
        }

        public VariableReferenceExpression getVariable()
        {
            return variable;
        }

        public VariableReferenceExpression getBaseVariable()
        {
            return baseVariable;
        }

        public DereferenceExpression getDereference()
        {
            return dereferenceExpression;
        }

        public boolean isFromValidSource()
        {
            return fromValidSource;
        }

        public void doesFromValidSource()
        {
            fromValidSource = true;
        }

        @Override
        public String toString()
        {
            return String.format("(%s, %s, %s)", dereferenceExpression, variable, baseVariable);
        }
    }
}
