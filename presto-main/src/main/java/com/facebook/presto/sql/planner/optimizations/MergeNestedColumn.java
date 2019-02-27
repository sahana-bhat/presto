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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.NestedColumn;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class MergeNestedColumn
        implements PlanOptimizer
{
    Metadata metadata;
    SqlParser sqlParser;

    public MergeNestedColumn(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    public static boolean prefixExists(Expression expression, Set<Expression> allExpressions)
    {
        int[] referenceCount = {0};
        new DefaultExpressionTraversalVisitor<Void, int[]>()
        {
            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, int[] referenceCount)
            {
                if (allExpressions.contains(node)) {
                    referenceCount[0] += 1;
                }
                process(node.getBase(), referenceCount);
                return null;
            }

            @Override
            protected Void visitSymbolReference(SymbolReference node, int[] context)
            {
                if (allExpressions.contains(node)) {
                    referenceCount[0] += 1;
                }
                return null;
            }
        }.process(expression, referenceCount);
        return referenceCount[0] > 1;
    }

    public static Expression validDereferenceExpression(Expression expression)
    {
        SubscriptExpression[] shortestSubscriptExp = new SubscriptExpression[1];
        boolean[] valid = new boolean[1];
        valid[0] = true;
        new DefaultExpressionTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitSubscriptExpression(SubscriptExpression node, Void context)
            {
                shortestSubscriptExp[0] = node;
                process(node.getBase(), context);
                return null;
            }

            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
            {
                valid[0] &= (node.getBase() instanceof SymbolReference || node.getBase() instanceof DereferenceExpression || node.getBase() instanceof SubscriptExpression);
                process(node.getBase(), context);
                return null;
            }
        }.process(expression, null);
        if (valid[0]) {
            return shortestSubscriptExp[0] == null ? expression : shortestSubscriptExp[0].getBase();
        }

        return null;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (SystemSessionProperties.isNestedColumnPushdown(session)) {
            return SimplePlanRewriter.rewriteWith(new Optimizer(session, types, variableAllocator, idAllocator, metadata, sqlParser, warningCollector), plan);
        }
        return plan;
    }

    // expression: msg_12.foo -> nestedColumn: msg.foo -> expression: msg_12.foo

    private static class Optimizer
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final TypeProvider typeProvider;
        private final VariableAllocator variableAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final SqlParser sqlParser;
        private final WarningCollector warningCollector;

        public Optimizer(Session session, TypeProvider typeProvider, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, SqlParser sqlParser, WarningCollector warningCollector)
        {
            this.session = requireNonNull(session);
            this.typeProvider = requireNonNull(typeProvider);
            this.variableAllocator = requireNonNull(variableAllocator);
            this.idAllocator = requireNonNull(idAllocator);
            this.metadata = requireNonNull(metadata);
            this.sqlParser = requireNonNull(sqlParser);
            this.warningCollector = requireNonNull(warningCollector);
        }

        private static RowExpression replaceDereferences(RowExpression rowExpression, Map<Expression, VariableReferenceExpression> dereferences)
        {
            return castToRowExpression(ExpressionTreeRewriter.rewriteWith(new Rewriter(dereferences), castToExpression(rowExpression)));
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            if (node.getSource() instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) node.getSource();
                return mergeProjectWithTableScan(node, tableScanNode, context);
            }
            return context.defaultRewrite(node);
        }

        private Type extractType(Expression expression)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = ExpressionAnalyzer.getExpressionTypes(session, metadata, sqlParser, typeProvider, expression, emptyList(), warningCollector);
            return expressionTypes.get(NodeRef.of(expression));
        }

        public PlanNode mergeProjectWithTableScan(ProjectNode node, TableScanNode tableScanNode, RewriteContext<Void> context)
        {
            Set<Expression> allExpressions = node.getAssignments().getExpressions().stream()
                    .filter(OriginalExpressionUtils::isExpression)
                    .map(OriginalExpressionUtils::castToExpression)
                    .map(MergeNestedColumn::validDereferenceExpression)
                    .filter(Objects::nonNull)
                    .collect(toImmutableSet());

            Set<Expression> dereferences = allExpressions.stream()
                    .filter(expression -> !prefixExists(expression, allExpressions))
                    .filter(expression -> expression instanceof DereferenceExpression)
                    .collect(toImmutableSet());

            if (dereferences.isEmpty()) {
                return context.defaultRewrite(node);
            }

            NestedColumnTranslator nestedColumnTranslator = new NestedColumnTranslator(tableScanNode.getAssignments(), tableScanNode.getTable());
            Map<Expression, NestedColumn> nestedColumns = dereferences.stream().collect(Collectors.toMap(Function.identity(), nestedColumnTranslator::toNestedColumn));

            Map<NestedColumn, ColumnHandle> nestedColumnHandles =
                    metadata.getNestedColumnHandles(session, tableScanNode.getTable(), nestedColumns.values())
                            .entrySet().stream()
                            .filter(entry -> !nestedColumnTranslator.columnHandleExists(entry.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (nestedColumnHandles.isEmpty()) {
                return context.defaultRewrite(node);
            }

            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> columnHandleBuilder = ImmutableMap.builder();
            columnHandleBuilder.putAll(tableScanNode.getAssignments());

            // Use to replace expression in original dereference expression
            ImmutableMap.Builder<Expression, VariableReferenceExpression> symbolExpressionBuilder = ImmutableMap.builder();
            for (Map.Entry<NestedColumn, ColumnHandle> entry : nestedColumnHandles.entrySet()) {
                NestedColumn nestedColumn = entry.getKey();
                Expression expression = nestedColumnTranslator.toExpression(nestedColumn);
                VariableReferenceExpression symbol = variableAllocator.newVariable(nestedColumn.getName(), extractType(expression));
                symbolExpressionBuilder.put(expression, symbol);
                columnHandleBuilder.put(symbol, entry.getValue());
            }
            Map<Expression, VariableReferenceExpression> symbolExpression = symbolExpressionBuilder.build();
            ImmutableMap<VariableReferenceExpression, ColumnHandle> nestedColumnsMap = columnHandleBuilder.build();

            TableScanNode newTableScan = new TableScanNode(idAllocator.getNextId(), tableScanNode.getTable(), ImmutableList.copyOf(nestedColumnsMap.keySet()), nestedColumnsMap,
                    tableScanNode.getCurrentConstraint(), tableScanNode.getEnforcedConstraint());

            Assignments.Builder assignmentBuilder = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().entrySet()) {
                assignmentBuilder.put(entry.getKey(), replaceDereferences(entry.getValue(), symbolExpression));
            }

            return new ProjectNode(idAllocator.getNextId(), newTableScan, assignmentBuilder.build());
        }

        private class NestedColumnTranslator
        {
            private final Map<SymbolReference, String> symbolToColumnName;
            private final Map<String, SymbolReference> columnNameToSymbol;

            NestedColumnTranslator(Map<VariableReferenceExpression, ColumnHandle> columnHandleMap, TableHandle tableHandle)
            {
                BiMap<SymbolReference, String> symbolToColumnName = HashBiMap.create(columnHandleMap.entrySet().stream()
                        .collect(Collectors.toMap(
                                entry -> new SymbolReference(entry.getKey().getName()),
                                entry -> metadata.getColumnMetadata(session, tableHandle, entry.getValue()).getName())));
                this.symbolToColumnName = symbolToColumnName;
                this.columnNameToSymbol = symbolToColumnName.inverse();
            }

            boolean columnHandleExists(NestedColumn nestedColumn)
            {
                return columnNameToSymbol.containsKey(nestedColumn.getName());
            }

            NestedColumn toNestedColumn(Expression expression)
            {
                ImmutableList.Builder<String> builder = ImmutableList.builder();
                new DefaultExpressionTraversalVisitor<Void, Void>()
                {
                    @Override
                    protected Void visitSubscriptExpression(SubscriptExpression node, Void context)
                    {
                        return null;
                    }

                    @Override
                    protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
                    {
                        process(node.getBase(), context);
                        builder.add(node.getField().getValue());
                        return null;
                    }

                    @Override
                    protected Void visitSymbolReference(SymbolReference node, Void context)
                    {
                        Preconditions.checkArgument(symbolToColumnName.containsKey(node), "base [%s] doesn't exist in assignments [%s]", node, symbolToColumnName);
                        builder.add(symbolToColumnName.get(node));
                        return null;
                    }
                }.process(expression, null);
                List<String> names = builder.build();
                Preconditions.checkArgument(names.size() > 1, "names size is less than 0", names);
                return new NestedColumn(names);
            }

            Expression toExpression(NestedColumn nestedColumn)
            {
                Expression result = null;
                for (String part : nestedColumn.getNames()) {
                    if (result == null) {
                        Preconditions.checkArgument(columnNameToSymbol.containsKey(part), "element %s doesn't exist in map %s", part, columnNameToSymbol);
                        result = new SymbolReference(columnNameToSymbol.get(part).getName());
                    }
                    else {
                        result = new DereferenceExpression(result, new Identifier(part));
                    }
                }
                return result;
            }
        }
    }

    private static class Rewriter
            extends ExpressionRewriter<Void>
    {
        private final Map<Expression, VariableReferenceExpression> map;

        Rewriter(Map<Expression, VariableReferenceExpression> map)
        {
            this.map = map;
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (map.containsKey(node)) {
                return new SymbolReference(map.get(node).getName());
            }
            return treeRewriter.defaultRewrite(node, context);
        }

        @Override
        public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (map.containsKey(node)) {
                return new SymbolReference(map.get(node).getName());
            }
            return super.rewriteSymbolReference(node, context, treeRewriter);
        }
    }
}
