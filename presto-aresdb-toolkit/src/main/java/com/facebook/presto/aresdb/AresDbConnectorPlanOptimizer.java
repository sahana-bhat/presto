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

import com.facebook.presto.aresdb.AresDbQueryGenerator.AresDbQueryGeneratorResult;
import com.facebook.presto.aresdb.query.AresDbFilterExpressionConverter;
import com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNCLASSIFIED_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class AresDbConnectorPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private final AresDbQueryGenerator aresDbQueryGenerator;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final LogicalRowExpressions logicalRowExpressions;
    private final StandardFunctionResolution standardFunctionResolution;

    @Inject
    public AresDbConnectorPlanOptimizer(AresDbQueryGenerator aresDbQueryGenerator, TypeManager typeManager, FunctionMetadataManager functionMetadataManager, LogicalRowExpressions logicalRowExpressions, StandardFunctionResolution standardFunctionResolution)
    {
        this.aresDbQueryGenerator = requireNonNull(aresDbQueryGenerator, "aresDbQueryGenerator is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.logicalRowExpressions = requireNonNull(logicalRowExpressions, "logicalRowExpressions is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
    }

    private static class TableFindingVisitor
            extends PlanVisitor<Map<TableScanNode, Void>, Void>
    {
        @Override
        public Map<TableScanNode, Void> visitPlan(PlanNode node, Void context)
        {
            Map<TableScanNode, Void> ret = new IdentityHashMap<>();
            node.getSources().forEach(source -> ret.putAll(source.accept(this, context)));
            return ret;
        }

        @Override
        public Map<TableScanNode, Void> visitTableScan(TableScanNode node, Void context)
        {
            Map<TableScanNode, Void> ret = new IdentityHashMap<>();
            ret.put(node, null);
            return ret;
        }
    }

    private static Optional<AresDbTableHandle> getAresDbTableHandle(TableScanNode tableScanNode)
    {
        TableHandle table = tableScanNode.getTable();
        if (table != null) {
            ConnectorTableHandle connectorHandle = table.getConnectorHandle();
            if (connectorHandle instanceof AresDbTableHandle) {
                return Optional.of((AresDbTableHandle) connectorHandle);
            }
        }
        return Optional.empty();
    }

    private static Optional<TableScanNode> getOnlyAresDbTable(Map<TableScanNode, Void> scanNodes)
    {
        if (scanNodes.size() == 1) {
            TableScanNode tableScanNode = scanNodes.keySet().iterator().next();
            if (getAresDbTableHandle(tableScanNode).isPresent()) {
                return Optional.of(tableScanNode);
            }
        }
        return Optional.empty();
    }

    private static PlanNode replaceChildren(PlanNode node, List<PlanNode> children)
    {
        for (int i = 0; i < node.getSources().size(); i++) {
            if (children.get(i) != node.getSources().get(i)) {
                return node.replaceChildren(children);
            }
        }
        return node;
    }

    // Single use visitor that needs the AresDb table handle
    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final ConnectorSession session;
        private final TableScanNode tableScanNode;
        private final IdentityHashMap<FilterNode, Void> filtersSplitUp = new IdentityHashMap<>();

        public Visitor(TableScanNode tableScanNode, ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.idAllocator = idAllocator;
            this.tableScanNode = tableScanNode;
            // Just making sure that the table exists
            getAresDbTableHandle(this.tableScanNode).get().getTableName();
        }

        private Optional<PlanNode> tryCreatingNewScanNode(PlanNode plan)
        {
            Optional<AresDbQueryGeneratorResult> aql = aresDbQueryGenerator.generate(plan, session);
            if (!aql.isPresent()) {
                return Optional.empty();
            }
            AresDbTableHandle aresDbTableHandle = getAresDbTableHandle(tableScanNode).orElseThrow(() -> new AresDbException(ARESDB_UNCLASSIFIED_ERROR, null, "Expected to find a aresDb table handle"));
            AresDbQueryGeneratorContext context = aql.get().getContext();
            TableHandle oldTableHandle = tableScanNode.getTable();
            LinkedHashMap<VariableReferenceExpression, AresDbColumnHandle> assignments = context.getAssignments();
            TableHandle newTableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    new AresDbTableHandle(
                            aresDbTableHandle.getConnectorId(),
                            aresDbTableHandle.getTableName(),
                            aresDbTableHandle.getTimeColumnName(),
                            aresDbTableHandle.getTimeStampType(),
                            aresDbTableHandle.getRetention(),
                            Optional.of(aql.get().isQueryShort()),
                            aql),
                    oldTableHandle.getTransaction(),
                    oldTableHandle.getLayout());
            return Optional.of(
                    new TableScanNode(
                            idAllocator.getNextId(),
                            newTableHandle,
                            ImmutableList.copyOf(assignments.keySet()),
                            assignments.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, (e) -> (ColumnHandle) (e.getValue()))),
                            tableScanNode.getCurrentConstraint(),
                            tableScanNode.getEnforcedConstraint()));
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            Optional<PlanNode> pushedDownPlan = tryCreatingNewScanNode(node);
            return pushedDownPlan.orElseGet(() -> replaceChildren(
                    node,
                    node.getSources().stream().map(source -> source.accept(this, null)).collect(toImmutableList())));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            if (filtersSplitUp.containsKey(node)) {
                return this.visitPlan(node, context);
            }
            filtersSplitUp.put(node, null);
            FilterNode nodeToRecurseInto = node;
            List<RowExpression> pushable = new ArrayList<>();
            List<RowExpression> nonPushable = new ArrayList<>();
            AresDbFilterExpressionConverter aresDbExpressionConverter = new AresDbFilterExpressionConverter(typeManager, functionMetadataManager, standardFunctionResolution);
            for (RowExpression conjunct : LogicalRowExpressions.extractConjuncts(node.getPredicate())) {
                try {
                    conjunct.accept(aresDbExpressionConverter, (var) -> new AresDbQueryGeneratorContext.Selection(var.getName(), AresDbQueryGeneratorContext.Origin.DERIVED, Optional.empty()));
                    pushable.add(conjunct);
                }
                catch (AresDbException ae) {
                    nonPushable.add(conjunct);
                }
            }
            if (!pushable.isEmpty()) {
                FilterNode pushableFilter = new FilterNode(idAllocator.getNextId(), node.getSource(), logicalRowExpressions.combineConjuncts(pushable));
                Optional<FilterNode> nonPushableFilter = nonPushable.isEmpty() ? Optional.empty() : Optional.of(new FilterNode(idAllocator.getNextId(), pushableFilter, logicalRowExpressions.combineConjuncts(nonPushable)));

                filtersSplitUp.put(pushableFilter, null);
                if (nonPushableFilter.isPresent()) {
                    FilterNode nonPushableFilterNode = nonPushableFilter.get();
                    filtersSplitUp.put(nonPushableFilterNode, null);
                    nodeToRecurseInto = nonPushableFilterNode;
                }
                else {
                    nodeToRecurseInto = pushableFilter;
                }
            }
            return this.visitFilter(nodeToRecurseInto, context);
        }
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        Map<TableScanNode, Void> scanNodes = maxSubplan.accept(new TableFindingVisitor(), null);
        TableScanNode aresDbTableScanNode = getOnlyAresDbTable(scanNodes)
                .orElseThrow(() -> new PrestoException(GENERIC_INTERNAL_ERROR,
                        "Expected to find the Aresdb table handle for the scan node"));
        return maxSubplan.accept(new Visitor(aresDbTableScanNode, session, idAllocator), null);
    }
}
