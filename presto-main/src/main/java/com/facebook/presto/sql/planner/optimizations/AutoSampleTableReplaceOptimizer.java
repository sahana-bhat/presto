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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableSample;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class AutoSampleTableReplaceOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;

    public AutoSampleTableReplaceOptimizer(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (!SystemSessionProperties.isAutoSampleTableReplace(session)) {
            return plan;
        }
        SampledTableReplacer.Context ctx = new SampledTableReplacer.Context();
        PlanNode root = SimplePlanRewriter.rewriteWith(new SampledTableReplacer(metadata, variableAllocator, idAllocator, session, types), plan, ctx);
        if (ctx.getSamplingFailed()) {
            throw new PrestoException(StandardErrorCode.SAMPLING_UNSUPPORTED_CASE, ctx.getFailureMessage());
        }
        return root;
    }

    private static class SampledTableReplacer
            extends SimplePlanRewriter<SampledTableReplacer.Context>
    {
        private static final Logger log = Logger.get(SampledTableReplacer.class);
        private final Metadata metadata;
        private final Session session;
        private final TypeProvider types;
        private final PlanVariableAllocator variableAllocator;
        private final PlanNodeIdAllocator idAllocator;

        private SampledTableReplacer(
                Metadata metadata,
                PlanVariableAllocator variableAllocator,
                PlanNodeIdAllocator idAllocator,
                Session session,
                TypeProvider types)
        {
            this.metadata = metadata;
            this.session = session;
            this.variableAllocator = variableAllocator;
            this.idAllocator = idAllocator;
            this.types = types;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Context> context)
        {
            if (!session.getCatalog().isPresent()) {
                return node;
            }

            if (!(node.getCurrentConstraint().equals(TupleDomain.all()) && node.getEnforcedConstraint().equals(TupleDomain.all()))) {
                // Not handling it here. Hopefully this optimizer will run before any other optimizer that adds these
                // constraints, so that they (and any table layouts) will get added after this transformation.
                return node;
            }

            TableHandle oldTableHandle = node.getTable();
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, oldTableHandle).getMetadata();

            List<TableSample> sampledTables = tableMetadata.getSampledTables();
            if (sampledTables.isEmpty()) {
                return node;
            }
            TableSample sampleTable = sampledTables.get(0);
            QualifiedObjectName name = new QualifiedObjectName(
                    session.getCatalog().get(),
                    sampleTable.getTableName().getSchemaName(),
                    sampleTable.getTableName().getTableName());
            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, name);
            if (!tableHandle.isPresent()) {
                log.warn("Could not create table handle for " + sampleTable);
                return node;
            }

            Map<String, ColumnHandle> sampleColumnHandles = metadata.getColumnHandles(session, tableHandle.get());
            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> columns = ImmutableMap.builder();
            Map<VariableReferenceExpression, SampleInfo> samples = new HashMap<>();
            for (VariableReferenceExpression vre : node.getAssignments().keySet()) {
                ColumnMetadata cm = metadata.getColumnMetadata(session, oldTableHandle, node.getAssignments().get(vre));
                if (!sampleColumnHandles.containsKey(cm.getName())) {
                    log.warn("Sample Table " + sampleTable.getTableName() + " does not contain column " + cm.getName());
                    return node;
                }
                columns.put(vre, sampleColumnHandles.get(cm.getName()));
                samples.put(vre, new SampleInfo(sampleTable, cm.getName()));
            }

            if (!node.getOutputVariables().stream().allMatch(vre -> samples.containsKey(vre))) {
                log.warn("Output variables not found in assignments for table " + tableMetadata.getTable());
                return node;
            }
            node.getOutputVariables().forEach(x -> context.get().getMap().put(x.getName(), samples.get(x)));

            log.debug("tablescan. map contains " + context.get().getMap());

            return new TableScanNode(
                    idAllocator.getNextId(),
                    tableHandle.get(),
                    node.getOutputVariables(),
                    columns.build(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint(),
                    true,
                    node.isPartialAggregationPushedDown());
        }

        private ImmutableSet<String> getVariableNames(RowExpression expression)
                throws Exception
        {
            ImmutableSet.Builder<String> variablesBuilder = ImmutableSet.builder();
            if (expression instanceof VariableReferenceExpression) {
                variablesBuilder.add(((VariableReferenceExpression) expression).getName());
            }
            else if (isExpression(expression)) {
                ExpressionTreeRewriter.rewriteWith(new Visitor(), castToExpression(expression), variablesBuilder);
            }
            else {
                throw new Exception(
                        "Unable to get variable name from expression " + expression.toString());
            }

            ImmutableSet<String> vars = variablesBuilder.build();
            return vars;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Context> context)
        {
            log.debug("entering project node, id " + node.getId() + ". assignments " + node.getAssignments().getMap() + ". outputs " + node.getOutputVariables());
            PlanNode source = context.rewrite(node.getSource(), context.get());

            if (context.get().getSamplingFailed()) {
                return node;
            }

            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().getMap().entrySet()) {
                if (context.get().getMap().containsKey(entry.getKey().getName())) {
                    continue;
                }
                ImmutableSet<String> variables;
                try {
                    variables = getVariableNames(entry.getValue());
                }
                catch (Exception ex) {
                    context.get().setFailed(ex.getMessage());
                    return node;
                }
                SampleInfo si = null;
                for (String variable : variables) {
                    SampleInfo lsi = context.get().getMap().get(variable);
                    if (lsi != null) {
                        if (si == null) {
                            si = lsi;
                        }
                        else {
                            if (!si.getTableSample().equals(lsi.getTableSample())) {
                                context.get().setFailed("variable assignment based on multiple different sampled vars");
                                return node;
                            }
                        }
                    }
                }
                if (si != null) {
                    context.get().getMap().put(entry.getKey().getName(), si);
                    log.debug("project node " + node.getId() + ". Adding variable " + entry.getKey() + " to map.");
                }
            }
            log.debug("leaving project node " + node.getId());
            return new ProjectNode(node.getId(), source, node.getAssignments());
        }

        private boolean shouldBeScaled(CallExpression expression)
        {
            String call = expression.getDisplayName();
            return call.equals("sum") || call.equals("count") || call.equals("approx_distinct");
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Context> context)
        {
            log.debug("Aggregation node, " + node.getId() + ". aggregations " + node.getAggregations() + ". outputs " + node.getOutputVariables());
            PlanNode source = context.rewrite(node.getSource(), context.get());

            if (context.get().getSamplingFailed()) {
                return node;
            }

            ImmutableSet.Builder<VariableReferenceExpression> toBePassed = ImmutableSet.builder();
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> newVariables = ImmutableMap.builder();
            Assignments.Builder assignmentsBuilder = Assignments.builder();

            final Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = node.getAggregations();
            for (VariableReferenceExpression variable : aggregations.keySet()) {
                AggregationNode.Aggregation aggregation = aggregations.get(variable);
                if (!shouldBeScaled(aggregation.getCall())) {
                    toBePassed.add(variable);
                    continue;
                }
                ImmutableSet.Builder<String> baseVarsBuilder = ImmutableSet.builder();
                if (aggregation.getArguments().isEmpty()) {
                    if (node.getGroupingSets().getGroupingKeys().isEmpty()) {
                        context.get().setFailed("Group aggregations not supported yet");
                        return node;
                    }
                    node.getGroupingSets().getGroupingKeys().forEach(v -> baseVarsBuilder.add(v.getName()));
                }
                else {
                    aggregation.getArguments().forEach(e -> {
                        try {
                            baseVarsBuilder.addAll(getVariableNames(e));
                        }
                        catch (Exception ex) {
                            context.get().setFailed(ex.getMessage());
                        }
                    });
                    if (context.get().getSamplingFailed()) {
                        return node;
                    }
                }
                ImmutableSet<String> eligibleVars = baseVarsBuilder
                        .build()
                        .stream()
                        .filter(
                                x -> context.get().getMap().containsKey(x) &&
                                        context.get().getMap().get(x).getTableSample().getSamplingPercentage().isPresent())
                        .collect(toImmutableSet());

                if (eligibleVars.isEmpty()) {
                    toBePassed.add(variable);
                    continue;
                }
                // The aggregation is based on more than one eligible variables
                SampleInfo si = context.get().getMap().get(eligibleVars.asList().get(0));
                if (!eligibleVars.stream().allMatch(x -> context.get().getMap().get(x).getTableSample().equals(si.getTableSample()))) {
                    context.get().setFailed("Aggregation " + aggregation.toString() + " on multiple vars " + eligibleVars + " cannot be scaled");
                    return node;
                }
                log.debug("Scaling " + eligibleVars + " to " + si.getTableSample());

                String scaleFactorDouble = Double.toString(100.0 / si.getTableSample().getSamplingPercentage().get());
                String scaleFactorLong = Long.toString(Math.round(100.0 / si.getTableSample().getSamplingPercentage().get()));
                VariableReferenceExpression newVar = variableAllocator.newVariable("sample_" + variable.getName(), variable.getType());
                Expression expr = new ArithmeticBinaryExpression(
                        ArithmeticBinaryExpression.Operator.MULTIPLY, new SymbolReference(newVar.getName()),
                        variable.getType() instanceof BigintType
                                ? new Cast(new LongLiteral(scaleFactorLong), BigintType.BIGINT.toString())
                                : new Cast(new DecimalLiteral(scaleFactorDouble), DOUBLE.toString()));
                assignmentsBuilder.put(
                        variable,
                        OriginalExpressionUtils.castToRowExpression(expr));
                newVariables.put(variable, newVar);
            }
            log.debug("to be passed " + toBePassed.build());

            ImmutableMap<VariableReferenceExpression, VariableReferenceExpression> newVars = newVariables.build();

            // If any of the variables being aggregated are eligible for scaling, then
            // add a project node on top of the aggregation node to do the scaleup
            if (!newVars.isEmpty()) {
                SymbolMapper mapper = new SymbolMapper(newVars);
                AggregationNode agn = mapper.map(node, source);

                // Also, Pass the aggregation outputs that are not being aggregated
                agn.getOutputVariables()
                        .stream()
                        .filter(x -> !newVars.containsValue(x))
                        .forEach(x -> assignmentsBuilder.put(x, OriginalExpressionUtils.castToRowExpression(new SymbolReference(x.getName()))));

                ProjectNode pjn = new ProjectNode(idAllocator.getNextId(), agn, assignmentsBuilder.build());
                return pjn;
            }

            log.debug("Not doing any scaling on top of the aggregation node");

            return new AggregationNode(
                    node.getId(),
                    source,
                    node.getAggregations(),
                    node.getGroupingSets(),
                    node.getPreGroupedVariables(),
                    node.getStep(),
                    node.getHashVariable(),
                    node.getGroupIdVariable());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Context> context)
        {
            PlanNode source = context.rewrite(node.getSource(), context.get());
            if (context.get().getSamplingFailed()) {
                return node;
            }

            ImmutableSet<String> predicateVars;
            try {
                predicateVars = getVariableNames(node.getPredicate());
            }
            catch (Exception ex) {
                context.get().setFailed(ex.getMessage());
                return node;
            }

            for (String predVar : predicateVars) {
                SampleInfo si = context.get().getMap().get(predVar);
                if (si != null && si.isSamplingColumn()) {
                    context.get().setFailed("Filtering " + predVar + " based on sampling column " + si.getColumnName());
                    return node;
                }
            }

            return new FilterNode(node.getId(), source, node.getPredicate());
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<Context> context)
        {
            SampledTableReplacer.Context inputContext = new SampledTableReplacer.Context();
            PlanNode input = context.rewrite(node.getInput(), inputContext);

            if (inputContext.getSamplingFailed()) {
                context.get().setFailed(inputContext.getFailureMessage());
                return node;
            }

            SampledTableReplacer.Context subQueryContext = new SampledTableReplacer.Context();
            PlanNode subQuery = context.rewrite(node.getSubquery(), subQueryContext);

            if (subQueryContext.getSamplingFailed()) {
                context.get().setFailed(subQueryContext.getFailureMessage());
                return node;
            }

            if (!inputContext.getMap().isEmpty() && !subQueryContext.getMap().isEmpty()) {
                context.get().setFailed("Both input and subQuery of apply are being sampled");
                return node;
            }

            if (!inputContext.getMap().isEmpty()) {
                context.get().getMap().putAll(inputContext.getMap());
            }
            else if (!subQueryContext.getMap().isEmpty()) {
                context.get().getMap().putAll(subQueryContext.getMap());
            }

            return new ApplyNode(
                    node.getId(),
                    input,
                    subQuery,
                    node.getSubqueryAssignments(),
                    node.getCorrelation(),
                    node.getOriginSubqueryError());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Context> context)
        {
            SampledTableReplacer.Context leftContext = new SampledTableReplacer.Context();
            PlanNode left = context.rewrite(node.getLeft(), leftContext);

            if (leftContext.getSamplingFailed()) {
                context.get().setFailed(leftContext.getFailureMessage());
                return node;
            }

            SampledTableReplacer.Context rightContext = new SampledTableReplacer.Context();
            PlanNode right = context.rewrite(node.getRight(), rightContext);

            if (rightContext.getSamplingFailed()) {
                context.get().setFailed(rightContext.getFailureMessage());
                return node;
            }

            if (!leftContext.getMap().isEmpty() && !rightContext.getMap().isEmpty()) {
                context.get().setFailed("Both left and right side of join are being sampled");
                return node;
            }

            if (!leftContext.getMap().isEmpty()) {
                context.get().getMap().putAll(leftContext.getMap());
            }
            else if (!rightContext.getMap().isEmpty()) {
                context.get().getMap().putAll(rightContext.getMap());
            }

            return new JoinNode(
                    node.getId(),
                    node.getType(),
                    left,
                    right,
                    node.getCriteria(),
                    node.getOutputVariables(),
                    node.getFilter(),
                    node.getLeftHashVariable(),
                    node.getRightHashVariable(),
                    node.getDistributionType());
        }

        public static class Context
        {
            private final Map<String, SampleInfo> sampleVariables = new HashMap<>();
            private boolean samplingFailed;
            private String failureMessage;

            public Map<String, SampleInfo> getMap()
            {
                return this.sampleVariables;
            }

            public boolean getSamplingFailed()
            {
                return this.samplingFailed;
            }

            public void setFailed(String failureMessage)
            {
                this.samplingFailed = true;
                this.failureMessage = failureMessage;
            }

            public String getFailureMessage()
            {
                return this.failureMessage;
            }
        }

        private static class Visitor
                extends ExpressionRewriter<ImmutableSet.Builder<String>>
        {
            @Override
            public Expression rewriteSymbolReference(
                    SymbolReference node,
                    ImmutableSet.Builder<String> context,
                    ExpressionTreeRewriter<ImmutableSet.Builder<String>> treeRewriter)
            {
                context.add(node.getName());
                return node;
            }
        }

        private static class SampleInfo
        {
            private final TableSample tableSample;
            private final String columnName;
            private final boolean isSamplingColumn;

            public SampleInfo(TableSample ts, String columnName)
            {
                this.tableSample = ts;
                this.columnName = columnName;
                this.isSamplingColumn = ts.getSamplingColumnName().isPresent()
                        && ts.getSamplingColumnName().get().equals(columnName);
            }

            public TableSample getTableSample()
            {
                return tableSample;
            }

            public String getColumnName()
            {
                return columnName;
            }

            public boolean isSamplingColumn()
            {
                return isSamplingColumn;
            }
        }
    }
}
