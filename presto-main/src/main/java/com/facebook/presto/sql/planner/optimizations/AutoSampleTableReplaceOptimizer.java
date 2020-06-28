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
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableSample;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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

        return SimplePlanRewriter.rewriteWith(new SampledTableReplacer(metadata, idAllocator, session), plan);
    }

    private static class SampledTableReplacer
            extends SimplePlanRewriter<Void>
    {
        private static final Logger log = Logger.get(SampledTableReplacer.class);
        private final Metadata metadata;
        private final Session session;
        private final PlanNodeIdAllocator idAllocator;

        private SampledTableReplacer(Metadata metadata, PlanNodeIdAllocator idAllocator, Session session)
        {
            this.metadata = metadata;
            this.session = session;
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
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
            for (VariableReferenceExpression vre : node.getAssignments().keySet()) {
                if (!sampleColumnHandles.containsKey(vre.getName())) {
                    log.warn("sample table " + sampleTable + " does not contain column " + vre.getName());
                    return node;
                }
                columns.put(vre, sampleColumnHandles.get(vre.getName()));
            }

            PlanNode tableScan = new TableScanNode(
                    idAllocator.getNextId(),
                    tableHandle.get(),
                    node.getOutputVariables(),
                    columns.build(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint());
            return tableScan;
        }
    }
}
