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
package com.facebook.presto.rta;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.pinot.PinotTableHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class RtaConnectorPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private static final Logger log = Logger.get(RtaConnectorPlanOptimizer.class);

    private final RtaConnectorProvider connectorProvider;

    @Inject
    public RtaConnectorPlanOptimizer(RtaConnectorProvider connectorProvider)
    {
        this.connectorProvider = connectorProvider;
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        RtaPlanRewriterContext context = new RtaPlanRewriterContext(idAllocator);

        PlanNode rewrittenNode = SimplePlanRewriter.rewriteWith(new ConnectorSpecificPlanRewriter(), maxSubplan, context);
        Set<ConnectorPlanOptimizer> optimizers = new HashSet<>();

        context.getAllTables().entrySet().forEach(stringRtaTableHandleEntry ->
                optimizers.addAll(connectorProvider.getConnector(stringRtaTableHandleEntry.getValue().getKey()).getConnectorPlanOptimizerProvider().getLogicalPlanOptimizers()));

        for (ConnectorPlanOptimizer optimizer : optimizers) {
            rewrittenNode = optimizer.optimize(rewrittenNode, session, variableAllocator, idAllocator);
        }

        rewrittenNode = SimplePlanRewriter.rewriteWith(new ConnectorToRtaPlanRewriter(), rewrittenNode, context);
        return rewrittenNode;
    }

    private static class ConnectorSpecificPlanRewriter
            extends SimplePlanRewriter<RtaPlanRewriterContext>
    {
        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<RtaPlanRewriterContext> context)
        {
            TableScanNode scanNode = (TableScanNode) context.defaultRewrite(node);

            TableHandle tableHandle = scanNode.getTable();
            context.get().addConnectorTableHandle(tableHandle.getConnectorHandle());

            RtaTableHandle connectorTableHandle = (RtaTableHandle) tableHandle.getConnectorHandle();
            Optional<ConnectorTableLayoutHandle> tableLayoutHandle = tableHandle.getLayout().isPresent()
                    ? Optional.of(((RtaTableLayoutHandle) tableHandle.getLayout().get()).createConnectorSpecificTableLayoutHandle())
                    : Optional.empty();

            TableHandle newTableHandle = new TableHandle(tableHandle.getConnectorId(), connectorTableHandle.getHandle(), tableHandle.getTransaction(), tableLayoutHandle);

            return new TableScanNode(context.get().getPlanNodeIdAllocator().getNextId(), newTableHandle, scanNode.getOutputVariables(), scanNode.getAssignments(), scanNode.getCurrentConstraint(), scanNode.getEnforcedConstraint());
        }
    }

    private static class ConnectorToRtaPlanRewriter
            extends SimplePlanRewriter<RtaPlanRewriterContext>
    {
        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<RtaPlanRewriterContext> context)
        {
            TableScanNode scanNode = (TableScanNode) context.defaultRewrite(node);
            TableHandle tableHandle = scanNode.getTable();

            RtaTableHandle rtaTableHandle = context.get().getRtaTableHandle(tableHandle.getConnectorHandle());
            RtaTableHandle newRtaTableHandle = new RtaTableHandle(rtaTableHandle.getConnectorId(), rtaTableHandle.getKey(), rtaTableHandle.getSchemaTableName(), tableHandle.getConnectorHandle());

            TableHandle newTableHandle = new TableHandle(tableHandle.getConnectorId(), newRtaTableHandle, tableHandle.getTransaction(), Optional.of(new RtaTableLayoutHandle(newRtaTableHandle)));

            return new TableScanNode(context.get().getPlanNodeIdAllocator().getNextId(), newTableHandle, scanNode.getOutputVariables(), scanNode.getAssignments(), scanNode.getCurrentConstraint(), scanNode.getEnforcedConstraint());
        }
    }

    private static class RtaPlanRewriterContext
    {
        private final PlanNodeIdAllocator planNodeIdAllocator;
        private final Map<String, RtaTableHandle> tableHandleMap;

        RtaPlanRewriterContext(PlanNodeIdAllocator planNodeIdAllocator)
        {
            this.planNodeIdAllocator = planNodeIdAllocator;
            this.tableHandleMap = new HashMap<>();
        }

        public void addConnectorTableHandle(ConnectorTableHandle connectorTableHandle)
        {
            if (!(connectorTableHandle instanceof RtaTableHandle)) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Expected to find the rta table handle for the scan node");
            }

            RtaTableHandle rtaTableHandle = (RtaTableHandle) connectorTableHandle;

            if (rtaTableHandle.getHandle() instanceof PinotTableHandle) {
                tableHandleMap.put(((PinotTableHandle) rtaTableHandle.getHandle()).toSchemaTableName().toString(), rtaTableHandle);
            }
            else {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Found unexpected underlying connector: " + rtaTableHandle.getHandle().getClass());
            }
        }

        public RtaTableHandle getRtaTableHandle(ConnectorTableHandle connectorTableHandle)
        {
            if (connectorTableHandle instanceof PinotTableHandle) {
                String key = ((PinotTableHandle) connectorTableHandle).toSchemaTableName().toString();

                if (tableHandleMap.containsKey(key)) {
                    return tableHandleMap.get(key);
                }
            }

            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Connector table not found");
        }

        public Map<String, RtaTableHandle> getAllTables()
        {
            return tableHandleMap;
        }

        public PlanNodeIdAllocator getPlanNodeIdAllocator()
        {
            return planNodeIdAllocator;
        }
    }
}
