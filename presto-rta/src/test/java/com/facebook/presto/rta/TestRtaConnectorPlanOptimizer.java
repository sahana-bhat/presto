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

import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.pinot.PinotColumnHandle;
import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.PinotConnectorPlanOptimizer;
import com.facebook.presto.pinot.PinotTableHandle;
import com.facebook.presto.pinot.TestPinotQueryBase;
import com.facebook.presto.pinot.TestPinotSplitManager;
import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRtaConnectorPlanOptimizer
        extends TestPinotQueryBase
{
    private static final SessionHolder defaultSessionHolder = new SessionHolder(false);
    private final LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(
            new RowExpressionDeterminismEvaluator(functionMetadataManager),
            new FunctionResolution(functionMetadataManager),
            functionMetadataManager);
    private final PinotTableHandle pinotTable = TestPinotSplitManager.hybridTable;
    private final RtaStorageKey pinotStorageKey = new RtaStorageKey("dca1", RtaStorageType.PINOT);
    private final RtaTableHandle rtaTableHandle = new RtaTableHandle(new RtaConnectorId("rta"), pinotStorageKey, new SchemaTableName("rta", "test"), pinotTable);

    @Test
    public void testPinotPlanPushDown()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        TableScanNode tableScanNode = rtaTableScan(planBuilder, rtaTableHandle, regionId, city, fare, secondsSinceEpoch);
        PlanNode originalPlan = filter(planBuilder, tableScanNode, getRowExpression("fare > 100", defaultSessionHolder));
        PlanNode optimized = getOptimizedPlan(planBuilder, originalPlan, true);
        assertTrue(optimized instanceof TableScanNode);
        TableScanNode scanNode = (TableScanNode) optimized;
        assertTrue(scanNode.getTable().getConnectorHandle() instanceof RtaTableHandle);
        RtaTableHandle rtaTableHandle1 = (RtaTableHandle) scanNode.getTable().getConnectorHandle();
        assertTrue(rtaTableHandle1.getHandle() instanceof PinotTableHandle);
        PinotTableHandle pushedDownPlan = (PinotTableHandle) rtaTableHandle1.getHandle();
        assertEquals(pushedDownPlan.getPql().get().getPql(), "SELECT regionId, city, fare, secondsSinceEpoch FROM hybrid__TABLE_NAME_SUFFIX_TEMPLATE__ WHERE (fare > 100)__TIME_BOUNDARY_FILTER_TEMPLATE__ LIMIT 2147483647");
    }

    private TableScanNode rtaTableScan(PlanBuilder planBuilder, RtaTableHandle connectorTableHandle, PinotColumnHandle... columnHandles)
    {
        List<VariableReferenceExpression> variables = Arrays.stream(columnHandles).map(ch -> new VariableReferenceExpression(ch.getColumnName().toLowerCase(ENGLISH), ch.getDataType())).collect(toImmutableList());
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.builder();
        for (int i = 0; i < variables.size(); ++i) {
            assignments.put(variables.get(i), columnHandles[i]);
        }
        TableHandle tableHandle = new TableHandle(
                pinotConnectorId,
                connectorTableHandle,
                TestingTransactionHandle.create(),
                Optional.empty());
        return planBuilder.tableScan(
                tableHandle,
                variables,
                assignments.build());
    }

    private PlanNode getOptimizedPlan(PlanBuilder planBuilder, PlanNode originalPlan, boolean scanParallelism)
    {
        PinotConfig pinotConfig = new PinotConfig().setPreferBrokerQueries(scanParallelism);
        PinotQueryGenerator pinotQueryGenerator = new PinotQueryGenerator(pinotConfig, typeManager, functionMetadataManager, standardFunctionResolution);
        ConnectorPlanOptimizer pinotOptimizer = new PinotConnectorPlanOptimizer(pinotQueryGenerator, typeManager, functionMetadataManager, logicalRowExpressions, standardFunctionResolution);
        RtaConnectorProvider rtaConnectorProvider = mock(RtaConnectorProvider.class);
        RtaConnector rtaConnector = mock(RtaConnector.class);
        when(rtaConnector.getConnectorPlanOptimizerProvider()).thenReturn(new ConnectorPlanOptimizerProvider() {
            @Override
            public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers()
            {
                return ImmutableSet.of(pinotOptimizer);
            }

            @Override
            public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers()
            {
                return ImmutableSet.of();
            }
        });
        when(rtaConnectorProvider.getConnector(pinotStorageKey)).thenReturn(rtaConnector);
        RtaConnectorPlanOptimizer rtaConnectorPlanOptimizer = new RtaConnectorPlanOptimizer(rtaConnectorProvider);
        return rtaConnectorPlanOptimizer.optimize(originalPlan, defaultSessionHolder.getConnectorSession(), new PlanVariableAllocator(), planBuilder.getIdAllocator());
    }
}
