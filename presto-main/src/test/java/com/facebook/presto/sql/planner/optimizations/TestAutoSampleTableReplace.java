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
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableSample;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.TestingMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.AUTO_SAMPLE_TABLE_REPLACE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestAutoSampleTableReplace
        extends BasePlanTest
{
    // Test that the optimizer replaces the original table with the sampled table
    @Test
    public void testSampleTableReplaceWithProperty()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanVariableAllocator variableAllocator = new PlanVariableAllocator();

        Session session = testSessionBuilder()
                .setSystemProperty(AUTO_SAMPLE_TABLE_REPLACE, "true")
                .build();
        MockMetadata metadata = new MockMetadata();

        SchemaTableName table1Name = new SchemaTableName("db_name", "table_name");
        SchemaTableName table2Name = new SchemaTableName("db_name", "table_name2");

        ImmutableList.Builder<ColumnMetadata> columnsBuilder = ImmutableList.builder();
        columnsBuilder.add(new ColumnMetadata("column1", BIGINT));
        columnsBuilder.add(new ColumnMetadata("column2", VARCHAR));
        columnsBuilder.add(new ColumnMetadata("column3", BOOLEAN));
        columnsBuilder.add(new ColumnMetadata("column4", DATE));
        ImmutableList<ColumnMetadata> columns = columnsBuilder.build();

        ConnectorTableMetadata table1 = new ConnectorTableMetadata(
                table1Name,
                columns,
                ImmutableMap.of(),
                Optional.empty(),
                Collections.singletonList(new TableSample((table2Name))));

        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignmentsBuilder = ImmutableMap.builder();
        ImmutableList.Builder<VariableReferenceExpression> variableBuilder = ImmutableList.builder();
        ImmutableMap.Builder<String, ColumnHandle> columnHandleBuilder1 = ImmutableMap.builder();
        ImmutableMap.Builder<String, ColumnHandle> columnHandleBuilder2 = ImmutableMap.builder();

        for (ColumnMetadata cm : columns) {
            VariableReferenceExpression var1 = variableAllocator.newVariable(cm.getName(), cm.getType());
            ColumnHandle c1 = new TestingColumnHandle(cm.getName());
            variableBuilder.add(var1);
            assignmentsBuilder.put(var1, c1);
            columnHandleBuilder1.put(cm.getName(), c1);
            columnHandleBuilder2.put(cm.getName(), new TestingColumnHandle(cm.getName()));
            metadata.addColumnMetadata(c1, cm);
        }

        ImmutableList<VariableReferenceExpression> vars = variableBuilder.build();
        ImmutableMap<VariableReferenceExpression, ColumnHandle> assignments = assignmentsBuilder.build();

        ImmutableMap<String, ColumnHandle> columnHandles1 = columnHandleBuilder1.build();
        ImmutableMap<String, ColumnHandle> columnHandles2 = columnHandleBuilder2.build();

        PlanBuilder p = new PlanBuilder(session, idAllocator, metadata);
        TableScanNode plan = p.tableScan(table1Name, vars, assignments);

        TableScanNode plan2 = p.tableScan(table2Name, vars, assignments);

        metadata.addTable(table1);
        metadata.addColumnHandles(plan.getTable(), columnHandles1);

        metadata.addTable(plan2.getTable());
        metadata.addColumnHandles(plan2.getTable(), columnHandles2);

        PlanNode optimized = optimize(plan, session, idAllocator, metadata);
        assertTrue(optimized instanceof TableScanNode);
        assertNotEquals(MockMetadata.getTableName(((TableScanNode) optimized).getTable()), MockMetadata.getTableName(plan.getTable()));
        assertEquals(MockMetadata.getTableName(((TableScanNode) optimized).getTable()), MockMetadata.getTableName(plan2.getTable()));
    }

    // Test that if the variable names in the TableScanNode assignments do not
    // match the column handle names, it still works
    @Test
    public void testSampleTableReplaceWithPropertyWithDifferentVarNames()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanVariableAllocator variableAllocator = new PlanVariableAllocator();

        Session session = testSessionBuilder()
                .setSystemProperty(AUTO_SAMPLE_TABLE_REPLACE, "true")
                .build();
        MockMetadata metadata = new MockMetadata();

        SchemaTableName table1Name = new SchemaTableName("db_name", "table_name");
        SchemaTableName table2Name = new SchemaTableName("db_name", "table_name2");

        ImmutableList.Builder<ColumnMetadata> columnsBuilder = ImmutableList.builder();
        columnsBuilder.add(new ColumnMetadata("column1", BIGINT));
        columnsBuilder.add(new ColumnMetadata("column2", VARCHAR));
        columnsBuilder.add(new ColumnMetadata("column3", BOOLEAN));
        columnsBuilder.add(new ColumnMetadata("column4", DATE));
        ImmutableList<ColumnMetadata> columns = columnsBuilder.build();

        ConnectorTableMetadata table1 = new ConnectorTableMetadata(
                table1Name,
                columns,
                ImmutableMap.of(),
                Optional.empty(),
                Collections.singletonList(new TableSample((table2Name))));

        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignmentsBuilder = ImmutableMap.builder();
        ImmutableList.Builder<VariableReferenceExpression> variableBuilder = ImmutableList.builder();
        ImmutableMap.Builder<String, ColumnHandle> columnHandleBuilder1 = ImmutableMap.builder();
        ImmutableMap.Builder<String, ColumnHandle> columnHandleBuilder2 = ImmutableMap.builder();

        for (ColumnMetadata cm : columns) {
            VariableReferenceExpression var1 = variableAllocator.newVariable(cm.getName() + "tmp", cm.getType());
            ColumnHandle c1 = new TestingColumnHandle(cm.getName());
            variableBuilder.add(var1);
            assignmentsBuilder.put(var1, c1);
            columnHandleBuilder1.put(cm.getName(), c1);
            columnHandleBuilder2.put(cm.getName(), new TestingColumnHandle(cm.getName()));
            metadata.addColumnMetadata(c1, cm);
        }

        ImmutableList<VariableReferenceExpression> vars = variableBuilder.build();
        ImmutableMap<VariableReferenceExpression, ColumnHandle> assignments = assignmentsBuilder.build();

        ImmutableMap<String, ColumnHandle> columnHandles1 = columnHandleBuilder1.build();
        ImmutableMap<String, ColumnHandle> columnHandles2 = columnHandleBuilder2.build();

        PlanBuilder p = new PlanBuilder(session, idAllocator, metadata);
        TableScanNode plan = p.tableScan(table1Name, vars, assignments);

        TableScanNode plan2 = p.tableScan(table2Name, vars, assignments);

        metadata.addTable(table1);
        metadata.addColumnHandles(plan.getTable(), columnHandles1);

        metadata.addTable(plan2.getTable());
        metadata.addColumnHandles(plan2.getTable(), columnHandles2);

        PlanNode optimized = optimize(plan, session, idAllocator, metadata);
        assertTrue(optimized instanceof TableScanNode);
        assertNotEquals(MockMetadata.getTableName(((TableScanNode) optimized).getTable()), MockMetadata.getTableName(plan.getTable()));
        assertEquals(MockMetadata.getTableName(((TableScanNode) optimized).getTable()), MockMetadata.getTableName(plan2.getTable()));
    }

    @Test
    public void testSampleTableReplaceWithoutProperty()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Session session = testSessionBuilder()
                .setSystemProperty(AUTO_SAMPLE_TABLE_REPLACE, "true")
                .build();
        MockMetadata metadata = new MockMetadata();

        SchemaTableName table1Name = new SchemaTableName("db_name", "table_name");
        ConnectorTableMetadata table1 = new ConnectorTableMetadata(table1Name, ImmutableList.of(), ImmutableMap.of());
        metadata.addTable(table1);

        PlanBuilder p = new PlanBuilder(session, idAllocator, metadata);
        TableScanNode plan = p.tableScan(table1Name, ImmutableList.of(), ImmutableMap.of());

        PlanNode optimized = optimize(plan, session, idAllocator, metadata);
        assertTrue(optimized instanceof TableScanNode);
        assertEquals(MockMetadata.getTableName(((TableScanNode) optimized).getTable()), MockMetadata.getTableName(plan.getTable()));
    }

    @Test
    public void testSampleTableNoReplaceWithoutSession()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Metadata metadata = new MockMetadata();
        PlanBuilder p = new PlanBuilder(TEST_SESSION, idAllocator, metadata);
        PlanNode plan = p.tableScan(ImmutableList.of(), ImmutableMap.of());
        PlanNode actual = optimize(plan, TEST_SESSION, idAllocator, metadata);
        assertEquals(actual, plan);
    }

    private PlanNode optimize(PlanNode plan, Session session, PlanNodeIdAllocator idAllocator, Metadata metadata)
    {
        AutoSampleTableReplaceOptimizer optimizer = new AutoSampleTableReplaceOptimizer(metadata);
        return optimizer.optimize(plan, session, TypeProvider.empty(), new PlanVariableAllocator(), idAllocator, WarningCollector.NOOP);
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final ConcurrentMap<SchemaTableName, ConnectorTableMetadata> tables = new ConcurrentHashMap<>();
        private final ConcurrentMap<SchemaTableName, TableHandle> tableHandles = new ConcurrentHashMap<>();
        private final ConcurrentMap<SchemaTableName, Map<String, ColumnHandle>> columnHandles = new ConcurrentHashMap<>();
        private final ConcurrentMap<ColumnHandle, ColumnMetadata> columnMetadataMap = new ConcurrentHashMap<>();

        public void addTable(ConnectorTableMetadata tableMetadata)
        {
            tables.putIfAbsent(tableMetadata.getTable(), tableMetadata);
        }

        public void addTable(TableHandle tableHandle)
        {
            tableHandles.putIfAbsent(getTableName(tableHandle), tableHandle);
        }

        public void addColumnHandles(TableHandle tableHandle, Map<String, ColumnHandle> columnHandleMap)
        {
            columnHandles.putIfAbsent(getTableName(tableHandle), columnHandleMap);
        }

        public void addColumnMetadata(ColumnHandle columnHandle, ColumnMetadata columnMetadata)
        {
            columnMetadataMap.putIfAbsent(columnHandle, columnMetadata);
        }

        @Override
        public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
        {
            requireNonNull(tableHandle, "tableHandle is null");
            SchemaTableName tableName = getTableName(tableHandle);
            ConnectorTableMetadata tableMetadata = tables.get(tableName);
            checkArgument(tableMetadata != null, "Table %s does not exist", tableName);
            return new TableMetadata(tableHandle.getConnectorId(), tableMetadata);
        }

        private static SchemaTableName getTableName(TableHandle tableHandle)
        {
            requireNonNull(tableHandle, "tableHandle is null");
            ConnectorTableHandle connTableHandle = tableHandle.getConnectorHandle();
            checkArgument(connTableHandle instanceof TestingMetadata.TestingTableHandle, "tableHandle is not an instance of TestingTableHandle");
            TestingMetadata.TestingTableHandle testingTableHandle = (TestingMetadata.TestingTableHandle) connTableHandle;
            return testingTableHandle.getTableName();
        }

        @Override
        public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
        {
            SchemaTableName tableName2 = new SchemaTableName(tableName.getSchemaName(), tableName.getObjectName());
            if (!tableHandles.containsKey(tableName2)) {
                return Optional.empty();
            }
            return Optional.of(tableHandles.get(tableName2));
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
        {
            SchemaTableName tableName = getTableName(tableHandle);
            return columnHandles.getOrDefault(tableName, ImmutableMap.of());
        }

        @Override
        public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            return columnMetadataMap.get(columnHandle);
        }
    }
}
