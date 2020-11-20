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
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class EnforcePartitionFilter
        implements PlanOptimizer
{
    private static final String ERROR_MESSAGE_FORMAT =
            "Filters need to be specified on all partition columns of a table. Your query is missing filters on "
                    + "columns ('%s') for table '%s'. Please add filters in the WHERE clause of your query. For example: "
                    + "WHERE DATE(%s) > CURRENT_DATE - INTERVAL '7' DAY. See more details at "
                    + "https://engwiki.uberinternal.com/display/TE0PRESTO/Query+Optimization#QueryOptimization-Filterbypartitioncolumn ";
    private static final Pattern DATE_COLUMN_NAME_PATTERN = Pattern.compile(".*(date|week|month).*");
    private final Metadata metadata;

    public EnforcePartitionFilter(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.metadata = metadata;
    }

    private static Set<SchemaTableName> getSchemaTableNames(String tableList)
    {
        Spliterator<String> spliterator = Splitter.on(':').omitEmptyStrings().trimResults().split(tableList).spliterator();
        return StreamSupport.stream(spliterator, false).map(EnforcePartitionFilter::parseTableName).collect(Collectors.toSet());
    }

    private static SchemaTableName parseTableName(String schemaTableName)
    {
        checkArgument(!isNullOrEmpty(schemaTableName), "schemaTableName is null or is empty");
        List<String> parts = Splitter.on('.').splitToList(schemaTableName);
        checkArgument(parts.size() == 2, "Invalid schemaTableName: %s", schemaTableName);
        return new SchemaTableName(parts.get(0), parts.get(1).replace("*", ".*"));
    }

    public static TableLayout getTableLayout(Metadata metadata, Session session, TableScanNode tableScan)
    {
        if (!tableScan.getTable().getLayout().isPresent()) {
            return metadata.getLayout(session, tableScan.getTable(), Constraint.<ColumnHandle>alwaysTrue(), Optional.empty()).getLayout();
        }
        else {
            return metadata.getLayout(session, tableScan.getTable());
        }
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        String partitionFilterTables = SystemSessionProperties.getPartitionFilterTables(session);
        if (!SystemSessionProperties.enforcePartitionFilter(session) || partitionFilterTables.isEmpty()) {
            return plan;
        }
        Set<SchemaTableName> enforcedTables = getSchemaTableNames(partitionFilterTables);
        Set<String> enforcedCompleteSchemas = enforcedTables.stream().filter(schemaTableName -> ".*".equals(schemaTableName.getTableName())).map(SchemaTableName::getSchemaName).collect(Collectors.toSet());
        PartitionFilteringContext context = new PartitionFilteringContext();
        plan.accept(new Visitor(), context);
        Map<PlanNodeId, TableScanInfo> tableScanInfos = context.getTableScanInfos();
        for (TableScanInfo tableScanInfo : tableScanInfos.values()) {
            checkTableScan(tableScanInfo, types, session, enforcedTables, enforcedCompleteSchemas);
        }
        return plan;
    }

    private void checkTableScan(TableScanInfo tableScanInfo, TypeProvider types, Session session, Set<SchemaTableName> enforcedTables, Set<String> enforcedCompleteSchemas)
    {
        TableScanNode tableScan = tableScanInfo.getTableScanNode();
        TableLayout layout = getTableLayout(metadata, session, tableScan);
        if (layout == null || !layout.getDiscretePredicates().isPresent()) {
            return;
        }
        SchemaTableName schemaTableName = metadata.getTableMetadata(session, tableScan.getTable()).getTable();
        if (!enforcedCompleteSchemas.contains(schemaTableName.getSchemaName()) && !isTableEnforced(schemaTableName, enforcedTables)) {
            return;
        }
        List<ColumnHandle> partitionColumns = layout.getDiscretePredicates().get().getColumns();
        Set<ColumnHandle> predicateColumns = ImmutableSet.<ColumnHandle>builder()
                .addAll(getColumnHandleInFilterPredicate(tableScanInfo, types))
                .addAll(getColumnHandleFromEnforcedConstraint(tableScan)).build();
        String dateColumnName = "datestr";
        List<String> missingPartitionColumnNames = new ArrayList<>();
        for (ColumnHandle partitionColumn : partitionColumns) {
            if (!predicateColumns.contains(partitionColumn)) {
                String missingColumnName = metadata.getColumnMetadata(session, tableScan.getTable(), partitionColumn).getName();
                missingPartitionColumnNames.add(missingColumnName);
                if (DATE_COLUMN_NAME_PATTERN.matcher(missingColumnName).matches()) {
                    dateColumnName = missingColumnName;
                }
            }
        }
        if (!missingPartitionColumnNames.isEmpty()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, String.format(ERROR_MESSAGE_FORMAT,
                    String.join("', '", missingPartitionColumnNames), schemaTableName, dateColumnName));
        }
    }

    private boolean isTableEnforced(SchemaTableName schemaTableName, Set<SchemaTableName> enforcedTables)
    {
        for (SchemaTableName enforcedSchemaTableName : enforcedTables) {
            if (schemaTableName.getSchemaName().equals(enforcedSchemaTableName.getSchemaName())) {
                if (schemaTableName.getTableName().matches(enforcedSchemaTableName.getTableName())) {
                    return true;
                }
            }
        }
        return false;
    }

    private Set<ColumnHandle> getColumnHandleInFilterPredicate(TableScanInfo tableScanInfo, TypeProvider types)
    {
        if (!tableScanInfo.getPredicates().isPresent()) {
            return ImmutableSet.of();
        }
        TableScanNode tableScan = tableScanInfo.getTableScanNode();
        RowExpression predicate = tableScanInfo.getPredicates().get();

        Set<VariableReferenceExpression> references;
        if (OriginalExpressionUtils.isExpression(predicate)) {
            references = VariablesExtractor.extractUnique(OriginalExpressionUtils.castToExpression(predicate), types);
        }
        else {
            references = VariablesExtractor.extractUnique(tableScanInfo.getPredicates().get());
        }
        return references.stream()
                .filter(symbol -> tableScan.getAssignments().containsKey(symbol))
                .map(symbol -> tableScan.getAssignments().get(symbol))
                .collect(toImmutableSet());
    }

    private Set<ColumnHandle> getColumnHandleFromEnforcedConstraint(TableScanNode tableScanNode)
    {
        Set<ColumnHandle> symbols = new HashSet<>();
        tableScanNode.getEnforcedConstraint().getDomains().map(Map::keySet).ifPresent(symbols::addAll);
        return symbols;
    }

    private static class PartitionFilteringContext
    {
        private Map<PlanNodeId, TableScanInfo> tableScanInfos = new HashMap<>();
        private boolean isExplainAnalyze;

        boolean isExplainAnalyze()
        {
            return isExplainAnalyze;
        }

        void setExplainAnalyze(boolean explainAnalyze)
        {
            isExplainAnalyze = explainAnalyze;
        }

        Map<PlanNodeId, TableScanInfo> getTableScanInfos()
        {
            return tableScanInfos;
        }
    }

    private static class TableScanInfo
    {
        private final TableScanNode tableScanNode;
        private final Optional<RowExpression> predicates;

        TableScanInfo(TableScanNode tableScanNode, Optional<RowExpression> predicate)
        {
            this.tableScanNode = requireNonNull(tableScanNode);
            this.predicates = requireNonNull(predicate);
        }

        TableScanNode getTableScanNode()
        {
            return tableScanNode;
        }

        Optional<RowExpression> getPredicates()
        {
            return predicates;
        }
    }

    private static class Visitor
            extends SimplePlanVisitor<PartitionFilteringContext>
    {
        @Override
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, PartitionFilteringContext context)
        {
            context.setExplainAnalyze(true);
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, PartitionFilteringContext context)
        {
            if (context.isExplainAnalyze()) {
                return null;
            }
            if (node.getSource() instanceof TableScanNode) {
                TableScanNode child = (TableScanNode) node.getSource();
                Preconditions.checkArgument(!context.getTableScanInfos().containsKey(child.getId()), "tableScan already exists before checking filter node");
                context.getTableScanInfos().put(child.getId(), new TableScanInfo(child, Optional.of(node.getPredicate())));
            }
            return super.visitFilter(node, context);
        }

        @Override
        public Void visitTableScan(TableScanNode tableScan, PartitionFilteringContext context)
        {
            if (context.isExplainAnalyze()) {
                return null;
            }
            context.getTableScanInfos().putIfAbsent(tableScan.getId(), new TableScanInfo(tableScan, Optional.empty()));
            return null;
        }
    }
}
