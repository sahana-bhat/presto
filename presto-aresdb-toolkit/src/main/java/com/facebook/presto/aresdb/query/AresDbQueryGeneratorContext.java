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
package com.facebook.presto.aresdb.query;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.aresdb.AresDbColumnHandle;
import com.facebook.presto.aresdb.AresDbException;
import com.facebook.presto.aresdb.AresDbQueryGenerator.AugmentedAQL;
import com.facebook.presto.aresdb.AresDbSessionProperties;
import com.facebook.presto.aresdb.AresDbTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.predicate.AllOrNone;
import com.facebook.presto.spi.predicate.DiscreteValues;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.Ranges;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.units.Duration;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_QUERY_GENERATOR_FAILURE;
import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.aresdb.AresDbUtils.checkSupported;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AresDbQueryGeneratorContext
{
    // Fields defining the query
    // order map that maps the column definition in terms of input relation column(s)
    private final LinkedHashMap<VariableReferenceExpression, Selection> selections;
    private final Optional<TupleDomain<String>> currentConstraints;
    private final LinkedHashSet<VariableReferenceExpression> groupByColumns;
    private final Set<VariableReferenceExpression> hiddenColumnSet;
    private final Set<VariableReferenceExpression> variablesInAggregation;
    private final Optional<AresDbTableHandle> tableHandle;
    private final Optional<Domain> timeFilter;
    private final List<String> filters;
    private final OptionalInt limit;
    private final boolean aggregationApplied;
    private final List<JoinInfo> joins;
    private final boolean isPartialAggregation;

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("selections", selections)
                .add("groupByColumns", groupByColumns)
                .add("hiddenColumnSet", hiddenColumnSet)
                .add("tableHandle", tableHandle)
                .add("timeFilter", timeFilter)
                .add("filters", filters)
                .add("limit", limit)
                .add("aggregationApplied", aggregationApplied)
                .add("joins", joins)
                .toString();
    }

    public AresDbQueryGeneratorContext()
    {
        this(new LinkedHashMap<>(), Optional.empty(), Optional.empty());
    }

    public AresDbQueryGeneratorContext(LinkedHashMap<VariableReferenceExpression, Selection> selections, Optional<AresDbTableHandle> tableHandle, Optional<TupleDomain<String>> currentConstraints)
    {
        this(selections, tableHandle, currentConstraints, Optional.empty(), ImmutableList.of(), false, new LinkedHashSet<>(), OptionalInt.empty(),
                ImmutableList.of(), new HashSet<>(), new HashSet<>(), false);
    }

    private AresDbQueryGeneratorContext(LinkedHashMap<VariableReferenceExpression, Selection> selections, Optional<AresDbTableHandle> tableHandle, Optional<TupleDomain<String>> currentConstraints, Optional<Domain> timeFilter,
                                        List<String> filters, boolean aggregationApplied, LinkedHashSet<VariableReferenceExpression> groupByColumns, OptionalInt limit, List<JoinInfo> joins, Set<VariableReferenceExpression> variablesInAggregation,
                                        Set<VariableReferenceExpression> hiddenColumnSet, boolean isPartialAggregation)
    {
        this.selections = requireNonNull(selections, "selections can't be null");
        this.tableHandle = requireNonNull(tableHandle, "source can't be null");
        this.currentConstraints = requireNonNull(currentConstraints, "currentConstraints can't nbe null");
        this.aggregationApplied = aggregationApplied;
        this.groupByColumns = requireNonNull(groupByColumns, "groupByColumns can't be null. It could be empty if not available");
        this.filters = filters;
        this.limit = limit;
        this.timeFilter = timeFilter;
        this.joins = joins;
        this.variablesInAggregation = requireNonNull(variablesInAggregation, "variablesInAggregation is null");
        this.hiddenColumnSet = hiddenColumnSet;
        this.isPartialAggregation = isPartialAggregation;
    }

    public LinkedHashMap<VariableReferenceExpression, AresDbColumnHandle> getAssignments()
    {
        LinkedHashMap<VariableReferenceExpression, AresDbColumnHandle> result = new LinkedHashMap<>();
        selections.entrySet().stream().filter(e -> !hiddenColumnSet.contains(e.getKey())).forEach(entry -> {
            VariableReferenceExpression variable = entry.getKey();
            Selection selection = entry.getValue();
            AresDbColumnHandle handle = selection.getOrigin() == Origin.TABLE ? new AresDbColumnHandle(selection.getDefinition(), variable.getType(), AresDbColumnHandle.AresDbColumnType.REGULAR) : new AresDbColumnHandle(variable.getName(), variable.getType(), AresDbColumnHandle.AresDbColumnType.DERIVED);
            result.put(variable, handle);
        });
        return result;
    }

    /**
     * Apply the given filter to current context and return the updated context. Throws error for invalid operations.
     */
    public AresDbQueryGeneratorContext withFilters(List<String> extraFilters, Optional<Domain> extraTimeFilter)
    {
        checkSupported(!hasAggregation(), "AresDB doesn't support filtering the results of aggregation");
        checkSupported(!hasLimit(), "AresDB doesn't support filtering on top of the limit");
        checkSupported(!extraTimeFilter.isPresent() || !this.timeFilter.isPresent(), "Cannot put a time filter on top of a time filter");
        List<String> newFilters = ImmutableList.<String>builder().addAll(this.filters).addAll(extraFilters).build();
        Optional<Domain> newTimeFilter = this.timeFilter.isPresent() ? this.timeFilter : extraTimeFilter;
        return new AresDbQueryGeneratorContext(selections, tableHandle, currentConstraints, newTimeFilter, newFilters,
                aggregationApplied, groupByColumns, limit, joins, variablesInAggregation, hiddenColumnSet, isPartialAggregation);
    }

    /**
     * Apply the aggregation to current context and return the updated context. Throws error for invalid operations.
     */
    public AresDbQueryGeneratorContext withAggregation(LinkedHashMap<VariableReferenceExpression, Selection> newSelections, LinkedHashSet<VariableReferenceExpression>
            groupByColumns, Set<VariableReferenceExpression> hiddenColumnSet, boolean isPartialAggregation)
    {
        // there is only one aggregation supported.
        checkSupported(!hasAggregation(), "AresDB doesn't support aggregation on top of the aggregated data");
        checkSupported(!hasLimit(), "AresDB doesn't support aggregation on top of the limit");
        return new AresDbQueryGeneratorContext(newSelections, tableHandle, currentConstraints, timeFilter, filters, true, groupByColumns,
                limit, joins, variablesInAggregation, hiddenColumnSet, isPartialAggregation);
    }

    /**
     * Apply new selections/project to current context and return the updated context. Throws error for invalid operations.
     */
    public AresDbQueryGeneratorContext withProject(LinkedHashMap<VariableReferenceExpression, Selection> newSelections)
    {
        checkSupported(!hasAggregation(), "AresDB doesn't support new selections on top of the aggregated data");
        return new AresDbQueryGeneratorContext(newSelections, tableHandle, currentConstraints, timeFilter, filters, false, new LinkedHashSet<>(),
                limit, joins, variablesInAggregation, hiddenColumnSet, isPartialAggregation);
    }

    public AresDbQueryGeneratorContext withOutputColumns(List<VariableReferenceExpression> outputColumns)
    {
        LinkedHashMap<VariableReferenceExpression, Selection> newSelections = new LinkedHashMap<>();
        outputColumns.forEach(o -> newSelections.put(o, requireNonNull(selections.get(o), String.format("Cannot find the selection %s in the original context %s", o, this))));
        selections.entrySet().stream().filter(e -> hiddenColumnSet.contains(e.getKey())).forEach(e -> newSelections.put(e.getKey(), e.getValue()));
        return new AresDbQueryGeneratorContext(newSelections, tableHandle, currentConstraints, timeFilter, filters, aggregationApplied, groupByColumns, limit, joins, variablesInAggregation, hiddenColumnSet, isPartialAggregation);
    }

    public AresDbQueryGeneratorContext withVariablesInAggregation(Set<VariableReferenceExpression> newVariablesInAggregation)
    {
        return new AresDbQueryGeneratorContext(
                selections,
                tableHandle,
                currentConstraints,
                timeFilter,
                filters,
                aggregationApplied,
                groupByColumns,
                limit,
                joins,
                newVariablesInAggregation,
                hiddenColumnSet,
                isPartialAggregation);
    }

    /**
     * Apply limit to current context and return the updated context. Throws error for invalid operations.
     */
    public AresDbQueryGeneratorContext withLimit(OptionalInt limit)
    {
        checkSupported(!hasLimit(), "Don't support a limit atop a limit in ares");
        return new AresDbQueryGeneratorContext(selections, tableHandle, currentConstraints, timeFilter, filters, aggregationApplied,
                groupByColumns, limit, joins, variablesInAggregation, hiddenColumnSet, isPartialAggregation);
    }

    public AresDbQueryGeneratorContext withNewTimeBound(Domain newTimeFilter)
    {
        return new AresDbQueryGeneratorContext(selections, tableHandle, currentConstraints, Optional.of(newTimeFilter), filters,
                aggregationApplied, groupByColumns, limit, joins, variablesInAggregation, hiddenColumnSet, isPartialAggregation);
    }

    public LinkedHashMap<VariableReferenceExpression, Selection> getSelections()
    {
        return selections;
    }

    public Optional<TupleDomain<String>> getCurrentConstraints()
    {
        return currentConstraints;
    }

    public LinkedHashSet<VariableReferenceExpression> getGroupByColumns()
    {
        return groupByColumns;
    }

    public Set<VariableReferenceExpression> getHiddenColumnSet()
    {
        return hiddenColumnSet;
    }

    public Set<VariableReferenceExpression> getVariablesInAggregation()
    {
        return variablesInAggregation;
    }

    public Optional<AresDbTableHandle> getTableHandle()
    {
        return tableHandle;
    }

    public Optional<Domain> getTimeFilter()
    {
        return timeFilter;
    }

    public List<String> getFilters()
    {
        return filters;
    }

    private boolean hasLimit()
    {
        return limit.isPresent();
    }

    private boolean hasAggregation()
    {
        return aggregationApplied;
    }

    public boolean isPartialAggregation()
    {
        return isPartialAggregation;
    }

    public boolean isQueryShort(int nonAggregateRowLimit)
    {
        return hasAggregation() || limit.orElse(Integer.MAX_VALUE) < nonAggregateRowLimit;
    }

    public Optional<String> getTimeColumn()
    {
        return tableHandle.flatMap(AresDbTableHandle::getTimeColumnName);
    }

    /**
     * Convert the current context to a AresDB request (AQL)
     */
    public AugmentedAQL toQuery(Optional<ConnectorSession> session,
                                AresDbFilterExpressionConverter aresDbFilterExpressionConverter,
                                DomainTranslator domainTranslator)
    {
        List<Selection> measures;
        List<Selection> dimensions;
        if (groupByColumns.isEmpty() && !hasAggregation()) {
            // simple selections
            measures = ImmutableList.of(Selection.of("1", Origin.LITERAL));
            dimensions = new ArrayList<>(selections.values());
        }
        else if (!groupByColumns.isEmpty()) {
            measures = selections.entrySet().stream()
                    .filter(c -> !groupByColumns.contains(c.getKey()))
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());

            dimensions = selections.entrySet().stream()
                    .filter(c -> groupByColumns.contains(c.getKey()))
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());
        }
        else {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "Ares does not handle non group by aggregation " +
                    "queries yet, either fix in Ares or add final aggregation to table " + (tableHandle.map(AresDbTableHandle::getTableName).orElse(null)));
        }

        JSONObject request = new JSONObject();
        JSONArray measuresJson = new JSONArray();
        JSONArray dimensionsJson = new JSONArray();

        for (Selection measure : measures) {
            JSONObject measureJson = new JSONObject();
            measureJson.put("sqlExpression", measure.getDefinition());
            measuresJson.add(measureJson);
        }

        Set<TimeZoneKey> timeZonesInDimension = new HashSet<>();
        for (Selection dimension : dimensions) {
            JSONObject dimensionJson = new JSONObject();
            dimensionJson.put("sqlExpression", dimension.getDefinition());
            dimension.timeTokenizer.ifPresent(timeTokenizer -> {
                timeZonesInDimension.add(timeTokenizer.getTimeZoneKey());
                dimensionJson.put("timeBucketizer", timeTokenizer.getExpression());
                dimensionJson.put("timeUnit", "second"); // timeUnit: millisecond does not really work
            });
            dimensionsJson.add(dimensionJson);
        }
        Optional<TimeZoneKey> tzKey = session.map(ConnectorSession::getTimeZoneKey);
        if (!timeZonesInDimension.isEmpty()) {
            if (timeZonesInDimension.size() > 1) {
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, String.format("Found multiple time zone references in query: %", timeZonesInDimension));
            }
            tzKey = Optional.of(Iterables.getOnlyElement(timeZonesInDimension));
        }
        request.put("table", tableHandle.map(AresDbTableHandle::getTableName).orElseThrow(() -> new AresDbException(ARESDB_QUERY_GENERATOR_FAILURE, null, "Table name not encountered yet")));
        request.put("measures", measuresJson);
        request.put("dimensions", dimensionsJson);
        tzKey.ifPresent(t -> request.put("timeZone", t.getId()));

        List<String> filters = new ArrayList<>(this.filters);
        addTimeFilter(session, request, filters, aresDbFilterExpressionConverter, domainTranslator);

        if (!filters.isEmpty()) {
            JSONArray filterJson = new JSONArray();
            filters.forEach(filterJson::add);
            request.put("rowFilters", filterJson);
        }

        Integer limit = (this.limit.isPresent() ? this.limit.getAsInt() : null);
        if (!hasAggregation() && limit == null) {
            limit = -1;
        }
        if (limit != null) {
            request.put("limit", limit);
        }

        JSONArray requestJoins = new JSONArray(joins.size());
        for (JoinInfo joinInfo : joins) {
            JSONObject join = new JSONObject();
            join.put("alias", joinInfo.alias);
            join.put("table", joinInfo.name);
            JSONArray conditions = new JSONArray();
            conditions.addAll(joinInfo.conditions);
            join.put("conditions", conditions);
            requestJoins.add(join);
        }
        if (!requestJoins.isEmpty()) {
            request.put("joins", requestJoins);
        }

        String aql = new JSONObject(ImmutableMap.of("queries", new JSONArray(ImmutableList.of(request)))).toJSONString();

        return new AugmentedAQL(aql, getReturnedAQLExpressions());
    }

    private List<AQLExpression> getReturnedAQLExpressions()
    {
        LinkedHashMap<VariableReferenceExpression, Selection> expressionsInOrder = new LinkedHashMap<>();
        if (!groupByColumns.isEmpty()) {
            // Sanity check
            for (VariableReferenceExpression groupByColumn : groupByColumns) {
                if (!selections.containsKey(groupByColumn)) {
                    throw new IllegalStateException(format("Group By column (%s) definition not found in input selections: ",
                            groupByColumn, Joiner.on(",").withKeyValueSeparator(":").join(selections)));
                }
            }

            // first add the time bucketizer group by columns
            for (VariableReferenceExpression groupByColumn : groupByColumns) {
                Selection groupByColumnDefinition = selections.get(groupByColumn);
                if (groupByColumnDefinition.getTimeTokenizer().isPresent()) {
                    expressionsInOrder.put(groupByColumn, groupByColumnDefinition);
                }
            }

            // next add the non-time bucketizer group by columns
            for (VariableReferenceExpression groupByColumn : groupByColumns) {
                Selection groupByColumnDefinition = selections.get(groupByColumn);
                if (!groupByColumnDefinition.getTimeTokenizer().isPresent()) {
                    expressionsInOrder.put(groupByColumn, groupByColumnDefinition);
                }
            }
            // Group by columns come first and have already been added above
            // so we are adding the metrics below
            expressionsInOrder.putAll(selections);
        }
        else {
            expressionsInOrder.putAll(selections);
        }

        ImmutableList.Builder<AQLExpression> outputInfos = ImmutableList.builder();
        for (Map.Entry<VariableReferenceExpression, Selection> expression : expressionsInOrder.entrySet()) {
            outputInfos.add(new AQLExpression(expression.getKey().getName(), expression.getValue().getTimeTokenizer(),
                    hiddenColumnSet.contains(expression.getKey())));
        }
        return outputInfos.build();
    }

    public static List<AresDbOutputInfo> getIndicesMappingFromAresDbSchemaToPrestoSchema(List<AQLExpression> expressionsInOrder, List<AresDbColumnHandle> handles)
    {
        checkState(handles.size() == visibleExpressionsCount(expressionsInOrder),
                "Expected returned visible expressions %s to match column handles %s",
                Joiner.on(",").join(expressionsInOrder), Joiner.on(",").join(handles));
        Map<String, Integer> nameToIndex = new HashMap<>();
        for (int i = 0; i < handles.size(); ++i) {
            String columnName = handles.get(i).getColumnName();
            Integer prev = nameToIndex.put(columnName.toLowerCase(ENGLISH), i);
            if (prev != null) {
                throw new IllegalStateException(format("Expected AresDB column handle %s to occur only once, but we have: %s", columnName, Joiner.on(",").join(handles)));
            }
        }
        ImmutableList.Builder<AresDbOutputInfo> outputInfos = ImmutableList.builder();
        expressionsInOrder.forEach(expression -> {
            Integer index = nameToIndex.get(expression.getName());
            if (expression.isHidden()) {
                index = -1;
            }
            if (index == null) {
                throw new IllegalStateException(format("Expected to find a AresDB column handle for the expression %s, but we have %s",
                        expression, Joiner.on(",").withKeyValueSeparator(":").join(nameToIndex)));
            }
            outputInfos.add(new AresDbOutputInfo(index, expression.getTimeTokenizer(), expression.isHidden()));
        });
        return outputInfos.build();
    }

    private static long visibleExpressionsCount(List<AQLExpression> expressions)
    {
        return expressions.stream().filter(aqlExpression -> !aqlExpression.isHidden()).count();
    }

    public static class TimeFilterParsed
    {
        private final String timeColumnName;
        private final long from;
        private final Optional<Long> to;
        private final Optional<RowExpression> extra;

        public TimeFilterParsed(String timeColumnName, long from, Optional<Long> to, Optional<RowExpression> extra)
        {
            this.timeColumnName = timeColumnName;
            this.from = from;
            this.to = to;
            this.extra = extra;
        }

        public long getBoundInSeconds()
        {
            return to.orElseThrow(() -> new RuntimeException("To timebound is absent")) - from;
        }

        public long getFrom()
        {
            return from;
        }

        public Optional<Long> getTo()
        {
            return to;
        }

        public Optional<RowExpression> getExtra()
        {
            return extra;
        }

        public String getTimeColumnName()
        {
            return timeColumnName;
        }
    }

    private static class TimeDomainConsumerHelper
    {
        final String timeColumn;
        Optional<Long> from = Optional.empty();
        Optional<Long> to = Optional.empty();
        boolean none;
        boolean needsRowFilter;

        public TimeDomainConsumerHelper(String timeColumn)
        {
            this.timeColumn = timeColumn;
        }

        void consumeRanges(Ranges ranges)
        {
            if (ranges.getRangeCount() == 0) {
                none = true;
            }
            else if (ranges.getRangeCount() == 1) {
                Range range = ranges.getOrderedRanges().get(0);
                from = range.getLow().getValueOptional().map(value -> objectToTimeLiteral(value, true));
                to = range.getHigh().getValueOptional().map(value -> objectToTimeLiteral(value, false));
            }
            else if (ranges.getRangeCount() > 1) {
                Range span = ranges.getSpan();
                from = span.getLow().isLowerUnbounded() ? Optional.empty() : Optional.of(objectToTimeLiteral(span.getLow().getValue(), true));
                to = span.getHigh().isUpperUnbounded() ? Optional.empty() : Optional.of(objectToTimeLiteral(span.getHigh().getValue(), false));
                needsRowFilter = bound() > ranges.getRangeCount();
            }
        }

        private long bound()
        {
            // This may be ill defined if 'to' is not present
            return (to.get() - from.get() + 1);
        }

        void consumeDiscreteValues(DiscreteValues discreteValues)
        {
            List<Object> valuesSorted = discreteValues.getValues().stream().sorted(Comparator.comparingDouble(literal -> ((Number) literal).doubleValue())).collect(toImmutableList());
            from = Optional.of(objectToTimeLiteral(valuesSorted.get(0), true));
            to = Optional.of(objectToTimeLiteral(valuesSorted.get(valuesSorted.size() - 1), false));
            needsRowFilter = bound() > valuesSorted.size();
        }

        void consumeAllOrNone(AllOrNone allOrNone)
        {
            if (!allOrNone.isAll()) {
                none = true;
            }
        }

        public void restrict(Optional<TimeFilterParsed> retentionTime)
        {
            if (none || !retentionTime.isPresent()) {
                return;
            }
            long retentionFrom = retentionTime.get().getFrom();
            from = Optional.of(from.orElse(retentionFrom));
            if (retentionTime.get().getTo().isPresent()) {
                to = Optional.of(to.orElse(retentionTime.get().getTo().get()));
            }
        }

        // Its important to bound the "from" part. The time must be >= than something. Its okay if it does not have a to
        public Optional<TimeFilterParsed> get(Supplier<Optional<RowExpression>> extraSupplier)
        {
            if (from.isPresent()) {
                Optional<RowExpression> extra = needsRowFilter ? extraSupplier.get() : Optional.empty();
                return Optional.of(new TimeFilterParsed(timeColumn, from.get(), to, extra));
            }
            else {
                return Optional.empty();
            }
        }
    }

    private void addTimeFilter(Optional<ConnectorSession> session, JSONObject request, List<String> rowFilters,
                               AresDbFilterExpressionConverter aresDbFilterExpressionConverter, DomainTranslator domainTranslator)
    {
        getParsedTimeFilter(session, domainTranslator).ifPresent(timeFilterParsed -> {
            JSONObject timeFilterJson = new JSONObject();
            timeFilterJson.put("column", timeFilterParsed.getTimeColumnName());
            timeFilterJson.put("from", String.format("%d", timeFilterParsed.from));
            timeFilterParsed.to.ifPresent(to -> timeFilterJson.put("to", String.format("%d", to)));
            request.put("timeFilter", timeFilterJson);
            timeFilterParsed.getExtra().ifPresent(extraPd -> rowFilters.add(extraPd.accept(aresDbFilterExpressionConverter, selections::get).getDefinition()));
        });
    }

    public Optional<TimeFilterParsed> getParsedTimeFilter(Optional<ConnectorSession> session, DomainTranslator domainTranslator)
    {
        Optional<TimeFilterParsed> retentionTime = retentionTimeFilter(session, tableHandle.get());
        Optional<String> timeColumnName = getTimeColumn();
        if (!timeColumnName.isPresent() || (timeFilter.map(Domain::isAll).orElse(true))) {
            return retentionTime;
        }
        ValueSet values = timeFilter.get().getValues();
        String timeColumn = timeColumnName.get();
        TimeDomainConsumerHelper helper = new TimeDomainConsumerHelper(timeColumn);
        values.getValuesProcessor().consume(
                ranges -> helper.consumeRanges(ranges),
                discreteValues -> helper.consumeDiscreteValues(discreteValues),
                allOrNone -> helper.consumeAllOrNone(allOrNone));
        helper.restrict(retentionTime);
        Map<VariableReferenceExpression, Domain> domainMap = new HashMap<>();
        domainMap.put(new VariableReferenceExpression(timeColumn.toLowerCase(ENGLISH), BIGINT), timeFilter.get());
        return helper.get(() -> Optional.of(domainTranslator.toPredicate(TupleDomain.withColumnDomains(domainMap))));
    }

    private static long objectToTimeLiteral(Object literal, boolean isLow)
    {
        if (!(literal instanceof Number)) {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, String.format("Expected the tuple domain bound %s to be a number when extracting time bounds", literal));
        }
        double num = ((Number) literal).doubleValue();
        return (long) (isLow ? Math.floor(num) : Math.ceil(num));
    }

    private static ISOChronology getChronology(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            return ISOChronology.getInstanceUTC();
        }
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();
        DateTimeZone dateTimeZone = DateTimeZone.forID(timeZoneKey.getId());
        return ISOChronology.getInstance(dateTimeZone);
    }

    // Stolen from com.facebook.presto.operator.scalar.DateTimeFunctions.localTime
    public static long getSessionStartTimeInMs(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            return session.getStartTime();
        }
        else {
            return getChronology(session).getZone().convertUTCToLocal(session.getStartTime());
        }
    }

    private static Optional<TimeFilterParsed> retentionTimeFilter(Optional<ConnectorSession> session, AresDbTableHandle tableHandle)
    {
        Optional<Type> timeStampType = tableHandle.getTimeStampType();
        Optional<String> timeColumnName = tableHandle.getTimeColumnName();
        Optional<Duration> retention = tableHandle.getRetention();
        if (!timeColumnName.isPresent() || !retention.isPresent() || !timeStampType.isPresent() || !session.isPresent()) {
            return Optional.empty();
        }
        long sessionStartTimeInMs = getSessionStartTimeInMs(session.get());
        long retentionInstantInMs = getChronology(session.get()).seconds().subtract(sessionStartTimeInMs, retention.get().roundTo(TimeUnit.SECONDS));
        long lowTimeSeconds = retentionInstantInMs / 1000; // round down
        long highTimeSeconds = (sessionStartTimeInMs + 999) / 1000; // round up
        return Optional.of(new TimeFilterParsed(timeColumnName.get(), lowTimeSeconds, Optional.of(highTimeSeconds), Optional.empty()));
    }

    public boolean isInputTooBig(Optional<ConnectorSession> session, DomainTranslator domainTranslator)
    {
        Optional<TimeFilterParsed> timeFilterParsed = getParsedTimeFilter(session, domainTranslator);
        if (!timeFilterParsed.isPresent() || !session.isPresent()) {
            // Assume global tables without retention are small
            return false;
        }
        Optional<Duration> singleSplitLimit = AresDbSessionProperties.getSingleSplitLimit(session.get());
        return singleSplitLimit.map(limit -> timeFilterParsed.get().getBoundInSeconds() > limit.roundTo(TimeUnit.SECONDS)).orElse(false);
    }

    public enum Origin
    {
        TABLE,
        LITERAL,
        DERIVED,
    }

    public static class Selection
    {
        private String definition;
        private Origin origin;
        private Optional<TimeSpec> timeTokenizer;

        public Selection(String definition, Origin origin, Optional<TimeSpec> timeTokenizer)
        {
            this.definition = definition;
            this.origin = origin;
            this.timeTokenizer = timeTokenizer;
        }

        public static Selection of(String definition, Origin origin)
        {
            return new Selection(definition, origin, Optional.empty());
        }

        public static Selection of(String definition, Origin origin, Optional<TimeSpec> timeTokenizer)
        {
            return new Selection(definition, origin, timeTokenizer);
        }

        public String getDefinition()
        {
            return definition;
        }

        public Origin getOrigin()
        {
            return origin;
        }

        public Optional<TimeSpec> getTimeTokenizer()
        {
            return timeTokenizer;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("definition", definition)
                    .add("origin", origin)
                    .add("timeTokenizer", timeTokenizer)
                    .toString();
        }
    }

    public static class JoinInfo
    {
        private final String alias;
        private final String name;
        private final List<String> conditions;

        public JoinInfo(String alias, String name, List<String> conditions)
        {
            this.alias = alias;
            this.name = name;
            this.conditions = conditions;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("alias", alias)
                    .add("name", name)
                    .add("conditions", conditions)
                    .toString();
        }
    }

    public static class AresDbOutputInfo
    {
        public final int index;
        public final Optional<TimeSpec> timeBucketizer;
        public final Boolean isHidden;

        public AresDbOutputInfo(int index, Optional<TimeSpec> timeBucketizer, Boolean isHidden)
        {
            this.index = index;
            this.timeBucketizer = timeBucketizer;
            this.isHidden = isHidden;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AresDbOutputInfo that = (AresDbOutputInfo) o;
            return index == that.index &&
                    Objects.equals(timeBucketizer, that.timeBucketizer) &&
                    Objects.equals(isHidden, that.isHidden);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(index, timeBucketizer, isHidden);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("index", index)
                    .add("timeBucketizer", timeBucketizer)
                    .add("isHidden", isHidden)
                    .toString();
        }
    }
}
