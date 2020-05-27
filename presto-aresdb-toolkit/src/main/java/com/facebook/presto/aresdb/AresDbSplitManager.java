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

import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.aresdb.AresDbQueryGenerator.AresDbQueryGeneratorResult;
import com.facebook.presto.aresdb.AresDbQueryGenerator.AugmentedAQL;
import com.facebook.presto.aresdb.query.AresDbFilterExpressionConverter;
import com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext;
import com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.TimeFilterParsed;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.facebook.presto.spi.ErrorType.USER_ERROR;
import static java.util.Objects.requireNonNull;

public class AresDbSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final CounterStat cachedSplits = new CounterStat();
    private final CounterStat splits = new CounterStat();
    private final DomainTranslator domainTranslator;
    private final AresDbFilterExpressionConverter aresDbFilterExpressionConverter;

    @Inject
    public AresDbSplitManager(ConnectorId connectorId,
                              TypeManager typeManager,
                              FunctionMetadataManager functionMetadataManager,
                              StandardFunctionResolution standardFunctionResolution,
                              RowExpressionService rowExpressionService)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.aresDbFilterExpressionConverter = new AresDbFilterExpressionConverter(typeManager, functionMetadataManager, standardFunctionResolution);
        this.domainTranslator = requireNonNull(rowExpressionService, "rowExpressionService is null").getDomainTranslator();
    }

    @Managed
    @Nested
    public CounterStat getCachedSplits()
    {
        return cachedSplits;
    }

    @Managed
    @Nested
    public CounterStat getSplits()
    {
        return splits;
    }

    public enum QueryNotAdequatelyPushedDownErrorCode
            implements ErrorCodeSupplier
    {
        AQL_NOT_PRESENT(1, USER_ERROR, "Query uses unsupported expressions that cannot be pushed into the storage engine. Please see https://XXX for more details");

        private final ErrorCode errorCode;

        QueryNotAdequatelyPushedDownErrorCode(int code, ErrorType type, String guidance)
        {
            errorCode = new ErrorCode(code + 0x0625_0000, name() + ": " + guidance, type);
        }

        @Override
        public ErrorCode toErrorCode()
        {
            return errorCode;
        }
    }

    public static class QueryNotAdequatelyPushedDownException
            extends PrestoException
    {
        private final String connectorId;
        private final ConnectorTableHandle connectorTableHandle;

        public QueryNotAdequatelyPushedDownException(
                QueryNotAdequatelyPushedDownErrorCode errorCode,
                ConnectorTableHandle connectorTableHandle,
                String connectorId)
        {
            super(requireNonNull(errorCode, "error code is null"), (String) null);
            this.connectorId = requireNonNull(connectorId, "connector id is null");
            this.connectorTableHandle = requireNonNull(connectorTableHandle, "connector table handle is null");
        }

        @Override
        public String getMessage()
        {
            return super.getMessage() + String.format(" table: %s:%s", connectorId, connectorTableHandle);
        }
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
                                          ConnectorSession session,
                                          ConnectorTableLayoutHandle layout,
                                          SplitSchedulingContext splitSchedulingContext)
    {
        AresDbTableLayoutHandle aresDbTableLayoutHandle = (AresDbTableLayoutHandle) layout;
        AresDbTableHandle aresDbTableHandle = aresDbTableLayoutHandle.getTable();
        Supplier<PrestoException> exceptionSupplier = () -> new QueryNotAdequatelyPushedDownException(QueryNotAdequatelyPushedDownErrorCode.AQL_NOT_PRESENT, aresDbTableHandle, connectorId);
        if (!aresDbTableHandle.getIsQueryShort().isPresent() || !aresDbTableHandle.getIsQueryShort().get()) {
            throw (exceptionSupplier.get());
        }
        return generateAresDbSplits(aresDbTableHandle, aresDbTableHandle.getGeneratedResult().orElseThrow(exceptionSupplier), session);
    }

    private ConnectorSplitSource generateAresDbSplits(AresDbTableHandle aresDbTableHandle, AresDbQueryGeneratorResult aresDbQueryGeneratorResult, ConnectorSession session)
    {
        Optional<Type> timeStampType = aresDbTableHandle.getTimeStampType();
        AresDbQueryGeneratorContext aresDbQueryGeneratorContext = aresDbQueryGeneratorResult.getContext();
        Optional<TimeFilterParsed> parsedTimeFilter = aresDbQueryGeneratorContext.getParsedTimeFilter(Optional.of(session), domainTranslator);
        AugmentedAQL fullRequest = aresDbQueryGeneratorResult.getGeneratedAql();
        List<AresDbSplit.AresQL> aqls = new ArrayList<>();

        if (timeStampType.isPresent() && parsedTimeFilter.isPresent() && aresDbQueryGeneratorContext.isInputTooBig(Optional.of(session), domainTranslator) && aresDbQueryGeneratorContext.isPartialAggregation()) {
            long low = parsedTimeFilter.get().getFrom();
            long high = low + parsedTimeFilter.get().getBoundInSeconds();
            Duration singleSplitLimit = AresDbSessionProperties.getSingleSplitLimit(session).get();
            long limit = singleSplitLimit.roundTo(TimeUnit.SECONDS);
            long unsafeToCacheLowMark = AresDbSessionProperties.getUnsafeToCacheInterval(session).map(interval -> AresDbQueryGeneratorContext.getSessionStartTimeInMs(session) / 1000L - interval.roundTo(TimeUnit.SECONDS)).orElse(0L);

            do {
                long thisSplitHigh = low % limit == 0 ? low + limit : ((low + limit - 1) / limit) * limit;
                thisSplitHigh = Math.min(thisSplitHigh, high);
                AresDbQueryGeneratorContext withNewTimeFilter = aresDbQueryGeneratorContext.withNewTimeBound(Domain.create(ValueSet.ofRanges(new Range(Marker.above(timeStampType.get(), low), Marker.below(timeStampType.get(), thisSplitHigh))), false));
                // Only cache non partial splits that are not too fresh
                boolean cacheable = thisSplitHigh < unsafeToCacheLowMark && thisSplitHigh - low == limit;
                aqls.add(new AresDbSplit.AresQL(withNewTimeFilter.toQuery(Optional.of(session), aresDbFilterExpressionConverter, domainTranslator).getAql(), cacheable));
                low = thisSplitHigh;
            }
            while (low < high);

            Collections.shuffle(aqls, ThreadLocalRandom.current());
        }
        else {
            aqls.add(new AresDbSplit.AresQL(fullRequest.getAql(), false));
        }
        ImmutableList.Builder<AresDbSplit> splitBuilder = ImmutableList.builder();

        splits.update(aqls.size());
        cachedSplits.update(aqls.stream().filter(AresDbSplit.AresQL::isCacheable).count());

        int maxNumSplits = AresDbSessionProperties.getMaxNumOfSplits(session);
        int numAqlsPerSplit = aqls.size() > maxNumSplits ? aqls.size() / maxNumSplits : 1;
        Preconditions.checkState(numAqlsPerSplit >= 1);
        int aqlIdx = 0;
        int splitIndex = 0;
        while (aqlIdx < aqls.size()) {
            int newAqlIdx = Math.min(aqlIdx + numAqlsPerSplit, aqls.size());
            splitBuilder.add(new AresDbSplit(aresDbTableHandle, fullRequest.getExpressions(),
                    ImmutableList.copyOf(aqls.subList(aqlIdx, newAqlIdx)), splitIndex));
            aqlIdx = newAqlIdx;
            ++splitIndex;
        }
        return new FixedSplitSource(splitBuilder.build());
    }
}
