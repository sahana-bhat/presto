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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Request.Builder;
import com.facebook.airlift.http.client.StringResponseHandler;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import com.google.common.util.concurrent.FluentFuture;

import javax.inject.Inject;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_HTTP_ERROR;
import static com.facebook.presto.aresdb.AresDbUtils.isValidAresDbHttpResponseCode;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class AresDbConnection
{
    private final AresDbConfig aresDbConfig;
    private final HttpClient httpClient;

    private final LoadingCache<String, AresDbTable> aresDbTableCache;
    private final Supplier<List<String>> allTablesCache;
    private final AresDbMetrics aresDbMetrics;
    private final Ticker ticker = Ticker.systemTicker();
    private ScheduledExecutorService timeoutExecutor;

    @Inject
    public AresDbConnection(AresDbConfig aresDbConfig, @ForAresDb HttpClient httpClient, AresDbMetrics aresDbMetrics, @ForAresDb ScheduledExecutorService timeoutExecutor)
    {
        final long cacheExpiryMs = aresDbConfig.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS);
        this.aresDbConfig = aresDbConfig;
        this.httpClient = httpClient;
        this.aresDbMetrics = aresDbMetrics;
        this.timeoutExecutor = timeoutExecutor;

        this.allTablesCache = Suppliers.memoizeWithExpiration(
                () -> {
                    try {
                        return ImmutableList.of("rta_eats_order");
                    }
                    catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                },
                cacheExpiryMs,
                TimeUnit.MILLISECONDS);

        this.aresDbTableCache =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(cacheExpiryMs, TimeUnit.MILLISECONDS)
                        .build(new CacheLoader<String, AresDbTable>()
                        {
                            @Override
                            public AresDbTable load(String tableName)
                                    throws Exception
                            {
                                List<AresDbColumn> columns = new ArrayList<>();
                                columns.add(new AresDbColumn("workflowUUID", VARCHAR, false));
                                columns.add(new AresDbColumn("createdAt", BIGINT, true));
                                columns.add(new AresDbColumn("vehicleViewId", BIGINT, false));
                                columns.add(new AresDbColumn("tenancy", VARCHAR, false));
                                return new AresDbTable(tableName, columns);
                            }
                        });
    }

    public AresDbTable getTable(String tableName)
    {
        try {
            return aresDbTableCache.get(tableName);
        }
        catch (Exception e) {
            // TODO: check for NOT_FOUND and throw better exceptions
            throw new AresDbException(ARESDB_HTTP_ERROR, "Failed to get table info from AresDb: " + tableName);
        }
    }

    public List<String> getTables()
    {
        try {
            return allTablesCache.get();
        }
        catch (Exception e) {
            // TODO: check for NOT_FOUND and throw better exceptions
            throw new AresDbException(ARESDB_HTTP_ERROR, "Failed to get list of tables from AresDB");
        }
    }

    public Optional<String> getTimeColumn(String tableName)
    {
        try {
            AresDbTable aresDbTable = aresDbTableCache.get(tableName);
            for (AresDbColumn column : aresDbTable.getColumns()) {
                if (column.isTimeColumn()) {
                    return Optional.of(column.getName());
                }
            }

            return Optional.empty();
        }
        catch (Exception e) {
            // TODO: check for NOT_FOUND and throw better exceptions
            throw new AresDbException(ARESDB_HTTP_ERROR, "Failed to get table info from AresDb: " + tableName);
        }
    }

    public FluentFuture<String> queryAndGetResultsAsync(String aql, int aqlIndex, int splitIndex, ConnectorSession session)
    {
        Builder requestBuilder = Request.builder()
                .prepareGet()
                .setUri(URI.create("http://" + aresDbConfig.getServiceUrl() + "/query/aql"));

        requestBuilder = requestBuilder.setBodyGenerator(createStaticBodyGenerator(aql, StandardCharsets.UTF_8));
        requestBuilder.setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .setHeader(aresDbConfig.getCallerHeaderParam(), aresDbConfig.getCallerHeaderValue())
                .setHeader(aresDbConfig.getServiceHeaderParam(), aresDbConfig.getServiceName());
        for (Map.Entry<String, String> entry : aresDbConfig.getExtraHttpHeaders().entrySet()) {
            requestBuilder.setHeader(entry.getKey(), entry.getValue());
        }
        Request request = requestBuilder.build();
        long startTime = ticker.read();
        session.getSessionLogger().log(() -> String.format("Aql Issue Start %d of split %d", aqlIndex, splitIndex));
        FluentFuture<StringResponseHandler.StringResponse> future = FluentFuture.from(httpClient.executeAsync(request, createStringResponseHandler()));
        return aresDbConfig.getFetchTimeout()
                .map(timeout -> future.withTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS, timeoutExecutor))
                .orElse(future)
                .transform((stringResponse) -> {
                    long duration = ticker.read() - startTime;
                    aresDbMetrics.monitorRequest(request, stringResponse, duration, TimeUnit.NANOSECONDS);
                    session.getSessionLogger().log(() -> String.format("Aql Issue End %d of split %d", aqlIndex, splitIndex));
                    if (isValidAresDbHttpResponseCode(stringResponse.getStatusCode())) {
                        return stringResponse.getBody();
                    }
                    else {
                        throw new AresDbException(ARESDB_HTTP_ERROR,
                                String.format("Unexpected response status from AresDB: status code: %s, error: %s, url: %s, headers: %s", stringResponse.getStatusCode(), stringResponse.getBody(), request.getUri(), request.getHeaders()),
                                aql);
                    }
                }, directExecutor());
    }
}
