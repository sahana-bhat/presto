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

import com.facebook.airlift.configuration.Config;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class AresDbConfig
{
    private Duration metadataCacheExpiry = new Duration(1, TimeUnit.DAYS);
    private String serviceUrl;
    private String callerHeaderParam = "RPC-Caller";
    private String callerHeaderValue = "presto";
    private String serviceHeaderParam = "RPC-Service";
    private String serviceName;
    private Map<String, String> extraHttpHeaders = ImmutableMap.of();
    private Optional<Duration> fetchTimeout = Optional.empty();

    // The default data cache duration of 0 seconds, means that the ares page cache is evicted entirely
    // based on size bounds and not time.
    private Duration cacheDuration = new Duration(0, TimeUnit.SECONDS);
    private DataSize maxCacheSize = new DataSize(0, DataSize.Unit.BYTE); // Default is not to cache
    private Optional<Duration> unsafeToCacheInterval = Optional.empty();

    private int maxLimitWithoutAggregates = 25000;
    private Optional<Duration> singleSplitLimit = Optional.empty();
    private int maxNumOfSplits = 1;

    @MinDuration("0s")
    @NotNull
    public Duration getMetadataCacheExpiry()
    {
        return metadataCacheExpiry;
    }

    @Config("aresdb.metadata-expiry")
    public AresDbConfig setMetadataCacheExpiry(Duration metadataCacheExpiry)
    {
        this.metadataCacheExpiry = metadataCacheExpiry;
        return this;
    }

    @NotNull
    public String getServiceUrl()
    {
        return serviceUrl;
    }

    @Config("aresdb.service-url")
    public AresDbConfig setServiceUrl(String serviceUrl)
    {
        this.serviceUrl = serviceUrl;
        return this;
    }

    @NotNull
    public String getCallerHeaderParam()
    {
        return callerHeaderParam;
    }

    @Config("aresdb.caller-header-param")
    public AresDbConfig setCallerHeaderParam(String callerHeaderParam)
    {
        this.callerHeaderParam = callerHeaderParam;
        return this;
    }

    @NotNull
    public String getCallerHeaderValue()
    {
        return callerHeaderValue;
    }

    @Config("aresdb.caller-header-value")
    public AresDbConfig setCallerHeaderValue(String callerHeaderValue)
    {
        this.callerHeaderValue = callerHeaderValue;
        return this;
    }

    @NotNull
    public String getServiceHeaderParam()
    {
        return serviceHeaderParam;
    }

    @Config("aresdb.service-header-param")
    public AresDbConfig setServiceHeaderParam(String serviceHeaderParam)
    {
        this.serviceHeaderParam = serviceHeaderParam;
        return this;
    }

    @Nullable
    public String getServiceName()
    {
        return serviceName;
    }

    @Config("aresdb.service-name")
    public AresDbConfig setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
        return this;
    }

    @NotNull
    public Map<String, String> getExtraHttpHeaders()
    {
        return extraHttpHeaders;
    }

    @Config("aresdb.extra-http-headers")
    public AresDbConfig setExtraHttpHeaders(String headers)
    {
        extraHttpHeaders = ImmutableMap.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator(":").split(headers));
        return this;
    }

    @NotNull
    public Optional<Duration> getFetchTimeout()
    {
        return fetchTimeout;
    }

    @Config("aresdb.fetch-timeout")
    public AresDbConfig setFetchTimeout(Duration fetchTimeout)
    {
        this.fetchTimeout = Optional.ofNullable(fetchTimeout);
        return this;
    }

    public Duration getCacheDuration()
    {
        return cacheDuration;
    }

    @Config("aresdb.cache-duration")
    public AresDbConfig setCacheDuration(Duration cacheDuration)
    {
        this.cacheDuration = cacheDuration;
        return this;
    }

    @NotNull
    public DataSize getMaxCacheSize()
    {
        return maxCacheSize;
    }

    @Config("aresdb.max-cache-size")
    public AresDbConfig setMaxCacheSize(DataSize maxCacheSize)
    {
        this.maxCacheSize = maxCacheSize;
        return this;
    }

    @NotNull
    public Optional<Duration> getUnsafeToCacheInterval()
    {
        return unsafeToCacheInterval;
    }

    @Config("aresdb.unsafe-to-cache-interval")
    public AresDbConfig setUnsafeToCacheInterval(Duration unsafeToCacheInterval)
    {
        this.unsafeToCacheInterval = Optional.ofNullable(unsafeToCacheInterval);
        return this;
    }

    @NotNull
    public int getMaxLimitWithoutAggregates()
    {
        return maxLimitWithoutAggregates;
    }

    @Config("aresdb.max-limit-without-aggregates")
    public AresDbConfig setMaxLimitWithoutAggregates(int maxLimitWithoutAggregates)
    {
        this.maxLimitWithoutAggregates = maxLimitWithoutAggregates;
        return this;
    }

    @NotNull
    public Optional<Duration> getSingleSplitLimit()
    {
        return singleSplitLimit;
    }

    @Config("aresdb.single-split-limit")
    public AresDbConfig setSingleSplitLimit(Duration singleSplitLimit)
    {
        this.singleSplitLimit = Optional.ofNullable(singleSplitLimit);
        return this;
    }

    public int getMaxNumOfSplits()
    {
        return maxNumOfSplits;
    }

    @Config("aresdb.max-num-of-splits")
    public AresDbConfig setMaxNumOfSplits(int maxNumOfSplits)
    {
        this.maxNumOfSplits = maxNumOfSplits;
        return this;
    }
}
