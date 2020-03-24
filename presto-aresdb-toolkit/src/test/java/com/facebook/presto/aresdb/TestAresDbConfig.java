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

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestAresDbConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(
                ConfigAssertions.recordDefaults(AresDbConfig.class)
                    .setMetadataCacheExpiry(new Duration(1, TimeUnit.DAYS))
                    .setServiceUrl(null)
                    .setCallerHeaderParam("RPC-Caller")
                    .setCallerHeaderValue("presto")
                    .setServiceHeaderParam("RPC-Service")
                    .setServiceName(null)
                    .setExtraHttpHeaders("")
                    .setFetchTimeout(null)
                    .setCacheDuration(new Duration(0, TimeUnit.SECONDS))
                    .setMaxCacheSize(new DataSize(0, DataSize.Unit.BYTE))
                    .setUnsafeToCacheInterval(null)
                    .setMaxLimitWithoutAggregates(25000)
                    .setSingleSplitLimit(null)
                    .setMaxNumOfSplits(1));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("aresdb.metadata-expiry", "3d")
                .put("aresdb.service-url", "host1:1111,host2:1111")
                .put("aresdb.caller-header-param", "RPC-Caller1")
                .put("aresdb.caller-header-value", "neutrino")
                .put("aresdb.service-header-param", "RPC-Service1")
                .put("aresdb.service-name", "neutrino")
                .put("aresdb.extra-http-headers", "k:v")
                .put("aresdb.fetch-timeout", "1m")
                .put("aresdb.cache-duration", "10m")
                .put("aresdb.max-cache-size", "5MB")
                .put("aresdb.unsafe-to-cache-interval", "2m")
                .put("aresdb.max-limit-without-aggregates", "10000")
                .put("aresdb.single-split-limit", "1h")
                .put("aresdb.max-num-of-splits", "10")
                .build();
        AresDbConfig expected = new AresDbConfig()
                .setMetadataCacheExpiry(new Duration(3, TimeUnit.DAYS))
                .setServiceUrl("host1:1111,host2:1111")
                .setCallerHeaderParam("RPC-Caller1")
                .setCallerHeaderValue("neutrino")
                .setServiceHeaderParam("RPC-Service1")
                .setServiceName("neutrino")
                .setExtraHttpHeaders("k:v")
                .setFetchTimeout(new Duration(1, TimeUnit.MINUTES))
                .setCacheDuration(new Duration(10, TimeUnit.MINUTES))
                .setMaxCacheSize(new DataSize(5, DataSize.Unit.MEGABYTE))
                .setUnsafeToCacheInterval(new Duration(2, TimeUnit.MINUTES))
                .setMaxLimitWithoutAggregates(10000)
                .setSingleSplitLimit(new Duration(1, TimeUnit.HOURS))
                .setMaxNumOfSplits(10);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
