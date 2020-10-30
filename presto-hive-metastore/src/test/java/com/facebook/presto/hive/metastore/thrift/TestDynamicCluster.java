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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.units.Duration;
import org.apache.thrift.TException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestDynamicCluster
{
    private final HiveMetastoreClient metastoreClient = createFakeMetastoreClient();
    private final DynamicMetastoreConfig validConfig = new DynamicMetastoreConfig()
                        .setMetastoreDiscoveryUri("{\"url\":\"http://localhost:5436/discover\",\"headers\":{\"Rpc-Service\":\"hms-random\",\"Rpc-Caller\":\"presto\"}}");

    public TestDynamicCluster() throws TException {}

    @Test
    public void testFallbackHiveMetastore()
            throws TException
    {
        HiveCluster cluster = createHiveCluster(validConfig, asList(metastoreClient));
        assertEquals(cluster.createMetastoreClient(null, null), metastoreClient);
    }

    private static HiveCluster createHiveCluster(DynamicMetastoreConfig config, List<HiveMetastoreClient> clients)
    {
        return new DynamicHiveCluster(config, new MockHiveMetastoreClientFactory(Optional.empty(), new Duration(1, SECONDS), clients), new JettyHttpClient());
    }

    private static HiveMetastoreClient createFakeMetastoreClient()
    {
        return new MockHiveMetastoreClient();
    }
}