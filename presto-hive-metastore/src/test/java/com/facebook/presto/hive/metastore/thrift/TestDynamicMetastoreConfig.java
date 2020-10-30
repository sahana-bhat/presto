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

import org.apache.thrift.TException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestDynamicMetastoreConfig
{
    @Test
    public void testMetastoreDiscoveryUri() throws TException
    {
        DynamicMetastoreConfig expected = new DynamicMetastoreConfig()
                .setMetastoreDiscoveryUri("{\"url\":\"http://localhost:5436/discover\",\"headers\":{\"Rpc-Service\":\"hms-random\",\"Rpc-Caller\":\"presto\"}}")
                .setMetastoreUsername("presto")
                .setMetastoreDiscoveryType("dynamic");

        assertEquals(expected.getMetastoreDiscoveryType(), "dynamic");
        assertEquals(expected.getMetastoreDiscoveryUri().getUrl(), "http://localhost:5436/discover");
        assertEquals(expected.getMetastoreDiscoveryUri().getHeaders().get("Rpc-Service"), "hms-random");
        assertEquals(expected.getMetastoreDiscoveryUri().getHeaders().get("Rpc-Caller"), "presto");
        assertEquals(expected.getMetastoreUsername(), "presto");
    }

    @Test
    public void testMetastoreDiscoveryUriInvalid() throws TException
    {
        DynamicMetastoreConfig expected = new DynamicMetastoreConfig();
        assertThrows(TException.class, () -> expected.setMetastoreDiscoveryUri("invalid_json"));
    }

    @Test
    public void testMetastoreDiscoveryUriNull() throws TException
    {
        DynamicMetastoreConfig expected = new DynamicMetastoreConfig();
        assertThrows(TException.class, () -> expected.setMetastoreDiscoveryUri(null));
    }
}
