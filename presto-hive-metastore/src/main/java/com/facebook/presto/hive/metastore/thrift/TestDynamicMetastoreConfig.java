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

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestDynamicMetastoreConfig
{
    @Test
    public void testMetastoreDiscoveryUri() throws TException
    {
        String url = "http://localhost:5436/discover";
        Map<String, String> headers = new HashMap<>();
        headers.put("Rpc-Service", "hms-random");
        headers.put("Rpc-Caller", "presto");
        ThriftMetastoreHttpRequestDetails metastoreDiscoveryUri = new ThriftMetastoreHttpRequestDetails(url, headers);
        DynamicMetastoreConfig expected = new DynamicMetastoreConfig()
                .setMetastoreDiscoveryUri(metastoreDiscoveryUri)
                .setMetastoreUsername("presto")
                .setMetastoreDiscoveryType("dynamic");

        assertEquals(expected.getMetastoreDiscoveryType(), "dynamic");
        assertEquals(expected.getMetastoreDiscoveryUri().getUrl(), "http://localhost:5436/discover");
        assertEquals(expected.getMetastoreDiscoveryUri().getHeaders().get("Rpc-Service"), "hms-random");
        assertEquals(expected.getMetastoreDiscoveryUri().getHeaders().get("Rpc-Caller"), "presto");
        assertEquals(expected.getMetastoreUsername(), "presto");
    }
}
