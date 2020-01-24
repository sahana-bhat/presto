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
package com.facebook.presto.rta;

import com.facebook.presto.rta.schema.RTADeployment;
import com.facebook.presto.rta.schema.TestSchemaUtils;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.collect.Iterators.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRtaPropertyManager
{
    private RtaConfig rtaConfig;

    @BeforeTest
    public void setUp()
            throws URISyntaxException
    {
        URL resource = TestRtaPropertyManager.class.getResource("/rta_config.json");
        rtaConfig = new RtaConfig().setConfigFile(Paths.get(resource.toURI()).toString());
    }

    @Test
    public void testDatacenterSorting()
            throws IOException
    {
        RtaPropertyManager rtaPropertyManager = new RtaPropertyManager(rtaConfig.setDataCenterOverride("phx8"));
        List<RTADeployment> rtaDeployments = rtaPropertyManager.winningDeployment(ImmutableList.of(
//                new RTADeployment("aresdb", "nocare", "nocare", "nocare", "dca1", null),
                new RTADeployment("pinot", "nocare", "nocare", "pinot-cluster-a", "phx7", null),
//                new RTADeployment("aresdb", "nocare", "nocare", "nocare", "phx7", null),
//                new RTADeployment("aresdb", "nocare", "nocare", "nocare", "phx8", null),
                new RTADeployment("pinot", "nocare", "nocare", "pinot-cluster-b", "pHX8", null),
                new RTADeployment("pinot", "somecare", "somecare", "pinot-cluster-b", "Phx8", null)));
        assertEquals(rtaDeployments.size(), 2);
        assertEquals(getOnlyElement(rtaDeployments.stream().map(RTADeployment::getCluster).distinct().iterator()), "pinot-cluster-b");
        RTADeployment picked = rtaDeployments.get(0);
        assertEquals(picked.getStorageType(), RtaStorageType.PINOT);
        assertTrue(picked.getDataCenter().equalsIgnoreCase("phx8"));
    }

//    @Test
//    public void testDatacenterSortingRealWorld()
//            throws IOException
//    {
//        ImmutableList<RTADeployment> deployments = ImmutableList.of(
//                new RTADeployment("aresdb", "bi", "bi_latam_eats_trips", "prodj", "phx2", null),
//                new RTADeployment("aresdb", "bi", "bi_latam_eats_trips", "prodj", "dca1", null));
//        for (Map.Entry<String, String> entry : ImmutableMap.of("dca1", "dca1", "phx3", "phx2").entrySet()) {
//            String deploymentDc = entry.getKey();
//            String expectedDc = entry.getValue();
//            RtaPropertyManager rtaPropertyManager = new RtaPropertyManager(rtaConfig.setDataCenterOverride(deploymentDc));
//            RTADeployment picked = getOnlyElement(rtaPropertyManager.winningDeployment(deployments).iterator());
//            Assert.assertEquals(picked.getStorageType(), RtaStorageType.ARESDB);
//            Assert.assertTrue(picked.getDataCenter().equalsIgnoreCase(expectedDc), String.format("Expected: %s, Got: %s", expectedDc, picked));
//        }
//    }

    @Test
    public void testGetDefaultDeployment()
            throws IOException
    {
        RTADeployment deployment = getOnlyElement(new RtaPropertyManager(rtaConfig).winningDeployment(TestSchemaUtils.getDeployments()).iterator());
        assertEquals(deployment.getCluster(), "stagingb");
    }
}
