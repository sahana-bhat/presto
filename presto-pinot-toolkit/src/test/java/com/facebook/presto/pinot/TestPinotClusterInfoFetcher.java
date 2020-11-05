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

package com.facebook.presto.pinot;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.http.client.testing.TestingResponse;
import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.MediaType;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPinotClusterInfoFetcher
{
    private static final String BROKER_RESPONSE = "{\n" +
            "  \"tableName\": \"dummy\",\n" +
            "  \"brokers\": [\n" +
            "    {\n" +
            "      \"tableType\": \"offline\",\n" +
            "      \"instances\": [\n" +
            "        \"Broker_dummy-broker-host1-datacenter1_6513\",\n" +
            "        \"Broker_dummy-broker-host2-datacenter1_6513\",\n" +
            "        \"Broker_dummy-broker-host4-datacenter1_6513\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"tableType\": \"realtime\",\n" +
            "      \"instances\": [\n" +
            "        \"Broker_dummy-broker-host1-datacenter1_6513\",\n" +
            "        \"Broker_dummy-broker-host2-datacenter1_6513\",\n" +
            "        \"Broker_dummy-broker-host3-datacenter1_6513\"\n" +
            "      ]\n" +
            "    }\n" +
            "  ],\n" +
            "  \"server\": [\n" +
            "    {\n" +
            "      \"tableType\": \"offline\",\n" +
            "      \"instances\": [\n" +
            "        \"Server_dummy-server-host8-datacenter1_7090\",\n" +
            "        \"Server_dummy-server-host9-datacenter1_7090\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"tableType\": \"realtime\",\n" +
            "      \"instances\": [\n" +
            "        \"Server_dummy-server-host7-datacenter1_7090\",\n" +
            "        \"Server_dummy-server-host4-datacenter1_7090\",\n" +
            "        \"Server_dummy-server-host5-datacenter1_7090\",\n" +
            "        \"Server_dummy-server-host6-datacenter1_7090\"\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    @Test
    public void testBrokersParsed()
    {
        HttpClient httpClient = new TestingHttpClient((request) -> TestingResponse.mockResponse(HttpStatus.OK, MediaType.JSON_UTF_8, BROKER_RESPONSE));
        PinotConfig pinotConfig = new PinotConfig()
                .setMetadataCacheExpiry(new Duration(1, TimeUnit.MILLISECONDS))
                .setControllerUrls("localhost:7900");
        PinotClusterInfoFetcher pinotClusterInfoFetcher = new PinotClusterInfoFetcher(pinotConfig, new PinotMetrics(), httpClient, MetadataUtil.TABLES_JSON_CODEC, MetadataUtil.BROKERS_FOR_TABLE_JSON_CODEC, MetadataUtil.ROUTING_TABLES_JSON_CODEC, MetadataUtil.ROUTING_TABLES_JSON_CODEC_V4, MetadataUtil.TIME_BOUNDARY_JSON_CODEC);
        ImmutableSet<String> brokers = ImmutableSet.copyOf(pinotClusterInfoFetcher.getAllBrokersForTable(new PinotClusterInfoFetcher.BrokerCacheKey("dummy", Optional.empty())));
        Assert.assertEquals(ImmutableSet.of("dummy-broker-host1-datacenter1:6513", "dummy-broker-host2-datacenter1:6513", "dummy-broker-host3-datacenter1:6513", "dummy-broker-host4-datacenter1:6513"), brokers);
    }

    @Test
    public void testRoutingTable()
    {
        HttpClient httpClientV1 = new TestingHttpClient((request) ->
                request.getUri().getPath().contains("instance")
                        ? TestingResponse.mockResponse(HttpStatus.OK, MediaType.JSON_UTF_8, BROKER_RESPONSE)
                        : TestingResponse.mockResponse(HttpStatus.OK, MediaType.JSON_UTF_8, "{\n" +
                            "  \"routingTableSnapshot\": [\n" +
                            "    {\n" +
                            "      \"tableName\": \"eats_job_state_REALTIME\",\n" +
                            "      \"routingTableEntries\": [\n" +
                            "        {\n" +
                            "          \"Server_streampinot-sandbox01-phx3_7090\": [\n" +
                            "            \"eats_job_state__2__86__20201103T2337Z\",\n" +
                            "            \"eats_job_state__1__91__20201104T1452Z\",\n" +
                            "            \"eats_job_state__2__88__20201104T0930Z\",\n" +
                            "            \"eats_job_state__1__88__20201103T2351Z\",\n" +
                            "            \"eats_job_state__3__88__20201104T0458Z\",\n" +
                            "            \"eats_job_state__0__85__20201103T1842Z\",\n" +
                            "            \"eats_job_state__2__87__20201104T0430Z\",\n" +
                            "            \"eats_job_state__1__89__20201104T0452Z\"\n" +
                            "          ],\n" +
                            "          \"Server_streampinot-sandbox06-phx3_7090\": [\n" +
                            "            \"eats_job_state__3__89__20201104T0958Z\",\n" +
                            "            \"eats_job_state__0__89__20201104T1409Z\",\n" +
                            "            \"eats_job_state__2__89__20201104T1431Z\",\n" +
                            "            \"eats_job_state__1__90__20201104T0952Z\",\n" +
                            "            \"eats_job_state__0__88__20201104T0908Z\",\n" +
                            "            \"eats_job_state__0__86__20201103T2325Z\",\n" +
                            "            \"eats_job_state__0__90__20201104T1909Z\",\n" +
                            "            \"eats_job_state__1__87__20201103T1906Z\"\n" +
                            "          ],\n" +
                            "          \"Server_streampinot-sandbox03-phx3_7090\": [\n" +
                            "            \"eats_job_state__2__85__20201103T1853Z\",\n" +
                            "            \"eats_job_state__1__92__20201104T1938Z\",\n" +
                            "            \"eats_job_state__3__90__20201104T1458Z\",\n" +
                            "            \"eats_job_state__2__90__20201104T1927Z\",\n" +
                            "            \"eats_job_state__3__87__20201103T2358Z\",\n" +
                            "            \"eats_job_state__3__91__20201104T1941Z\",\n" +
                            "            \"eats_job_state__3__86__20201103T1912Z\",\n" +
                            "            \"eats_job_state__0__87__20201104T0408Z\"\n" +
                            "          ]\n" +
                            "        },\n" +
                            "        {\n" +
                            "          \"Server_streampinot-sandbox01-phx3_7090\": [\n" +
                            "            \"eats_job_state__3__89__20201104T0958Z\",\n" +
                            "            \"eats_job_state__0__89__20201104T1409Z\",\n" +
                            "            \"eats_job_state__2__88__20201104T0930Z\",\n" +
                            "            \"eats_job_state__1__90__20201104T0952Z\",\n" +
                            "            \"eats_job_state__3__87__20201103T2358Z\",\n" +
                            "            \"eats_job_state__0__85__20201103T1842Z\",\n" +
                            "            \"eats_job_state__3__86__20201103T1912Z\",\n" +
                            "            \"eats_job_state__1__89__20201104T0452Z\"\n" +
                            "          ],\n" +
                            "          \"Server_streampinot-sandbox06-phx3_7090\": [\n" +
                            "            \"eats_job_state__2__85__20201103T1853Z\",\n" +
                            "            \"eats_job_state__1__92__20201104T1938Z\",\n" +
                            "            \"eats_job_state__2__89__20201104T1431Z\",\n" +
                            "            \"eats_job_state__1__88__20201103T2351Z\",\n" +
                            "            \"eats_job_state__3__88__20201104T0458Z\",\n" +
                            "            \"eats_job_state__0__86__20201103T2325Z\",\n" +
                            "            \"eats_job_state__2__87__20201104T0430Z\",\n" +
                            "            \"eats_job_state__1__87__20201103T1906Z\"\n" +
                            "          ],\n" +
                            "          \"Server_streampinot-sandbox03-phx3_7090\": [\n" +
                            "            \"eats_job_state__2__86__20201103T2337Z\",\n" +
                            "            \"eats_job_state__1__91__20201104T1452Z\",\n" +
                            "            \"eats_job_state__3__90__20201104T1458Z\",\n" +
                            "            \"eats_job_state__2__90__20201104T1927Z\",\n" +
                            "            \"eats_job_state__0__88__20201104T0908Z\",\n" +
                            "            \"eats_job_state__3__91__20201104T1941Z\",\n" +
                            "            \"eats_job_state__0__90__20201104T1909Z\",\n" +
                            "            \"eats_job_state__0__87__20201104T0408Z\"\n" +
                            "          ]\n" +
                            "        }\n" +
                            "      ]\n" +
                            "    }\n" +
                            "  ]\n" +
                            "}"));

        HttpClient httpClientV4 = new TestingHttpClient((request) ->
                request.getUri().getPath().contains("instance")
                        ? TestingResponse.mockResponse(HttpStatus.OK, MediaType.JSON_UTF_8, BROKER_RESPONSE)
                        : TestingResponse.mockResponse(HttpStatus.OK, MediaType.JSON_UTF_8, "{\n" +
                            "  \"eats_job_state_REALTIME\": {\n" +
                            "    \"Server_streampinot-sandbox01-phx3_7090\": [\n" +
                            "      \"eats_job_state__2__87__20201104T0430Z\",\n" +
                            "      \"eats_job_state__3__86__20201103T1912Z\",\n" +
                            "      \"eats_job_state__0__88__20201104T0908Z\",\n" +
                            "      \"eats_job_state__1__88__20201103T2351Z\",\n" +
                            "      \"eats_job_state__3__89__20201104T0958Z\",\n" +
                            "      \"eats_job_state__0__85__20201103T1842Z\",\n" +
                            "      \"eats_job_state__1__91__20201104T1452Z\",\n" +
                            "      \"eats_job_state__1__92__20201104T1938Z\"\n" +
                            "    ],\n" +
                            "    \"Server_streampinot-sandbox03-phx3_7090\": [\n" +
                            "      \"eats_job_state__2__85__20201103T1853Z\",\n" +
                            "      \"eats_job_state__1__89__20201104T0452Z\",\n" +
                            "      \"eats_job_state__2__90__20201104T1927Z\",\n" +
                            "      \"eats_job_state__2__88__20201104T0930Z\",\n" +
                            "      \"eats_job_state__0__89__20201104T1409Z\",\n" +
                            "      \"eats_job_state__0__86__20201103T2325Z\",\n" +
                            "      \"eats_job_state__3__87__20201103T2358Z\",\n" +
                            "      \"eats_job_state__3__90__20201104T1458Z\"\n" +
                            "    ],\n" +
                            "    \"Server_streampinot-sandbox06-phx3_7090\": [\n" +
                            "      \"eats_job_state__1__87__20201103T1906Z\",\n" +
                            "      \"eats_job_state__0__90__20201104T1909Z\",\n" +
                            "      \"eats_job_state__3__88__20201104T0458Z\",\n" +
                            "      \"eats_job_state__1__90__20201104T0952Z\",\n" +
                            "      \"eats_job_state__2__89__20201104T1431Z\",\n" +
                            "      \"eats_job_state__2__86__20201103T2337Z\",\n" +
                            "      \"eats_job_state__0__87__20201104T0408Z\",\n" +
                            "      \"eats_job_state__3__91__20201104T1941Z\"\n" +
                            "    ]\n" +
                            "  }\n" +
                            "}\n"));
        PinotConfig pinotConfig = new PinotConfig()
                .setMetadataCacheExpiry(new Duration(1, TimeUnit.MILLISECONDS))
                .setControllerUrls("localhost:7900");
        PinotClusterInfoFetcher pinotClusterInfoFetcherv1 = new PinotClusterInfoFetcher(pinotConfig, new PinotMetrics(), httpClientV1, MetadataUtil.TABLES_JSON_CODEC, MetadataUtil.BROKERS_FOR_TABLE_JSON_CODEC, MetadataUtil.ROUTING_TABLES_JSON_CODEC, MetadataUtil.ROUTING_TABLES_JSON_CODEC_V4, MetadataUtil.TIME_BOUNDARY_JSON_CODEC);
        PinotClusterInfoFetcher pinotClusterInfoFetcherv4 = new PinotClusterInfoFetcher(pinotConfig, new PinotMetrics(), httpClientV4, MetadataUtil.TABLES_JSON_CODEC, MetadataUtil.BROKERS_FOR_TABLE_JSON_CODEC, MetadataUtil.ROUTING_TABLES_JSON_CODEC, MetadataUtil.ROUTING_TABLES_JSON_CODEC_V4, MetadataUtil.TIME_BOUNDARY_JSON_CODEC);
        Map<String, Map<String, List<String>>> routingTableV1 = pinotClusterInfoFetcherv1.getRoutingTableForTable(new PinotTableHandle("connId", "eats_job_state", "eats_job_state"));
        Map<String, Map<String, List<String>>> routingTableV4 = pinotClusterInfoFetcherv4.getRoutingTableForTable(new PinotTableHandle("connId", "eats_job_state", "eats_job_state"));
        routingTableV1.forEach(
                (tableName, serverMap) -> {
                    assertTrue(routingTableV4.containsKey(tableName));
                    serverMap.forEach((host, segments) -> {
                        routingTableV4.get(tableName).containsKey(host);
                    });
                });
    }

    @Test
    public void testTimeBoundary()
    {
        HttpClient httpClient = new TestingHttpClient((request) -> request.getUri().getPath().contains("instance")
                ? TestingResponse.mockResponse(HttpStatus.OK, MediaType.JSON_UTF_8, BROKER_RESPONSE)
                : TestingResponse.mockResponse(HttpStatus.NOT_FOUND, MediaType.ANY_TYPE, ""));
        PinotConfig pinotConfig = new PinotConfig()
                .setMetadataCacheExpiry(new Duration(1, TimeUnit.MILLISECONDS))
                .setControllerUrls("localhost:7900");
        PinotClusterInfoFetcher pinotClusterInfoFetcher = new PinotClusterInfoFetcher(pinotConfig, new PinotMetrics(), httpClient, MetadataUtil.TABLES_JSON_CODEC, MetadataUtil.BROKERS_FOR_TABLE_JSON_CODEC, MetadataUtil.ROUTING_TABLES_JSON_CODEC, MetadataUtil.ROUTING_TABLES_JSON_CODEC_V4, MetadataUtil.TIME_BOUNDARY_JSON_CODEC);
        PinotClusterInfoFetcher.TimeBoundary timeBoundary = pinotClusterInfoFetcher.getTimeBoundaryForTable(new PinotTableHandle("connId", "eats_job_state", "eats_job_state"));
        assertFalse(timeBoundary.getOfflineTimePredicate().isPresent());
        assertFalse(timeBoundary.getOnlineTimePredicate().isPresent());
    }
}
