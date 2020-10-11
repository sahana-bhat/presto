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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.StringResponseHandler;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.google.common.net.HostAndPort;
import org.apache.thrift.TException;

import javax.inject.Inject;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * HiveCluster implementation to handle fetching of metastore URIs dynamically
 */
public class DynamicHiveCluster
        implements HiveCluster
{
    private final String metastoreDiscoveryRpcServiceName;
    private final HiveMetastoreClientFactory clientFactory;
    private final String metastoreUsername;
    private final String metastoreDiscoveryUri;
    private final String rpcServiceHeaderStr = "RPC-Service";
    private final String rpcCallerHeaderStr = "RPC-Caller";
    private final String rpcCaller = "presto";
    private final HttpClient httpClient = new JettyHttpClient();
    private List<HostAndPort> addresses;

    @Inject
    public DynamicHiveCluster(DynamicMetastoreConfig config, HiveMetastoreClientFactory clientFactory)
    {
        this(config.getMetastoreUsername(), config.getMetastoreDiscoveryRpcServiceName(), config.getMetastoreDiscoveryUri(), clientFactory);
    }
    public DynamicHiveCluster(String metastoreUsername, String metastoreDiscoveryRpcServiceName, String metastoreDiscoveryUri, HiveMetastoreClientFactory clientFactory)
    {
        this.metastoreDiscoveryRpcServiceName = requireNonNull(metastoreDiscoveryRpcServiceName, "metastore discovery rpc name is null");
        this.metastoreDiscoveryUri = requireNonNull(metastoreDiscoveryUri, "metastore discovery uri is null");
        this.metastoreUsername = metastoreUsername;
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
    }

    /**
     * Placeholder implementation to return addresses to ThriftHiveMetastore
     */
    @Override
    public List<HostAndPort> getAddresses()
    {
        return Collections.emptyList();
    }

    @Override
    public HiveMetastoreClient createMetastoreClient(String token, HostAndPort metastore)
            throws TException
    {
        if (metastore == null) {
            // TODO: check if retries are required here. ThriftHiveMetastore calls are retried
            // so adding retries here could be redundant
            URI uri = getMetastoreUri();
            metastore = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        }
        TException lastException = null;
        try {
            HiveMetastoreClient client = clientFactory.create(metastore, token);

            if (!isNullOrEmpty(metastoreUsername)) {
                client.setUGI(metastoreUsername);
            }
            return client;
        }
        catch (TException e) {
            lastException = e;
        }
        throw new TException("Failed connecting to Hive metastore: " + metastore.getHost(), lastException);
    }

    private URI getMetastoreUri() throws TException
    {
        URI uri;
        // TODO: add caching to this call
        // TODO: meter the muttley calls
        Request request = prepareGet()
                .setUri(uriBuilderFrom(URI.create(metastoreDiscoveryUri)).build())
                .setHeader(rpcServiceHeaderStr, metastoreDiscoveryRpcServiceName)
                .setHeader(rpcCallerHeaderStr, rpcCaller)
                .build();
        StringResponseHandler.StringResponse response = httpClient.execute(request, createStringResponseHandler());
        if (response.getStatusCode() == OK.code()) {
            uri = URI.create(response.getBody());
        }
        else {
            throw new TException("Error in fetching metastore URI");
        }
        return uri;
    }
}
