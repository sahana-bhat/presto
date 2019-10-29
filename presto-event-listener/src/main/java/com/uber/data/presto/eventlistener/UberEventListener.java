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
package com.uber.data.presto.eventlistener;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import hpw_shaded.com.uber.data.heatpipe.HeatpipeFactory;
import hpw_shaded.com.uber.data.heatpipe.configuration.Heatpipe4JConfig;
import hpw_shaded.com.uber.data.heatpipe.configuration.PropertiesHeatpipeConfiguration;
import hpw_shaded.com.uber.data.heatpipe.errors.HeatpipeEncodeError;
import hpw_shaded.com.uber.data.heatpipe.errors.SchemaServiceNotAvailableException;
import hpw_shaded.com.uber.data.heatpipe.producer.HeatpipeProducer;
import hpw_shaded.com.uber.data.heatpipe.producer.HeatpipeProducerFactory;
import hpw_shaded.com.uber.m3.client.MetricConfig;
import hpw_shaded.com.uber.m3.client.Scope;
import hpw_shaded.com.uber.m3.client.Scopes;
import hpw_shaded.com.uber.stream.java.kafka.rest.client.KafkaRestClientException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class UberEventListener
        implements EventListener
{
    private static final Logger log = Logger.get(UberEventListener.class);
    private final String engine;
    private final String cluster;
    private final HeatpipeProducer producer;
    private final boolean logOnlyCompleteEvent;
    private final Optional<Scope> m3Client;

    public UberEventListener(String topic, String engine, String cluster, boolean syncProduce, boolean logOnlyCompleteEvent, boolean logToM3)
    {
        this.engine = engine;
        this.cluster = cluster;

        Properties prop = new Properties();
        prop.setProperty("heatpipe.app_id", "PrestoEventListener");
        prop.setProperty("kafka.syncProduction", Boolean.toString(syncProduce));
        Heatpipe4JConfig heatpipe4JConfig = new PropertiesHeatpipeConfiguration(prop);

        this.producer = createHeatpipeProducer(topic, heatpipe4JConfig);
        this.logOnlyCompleteEvent = logOnlyCompleteEvent;
        this.m3Client = Optional.ofNullable(logToM3 ? createM3Scope() : null);
    }

    private Scope createM3Scope()
    {
        // Leave this as a hashmap to avoid the immutablemap's strict null checking
        Map<String, String> commonTags = new HashMap<>();
        // production or staging
        commonTags.put("env", System.getenv("UBER_RUNTIME_ENVIRONMENT"));
        commonTags.put("dc", System.getenv("UBER_DATACENTER"));
        commonTags.put("service", System.getenv("UDEPLOY_SERVICE_NAME"));

        return Scopes.getScopeForConfig(
                MetricConfig.builder()
                        .setGenerateTagsFromEnv(false)
                        .setCommonTags(commonTags)
                        .build());
    }

    private HeatpipeProducer createHeatpipeProducer(String topic, Heatpipe4JConfig config)
    {
        try {
            HeatpipeFactory heatpipeFactory = new HeatpipeFactory(config);
            HeatpipeProducerFactory heatpipeProducerFactory = new HeatpipeProducerFactory(config);
            Integer version = Collections.max(heatpipeFactory.getClient().getSchemaVersions(topic));
            return heatpipeProducerFactory.get(topic, version);
        }
        catch (IOException | SchemaServiceNotAvailableException | KafkaRestClientException ex) {
            log.error("Failed to create kafka producer", ex);
            return null;
        }
    }

    void sendToHeatpipe(QueryEventInfo queryEventInfo)
    {
        if (producer == null) {
            return;
        }

        try {
            producer.produce(queryEventInfo.toMap());
        }
        catch (HeatpipeEncodeError heatpipeEncodeError) {
            log.error("failed to send data:", heatpipeEncodeError);
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (logOnlyCompleteEvent) {
            return;
        }
        QueryEventInfo queryEventInfo = new QueryEventInfo(queryCreatedEvent, this.engine, this.cluster);
        sendToHeatpipe(queryEventInfo);
        log.info("Query created. query id: " + queryEventInfo.getQueryId()
                + ", state: " + queryEventInfo.getState());
    }

    private void sendToM3(QueryEventInfo queryEventInfo)
    {
        if (!m3Client.isPresent()) {
            return;
        }
        try {
            queryEventInfo.sendToM3(m3Client.get());
        }
        catch (RuntimeException e) { // Just for good measure
            log.error("failed to log to m3", e);
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        QueryEventInfo queryEventInfo = new QueryEventInfo(queryCompletedEvent, this.engine, this.cluster);
        sendToHeatpipe(queryEventInfo);
        sendToM3(queryEventInfo);
        log.info("Query completed. query id: " + queryEventInfo.getQueryId()
                + ", state: " + queryEventInfo.getState());
    }
}
