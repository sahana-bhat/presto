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

package com.facebook.presto.rta.schema;

import com.facebook.presto.rta.RtaStorageType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

import static com.facebook.presto.rta.RtaUtil.checked;
import static com.facebook.presto.rta.RtaUtil.checkedOr;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Locale.ENGLISH;

/**
 * This class communicates with the rta-ums (muttley)/rtaums (udeploy) service. It's api is available here:
 * https://rtaums.uberinternal.com/tables/{namespace}/{tablename}/deployments
 */
public class RTADeployment
{
    private static final ObjectMapper mapper = new ObjectMapper();
    private final RtaStorageType storageTypeEnum;
    private String namespace;
    private String name;
    private String cluster;
    private String dataCenter;
    private final RTACluster rtaCluster;

    @JsonCreator
    public RTADeployment(@JsonProperty("storageType") String storageType,
                         @JsonProperty("namespace") String namespace,
                         @JsonProperty("name") String name,
                         @JsonProperty("cluster") String cluster,
                         @JsonProperty("datacenter") String dataCenter,
                         @JsonProperty(value = "rtaCluster", required = false) @Nullable RTACluster rc)
    {
        this.storageTypeEnum = RtaStorageType.valueOf(storageType.toUpperCase(ENGLISH));
        this.namespace = checked(namespace, "namespace");
        this.name = checked(name, "name");
        this.cluster = checkedOr(cluster, "").toLowerCase(ENGLISH);
        this.dataCenter = checked(dataCenter, "dataCenter").toLowerCase(ENGLISH);
        this.rtaCluster = rc;
    }

    public RtaStorageType getStorageType()
    {
        return storageTypeEnum;
    }

    @JsonProperty
    public String getNamespace()
    {
        return namespace;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getCluster()
    {
        return cluster;
    }

    @JsonProperty
    public String getDataCenter()
    {
        return dataCenter;
    }

    public String getDescriptor()
    {
        return dataCenter + (isNullOrEmpty(cluster) ? "" : "-" + cluster);
    }

    @JsonProperty
    public RTACluster getRtaCluster()
    {
        return rtaCluster;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("storageTypeEnum", storageTypeEnum)
                .add("namespace", namespace)
                .add("name", name)
                .add("cluster", cluster)
                .add("dataCenter", dataCenter)
                .add("rtaCluster", rtaCluster)
                .toString();
    }
}
