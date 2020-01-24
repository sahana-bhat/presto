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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RTACluster
{
    @JsonProperty
    private final String storageType;

    @JsonProperty
    private String name;

    @JsonProperty
    private String region;

    @JsonProperty
    private String muttleyRoService;

    @JsonProperty
    private String muttleyRwService;

    @JsonCreator
    public RTACluster(@JsonProperty("storageType") String storageType,
                      @JsonProperty("name") String name,
                      @JsonProperty("region") String region,
                      @JsonProperty("muttleyRoService") String muttleyRoService,
                      @JsonProperty("muttleyRwService") String muttleyRwService)
    {
        this.storageType = storageType;
        this.name = name;
        this.region = region;
        this.muttleyRoService = muttleyRoService;
        this.muttleyRwService = muttleyRwService;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getRegion()
    {
        return region;
    }

    @JsonProperty
    public String getMuttleyRoService()
    {
        return muttleyRoService;
    }

    @JsonProperty
    public String getMuttleyRwService()
    {
        return muttleyRwService;
    }

    @JsonProperty
    public String getStorageType()
    {
        return storageType;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RTACluster that = (RTACluster) o;
        return Objects.equals(name, that.name)
                && Objects.equals(region, that.region)
                && Objects.equals(storageType, that.storageType)
                && Objects.equals(muttleyRoService, that.muttleyRoService)
                && Objects.equals(muttleyRwService, that.muttleyRwService);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, region, storageType, muttleyRoService, muttleyRwService);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("storageType", storageType)
                .add("name", name)
                .add("region", region)
                .add("muttleyRoService", muttleyRoService)
                .add("muttleyRwService", muttleyRwService)
                .toString();
    }
}
