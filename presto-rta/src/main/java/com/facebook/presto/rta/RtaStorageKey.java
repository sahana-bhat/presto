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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class RtaStorageKey
{
    private final RtaStorageType type;

    @JsonCreator
    public RtaStorageKey(@JsonProperty("type") RtaStorageType type)
    {
        this.type = type;
    }

    public static RtaStorageKey fromDeployment(RTADeployment deployment)
    {
        return new RtaStorageKey(deployment.getStorageType());
    }

    @JsonProperty
    public RtaStorageType getType()
    {
        return type;
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
        RtaStorageKey that = (RtaStorageKey) o;
        return type == that.type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type);
    }

    @Override
    public String toString()
    {
        return String.format("%s", type);
    }
}
