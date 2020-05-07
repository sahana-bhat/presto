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

package com.facebook.presto.aresdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AresDbMuttleyConfig
{
    private final String muttleyRoService;
    private final Map<String, String> extraHeaders;

    @JsonCreator
    public AresDbMuttleyConfig(@JsonProperty("muttleyRoService") String muttleyRoService,
                               @JsonProperty("extraHeaders") Map<String, String> extraHeaders)
    {
        this.muttleyRoService = requireNonNull(muttleyRoService, "muttleyRoService is null");
        this.extraHeaders = requireNonNull(extraHeaders, "extraHeaders is null");
    }

    @JsonProperty
    public String getMuttleyRoService()
    {
        return muttleyRoService;
    }

    @JsonProperty
    public Map<String, String> getExtraHeaders()
    {
        return extraHeaders;
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

        AresDbMuttleyConfig that = (AresDbMuttleyConfig) o;
        return Objects.equals(muttleyRoService, that.muttleyRoService) &&
                Objects.equals(extraHeaders, that.extraHeaders);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(muttleyRoService, extraHeaders);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("MuttleyRoService", muttleyRoService)
                .add("extraHeaders", extraHeaders)
                .toString();
    }
}
