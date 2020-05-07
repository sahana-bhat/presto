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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PinotMuttleyConfig
{
    private final String muttleyRoService;
    private final String muttleyRwService;
    private final Map<String, String> extraHeaders;

    @JsonCreator
    public PinotMuttleyConfig(@JsonProperty("muttleyRoService") String muttleyRoService,
                               @JsonProperty("muttleyRwService") String muttleyRwService,
                               @JsonProperty("extraHeaders") Map<String, String> extraHeaders)
    {
        this.muttleyRoService = requireNonNull(muttleyRoService, "muttleyRoService is null");
        this.muttleyRwService = requireNonNull(muttleyRwService, "muttleyRwService is null");
        this.extraHeaders = requireNonNull(extraHeaders, "extraHeaders is null");
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

        PinotMuttleyConfig that = (PinotMuttleyConfig) o;
        return Objects.equals(muttleyRoService, that.muttleyRoService)
                && Objects.equals(muttleyRwService, that.muttleyRwService)
                && Objects.equals(extraHeaders, that.extraHeaders);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(muttleyRoService, muttleyRwService, extraHeaders);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("muttleyRoService", muttleyRoService)
                .add("muttleyRwService", muttleyRwService)
                .add("extraHeaders", extraHeaders)
                .toString();
    }
}
