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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class HttpRequestDetails
{
    private final String url;
    private final Map<String, String> headers;

    @JsonCreator
    public HttpRequestDetails(@JsonProperty("url") String url,
                          @JsonProperty("headers") Map<String, String> headers)
    {
        this.url = url;
        this.headers = headers;
    }

    @JsonProperty
    public String getUrl()
    {
        return url;
    }

    @JsonProperty
    public Map<String, String> getHeaders()
    {
        return headers;
    }
}
