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
package com.facebook.presto.aresdb.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class AQLExpression
{
    private String name;
    private Optional<TimeSpec> timeTokenizer;
    private boolean hidden;

    @JsonCreator
    public AQLExpression(@JsonProperty("name") String name, @JsonProperty("timeTokenizer") Optional<TimeSpec>
            timeTokenizer, @JsonProperty("hidden") boolean hidden)
    {
        this.name = name;
        this.timeTokenizer = timeTokenizer;
        this.hidden = hidden;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Optional<TimeSpec> getTimeTokenizer()
    {
        return timeTokenizer;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("timeTokenizer", timeTokenizer)
                .add("hidden", hidden)
                .toString();
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
        AQLExpression that = (AQLExpression) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(timeTokenizer, that.timeTokenizer) &&
                Objects.equals(hidden, that.hidden);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, timeTokenizer, hidden);
    }
}
