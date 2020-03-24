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

import java.util.Optional;

import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Origin;
import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Origin.DERIVED;
import static java.util.Objects.requireNonNull;

public class AresDbExpression
{
    private final String definition;
    private final Optional<TimeSpec> timeBucketizer;
    private final Origin origin;

    public AresDbExpression(String definition, Optional<TimeSpec> timeBucketizer, Origin origin)
    {
        this.definition = requireNonNull(definition, "definition is null");
        this.timeBucketizer = requireNonNull(timeBucketizer, "timeBucketizer is null");
        this.origin = requireNonNull(origin, "origin is null");
    }

    public String getDefinition()
    {
        return definition;
    }

    public Optional<TimeSpec> getTimeBucketizer()
    {
        return timeBucketizer;
    }

    public Origin getOrigin()
    {
        return origin;
    }

    public static AresDbExpression derived(String definition)
    {
        return new AresDbExpression(definition, Optional.empty(), DERIVED);
    }

    public static AresDbExpression derived(String definition, TimeSpec timeBucketizer)
    {
        return new AresDbExpression(definition, Optional.of(timeBucketizer), DERIVED);
    }
}
