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

package com.facebook.presto.server;

import com.facebook.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.annotation.Nonnull;

import java.util.Optional;

public class StatementResourceConfig
{
    private Optional<Duration> defaultWaitForEntireResponseIntervalMs = Optional.empty();

    @Nonnull
    public Optional<Duration> getDefaultWaitForEntireResponseIntervalMs()
    {
        return defaultWaitForEntireResponseIntervalMs;
    }

    @Config("statement.default-wait-for-entire-response-interval")
    @MinDuration("0s")
    public StatementResourceConfig setDefaultWaitForEntireResponseIntervalMs(Duration defaultWaitForEntireResponseIntervalMs)
    {
        this.defaultWaitForEntireResponseIntervalMs = Optional.ofNullable(defaultWaitForEntireResponseIntervalMs);
        return this;
    }
}
