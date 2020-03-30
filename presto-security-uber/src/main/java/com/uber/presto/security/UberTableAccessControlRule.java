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
package com.uber.presto.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class UberTableAccessControlRule
{
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> catalogRegex;
    private final Optional<Pattern> schemaRegex;
    private final Optional<Pattern> tableRegex;
    private final boolean allow;

    @JsonCreator
    public UberTableAccessControlRule(
            @JsonProperty("user") Optional<Pattern> user,
            @JsonProperty("catalog") Optional<Pattern> catalog,
            @JsonProperty("schema") Optional<Pattern> schema,
            @JsonProperty("table") Optional<Pattern> table,
            @JsonProperty("allow") boolean allow)
    {
        this.userRegex = requireNonNull(user, "userRegex is null");
        this.catalogRegex = requireNonNull(catalog, "catalogRegex is null");
        this.schemaRegex = requireNonNull(schema, "schemaRegex is null");
        this.tableRegex = requireNonNull(table, "tableRegex is null");
        this.allow = allow;
    }

    public Optional<Boolean> match(String user, String catalog, String schema, String table)
    {
        if (userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                catalogRegex.map(regex -> regex.matcher(catalog).matches()).orElse(true) &&
                schemaRegex.map(regex -> regex.matcher(schema).matches()).orElse(true) &&
                tableRegex.map(regex -> regex.matcher(table).matches()).orElse(true)) {
            return Optional.of(allow);
        }
        return Optional.empty();
    }
}
