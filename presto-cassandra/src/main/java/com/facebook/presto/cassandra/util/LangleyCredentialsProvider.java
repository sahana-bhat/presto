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
package com.facebook.presto.cassandra.util;

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.facebook.presto.cassandra.CassandraErrorCode.CASSANDRA_CREDENTIAL_PROVIDER_ERROR;
import static java.lang.String.format;

public class LangleyCredentialsProvider
{
    private final JsonNode credentials;

    public LangleyCredentialsProvider(String filePath)
    {
        byte[] jsonData;

        try {
            jsonData = Files.readAllBytes(Paths.get(filePath));
        }
        catch (IOException e) {
            throw new PrestoException(CASSANDRA_CREDENTIAL_PROVIDER_ERROR, format("Error reading credentials file %s", filePath), e);
        }

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            credentials = objectMapper.readTree(jsonData);
        }
        catch (IOException e) {
            throw new PrestoException(CASSANDRA_CREDENTIAL_PROVIDER_ERROR, format("Error parsing credentials json %s", new String(jsonData)), e);
        }
    }

    public String getUsername(String catalogName)
    {
        return credentials.get("cassandra").get(catalogName).get("username").asText("username");
    }

    public String getPassword(String catalogName)
    {
        return credentials.get("cassandra").get(catalogName).get("password").asText("password");
    }
}
