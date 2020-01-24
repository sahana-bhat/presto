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

import com.facebook.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class RtaConfig
{
    private String rtaUmsService;
    private Optional<String> configFile = Optional.empty();
    private Duration cacheExpiryTime = new Duration(1, TimeUnit.HOURS);
    private Optional<String> dataCenterOverride = Optional.empty();
    private String extraDefinitionFiles = "";
    private String extraDeploymentFiles = "";

    @NotNull
    public String getRtaUmsService()
    {
        return rtaUmsService;
    }

    @Config("rtaums_service")
    public RtaConfig setRtaUmsService(String rtaUmsService)
    {
        this.rtaUmsService = rtaUmsService;
        return this;
    }

    @NotNull
    public String getExtraDefinitionFiles()
    {
        return extraDefinitionFiles;
    }

    @Config("extra_definition_files")
    public RtaConfig setExtraDefinitionFiles(String extraDefinitionFiles)
    {
        this.extraDefinitionFiles = extraDefinitionFiles;
        return this;
    }

    @NotNull
    public String getExtraDeploymentFiles()
    {
        return extraDeploymentFiles;
    }

    @Config("extra_deployment_files")
    public RtaConfig setExtraDeploymentFiles(String extraDeploymentFiles)
    {
        this.extraDeploymentFiles = extraDeploymentFiles;
        return this;
    }

    @NotNull
    public Optional<String> getConfigFile()
    {
        return configFile;
    }

    @Config("config_file")
    public RtaConfig setConfigFile(String configFile)
    {
        this.configFile = Optional.of(configFile);
        return this;
    }

    @NotNull
    public Duration getMetadataCacheExpiryTime()
    {
        return cacheExpiryTime;
    }

    @Config("cache_expiry_time")
    public RtaConfig setMetadataCacheExpiryTime(Duration cacheExpiryTime)
    {
        this.cacheExpiryTime = cacheExpiryTime;
        return this;
    }

    @Nullable
    public Optional<String> getDataCenterOverride()
    {
        return dataCenterOverride;
    }

    @Config("datacenter_override")
    public RtaConfig setDataCenterOverride(String dataCenterOverride)
    {
        this.dataCenterOverride = Optional.of(dataCenterOverride);
        return this;
    }
}
