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
package com.facebook.presto.hive;

import com.facebook.airlift.log.Logger;

import javax.inject.Inject;

public class HiveTicketCache
{
    private static Logger log = Logger.get(HiveTicketCache.class);

    private static HdfsEnvironment hdfsEnvironment;

    @Inject
    public HiveTicketCache(HdfsEnvironment hdfsEnvironment)
    {
        this.hdfsEnvironment = hdfsEnvironment;
    }

    public HiveTicketCache()
    {
    }

    public HdfsEnvironment getHdfsEnvironment()
    {
        return this.hdfsEnvironment;
    }
}
