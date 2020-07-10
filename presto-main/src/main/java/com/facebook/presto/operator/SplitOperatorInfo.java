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
package com.facebook.presto.operator;

import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SplitOperatorInfo
        implements Mergeable<SplitOperatorInfo>, OperatorInfo
{
    // NOTE: this deserializes to a map instead of the expected type
    private final Object splitInfo;

    @JsonCreator
    public SplitOperatorInfo(
            @JsonProperty("splitInfo") Object splitInfo)
    {
        this.splitInfo = splitInfo;
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }

    @JsonProperty
    public Object getSplitInfo()
    {
        return splitInfo;
    }

    private String mergePartitionName(String partitionName1, String partitionName2)
    {
        if (partitionName1 == null) {
            return partitionName2;
        }

        if (partitionName2 == null) {
            return partitionName1;
        }

        if (partitionName1 == partitionName2) {
            return partitionName1;  // should take care of the <UNPARTITIONED> case
        }

        Map<String, Set<String>> partitionMap = new HashMap<String, Set<String>>();
        extractPartitions(partitionName1, partitionMap);
        extractPartitions(partitionName2, partitionMap);

        List<String> partitions = partitionMap.keySet()
                .stream()
                .map(k -> k + "=" + String.join(",", partitionMap.get(k)))
                .collect(Collectors.toList());
        return String.join("/", partitions);
    }

    // Assumes that the strings are of the format "name1=val1,val2,val3;name2=val4,val5"
    private void extractPartitions(String partitionName, Map<String, Set<String>> partitionMap)
    {
        for (String name : partitionName.split("/")) {
            int equalIndex = name.indexOf('=');
            String pName = name.substring(0, equalIndex);
            if (!partitionMap.containsKey(pName)) {
                partitionMap.put(pName, new HashSet<String>());
            }
            for (String pVal : name.substring(equalIndex + 1).split(",")) {
                partitionMap.get(pName).add(pVal);
            }
        }
    }

    @Override
    public SplitOperatorInfo mergeWith(SplitOperatorInfo other)
    {
        if (splitInfo instanceof Map && other.splitInfo instanceof Map) {
            Map splitInfoMap = (Map) splitInfo;
            Map otherSplitInfoMap = (Map) other.splitInfo;

            if (checkEqual(splitInfoMap, otherSplitInfoMap, "database") && checkEqual(splitInfoMap, otherSplitInfoMap, "table")) {
                String partitionName = null;
                try {
                    partitionName = mergePartitionName(
                            (String) splitInfoMap.get("partitionName"),
                            (String) otherSplitInfoMap.get("partitionName"));
                }
                finally {
                    if (partitionName == null) {
                        return new SplitOperatorInfo(
                                ImmutableMap.of(
                                        "database", splitInfoMap.get("database"),
                                        "table", splitInfoMap.get("table")));
                    }
                    else {
                        return new SplitOperatorInfo(
                                ImmutableMap.of(
                                        "database", splitInfoMap.get("database"),
                                        "table", splitInfoMap.get("table"),
                                        "partitionName", partitionName));
                    }
                }
            }
        }
        return null;
    }

    private boolean checkEqual(Map map1, Map map2, String key)
    {
        return map1.containsKey(key) && map2.containsKey(key) && map1.get(key).equals(map2.get(key));
    }
}
