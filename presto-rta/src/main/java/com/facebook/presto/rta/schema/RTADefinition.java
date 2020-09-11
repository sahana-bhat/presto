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

package com.facebook.presto.rta.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static com.facebook.presto.rta.RtaUtil.checkType;
import static com.facebook.presto.rta.RtaUtil.checked;
import static com.facebook.presto.rta.RtaUtil.checkedOr;
import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * This class communicates with the rta-ums (muttley)/rtaums (udeploy) service. It's api is available here:
 * https://rtaums.uberinternal.com/tables/definitions/{namespace}/{tablename}
 */
public class RTADefinition
{
    @JsonProperty
    private String namespace;

    @JsonProperty
    private String name;

    @JsonProperty
    private List<Field> fields;

    @JsonProperty("rtaTableMetadata")
    private RTAMetadata metadata;

    public List<Field> getFields()
    {
        return fields;
    }

    public RTAMetadata getMetadata()
    {
        return metadata;
    }

    public String getNamespace()
    {
        return namespace;
    }

    public String getName()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fields", fields)
                .add("metadata", metadata)
                .toString();
    }

    @JsonDeserialize(using = FieldDeserializer.class)
    public static class Field<T>
    {
        @JsonProperty
        private T type;

        @JsonProperty
        private String name;

        @JsonProperty
        private String uberLogicalType;

        @JsonProperty
        private String columnType;

        @JsonProperty
        private String cardinality;

        @JsonProperty
        private boolean multiValueField;

        public Field(@JsonProperty("type") T type, @JsonProperty("name") String name, @JsonProperty("uberLogicalType") String uberLogicalType, @JsonProperty("columnType") String columnType, @JsonProperty("cardinality") String cardinality, @JsonProperty("multiValueField") boolean multiValueField)
        {
            if (multiValueField) {
                checkType(type, DataType.class, "dataType");
            }
            else {
                checked((String) type, "type");
            }

            this.type = type;
            this.name = checked(name, "name");
            this.uberLogicalType = checkedOr(uberLogicalType, "");
            this.columnType = checkedOr(columnType, "");
            this.cardinality = checkedOr(cardinality, "");
            this.multiValueField = multiValueField;
        }

        public T getType()
        {
            return type;
        }

        public String getName()
        {
            return name;
        }

        public String getUberLogicalType()
        {
            return uberLogicalType;
        }

        public String getColumnType()
        {
            return columnType;
        }

        public String getCardinality()
        {
            return cardinality;
        }

        public boolean isMultiValueField()
        {
            return multiValueField;
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
            Field field = (Field) o;
            return Objects.equals(type, field.type)
                    && Objects.equals(name, field.name)
                    && Objects.equals(uberLogicalType, field.uberLogicalType)
                    && Objects.equals(columnType, field.columnType)
                    && Objects.equals(cardinality, field.cardinality);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, name, uberLogicalType, columnType, cardinality);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("type", type)
                    .add("name", name)
                    .add("uberLogicalType", uberLogicalType)
                    .add("columnType", columnType)
                    .add("cardinality", cardinality)
                    .toString();
        }
    }

    public static class DataType
    {
        @JsonProperty
        private String type;

        @JsonProperty
        private String items;

        @JsonCreator
        public DataType(
                @JsonProperty("type") String type, @JsonProperty("items") String items)
        {
            this.type = checked(type, "type");
            this.items = checked(items, "items");
        }

        public String getItems()
        {
            return items;
        }

        public String getType()
        {
            return type;
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
            DataType that = (DataType) o;
            return Objects.equals(type, that.type) && Objects.equals(items, that.items);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, items);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("type", type)
                    .add("items", items)
                    .toString();
        }
    }

    public static class RTAMetadata
    {
        @JsonProperty
        private boolean isFactTable;

        @JsonProperty
        private List<String> primaryKeys;

        @JsonProperty
        private List<String> queryTypes;

        @JsonProperty
        private String retentionDays;

        public String getRetentionDays()
        {
            return retentionDays;
        }

        public boolean isFactTable()
        {
            return isFactTable;
        }

        public List<String> getPrimaryKeys()
        {
            return primaryKeys;
        }

        public List<String> getQueryTypes()
        {
            return queryTypes;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("isFactTable", isFactTable)
                    .add("primaryKeys", primaryKeys)
                    .add("queryTypes", queryTypes)
                    .add("retentionDays", retentionDays)
                    .toString();
        }
    }

    public static class FieldDeserializer
            extends StdDeserializer<Field<?>>
    {
        protected FieldDeserializer()
        {
            this(null);
        }

        protected FieldDeserializer(Class<?> vc)
        {
            super(vc);
        }

        @Override
        public Field<?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException
        {
            JsonNode root = p.readValueAsTree();
            boolean multiValuedField = root.get("multiValueField") != null && root.get("multiValueField").booleanValue();
            Field<?> fieldValue;

            if (multiValuedField) {
                fieldValue = new Field<>(
                        new DataType(root.get("type").get("type").asText(), root.get("type").get("items").asText()),
                        root.get("name").asText(),
                        root.get("uberLogicalType").asText(),
                        root.get("columnType").asText(),
                        root.get("cardinality").asText(),
                        multiValuedField);
            }
            else {
                fieldValue = new Field<>(
                        root.get("type").asText(),
                        root.get("name").asText(),
                        root.get("uberLogicalType").asText(),
                        root.get("columnType").asText(),
                        root.get("cardinality").asText(),
                        multiValuedField);
            }

            return fieldValue;
        }
    }
}
