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

import com.facebook.presto.rta.RtaStorageType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRTAClient
{
    @Test
    public void testDeployments()
            throws IOException
    {
        List<RTADeployment> deployments = TestSchemaUtils.getDeployments();
        assertEquals(deployments.size(), 1);
        RTADeployment deployment = deployments.get(0);
        assertEquals(deployment.getCluster(), "stagingb");
        assertEquals(deployment.getName(), "rta_eats_order");
        assertEquals(deployment.getNamespace(), "rta");
        assertEquals(deployment.getStorageType(), RtaStorageType.ARESDB);
        assertEquals(deployment.getRtaCluster().getStorageType(), "ARESDB");
        assertEquals(deployment.getRtaCluster().getRegion(), "phx2");
        assertEquals(deployment.getRtaCluster().getName(), "ares-stagingb");
        assertEquals(deployment.getRtaCluster().getMuttleyRoService(), "gforcedb-stagingb");
        assertEquals(deployment.getRtaCluster().getMuttleyRwService(), "ares-controller");
    }

    @Test
    public void testDefinition()
            throws IOException
    {
        RTADefinition definiton = TestSchemaUtils.getDefinition();
        assertEquals(definiton.getFields().size(), 6);
        assertEquals(definiton.getMetadata().getPrimaryKeys().toArray(), new String[]{"workflowUUID"});
        assertTrue(definiton.getMetadata().isFactTable());
        assertEquals(definiton.getMetadata().getQueryTypes().toArray(), new String[]{"pre_defined"});
        assertEquals(definiton.getFields().get(0).getCardinality(), "high");
        assertEquals(definiton.getFields().get(0).getColumnType(), "metric");
        assertEquals(definiton.getFields().get(0).getName(), "workflowUUID");
        assertEquals(definiton.getFields().get(0).getUberLogicalType(), "String");
        assertEquals(definiton.getFields().get(0).getType(), "string");
        assertTrue(!definiton.getFields().get(0).isMultiValueField());
    }

    @Test
    public void testMultiValuedDefinition()
            throws IOException
    {
        RTADefinition definiton = TestSchemaUtils.getDefinition();
        assertEquals(definiton.getFields().size(), 6);

        assertEquals(definiton.getFields().get(4).getCardinality(), "high");
        assertEquals(definiton.getFields().get(4).getColumnType(), "dimension");
        assertEquals(definiton.getFields().get(4).getName(), "array_string");
        assertEquals(definiton.getFields().get(4).getUberLogicalType(), "Set");
        assertTrue(definiton.getFields().get(4).isMultiValueField());
        assertTrue(definiton.getFields().get(4).getType() instanceof RTADefinition.DataType);
        RTADefinition.DataType type = (RTADefinition.DataType) definiton.getFields().get(4).getType();
        assertEquals(type.getType(), "array");
        assertEquals(type.getItems(), "string");

        assertEquals(definiton.getFields().get(5).getCardinality(), "high");
        assertEquals(definiton.getFields().get(5).getColumnType(), "dimension");
        assertEquals(definiton.getFields().get(5).getName(), "array_long");
        assertEquals(definiton.getFields().get(5).getUberLogicalType(), "Set");
        assertTrue(definiton.getFields().get(5).isMultiValueField());
        assertTrue(definiton.getFields().get(5).getType() instanceof RTADefinition.DataType);
        type = (RTADefinition.DataType) definiton.getFields().get(5).getType();
        assertEquals(type.getType(), "array");
        assertEquals(type.getItems(), "long");
    }

    @Test
    public void testNamespaces()
            throws IOException
    {
        List<String> namespaces = TestSchemaUtils.getNamespaces();
        assertEquals(namespaces.toArray(), new String[]{"rta"});
    }

    @Test
    public void testTables()
            throws IOException
    {
        List<String> tables = TestSchemaUtils.getTables();
        assertEquals(tables.size(), 14);
    }
}
