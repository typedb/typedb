/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.migration.json;

import com.google.common.collect.Sets;
import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.*;
import io.mindmaps.graql.internal.util.GraqlType;
import mjson.Json;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JsonDataMigratorTest {

    private MindmapsGraph graph;
    private JsonDataMigrator migrator;

    @Before
    public void setUp() {
        graph =  Mindmaps.factory(Mindmaps.IN_MEMORY, "default").getGraph();
        migrator = new JsonDataMigrator().graph(graph);
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test
    public void testMigrateSimpleSchemaData() {
        migrateSchema("simple-schema");
        Json data = readData("simple-schema");
        Instance root = migrator.migrateData("simple-schema", data);

        Type rootType = graph.getType("simple-schema");
        Collection<? extends Concept> instances = rootType.instances();
        assertEquals(1, instances.size());

        assertEquals(root, instances.iterator().next());

        Instance address = getProperty(root, "address");

        Resource streetAddress = getResource(address, "streetAddress").asResource();
        assertEquals("21 2nd Street", streetAddress.getValue());

        Resource city = getResource(address, "city").asResource();
        assertEquals("New York", city.getValue());

        Instance phoneNumberArray = getProperty(root, "phoneNumber");

        Collection<Instance> phoneNumbers = getProperties(phoneNumberArray, "phoneNumber-item").collect(toSet());

        boolean phoneNumbersCorrect = phoneNumbers.stream().allMatch(phoneNumber -> {
            Object location = getResource(phoneNumber, "location").getValue();
            Object code = getResource(phoneNumber, "code").getValue();
            return ((location.equals("home") && code.equals(44L)) || (location.equals("work") && code.equals(45L)));
        });

        assertTrue(phoneNumbersCorrect);
    }

    @Test
    public void testMigrateAllTypesData() {
        migrateSchema("all-types");
        Json data = readData("all-types");
        migrator.migrateData("all-types", data);

        Type rootType = graph.getType("all-types");
        Collection<? extends Concept> instances = rootType.instances();
        assertEquals(1, instances.size());

        Instance root = instances.iterator().next().asInstance();

        Instance arrayOfInts = getProperty(root, "array-of-ints");
        Collection<Object> integers =
                getResources(arrayOfInts, "array-of-ints-item").map(r -> r.asResource().getValue()).collect(toSet());

        assertEquals(Sets.newHashSet(1L, 2L, 3L), integers);

        Resource aBoolean = getResource(root, "a-boolean");
        assertEquals(true, aBoolean.getValue());

        Resource aNumber = getResource(root, "a-number");
        assertEquals(42.1, aNumber.getValue());

        Resource aString = getResource(root, "a-string");
        assertEquals("hi", aString.getValue());

        Instance aNull = getProperty(root, "a-null");
        assertNotNull(aNull);
    }

    @Test
    public void testMigrateStringOrObjectData1() {
        migrateSchema("string-or-object");
        Json data = readData("string-or-object", "1");
        migrator.migrateData("string-or-object", data);

        Type rootType = graph.getType("string-or-object");
        Collection<? extends Concept> instances = rootType.instances();
        assertEquals(1, instances.size());

        Instance root = instances.iterator().next().asInstance();

        Type thingStringType = graph.getResourceType("the-thing-string");
        Resource thingString = getResource(root, "the-thing");
        assertEquals(thingStringType, thingString.type());
        assertEquals("hello", thingString.getValue());
    }

    @Test
    public void testMigrateStringOrObjectData2() {
        migrateSchema("string-or-object");
        Json data = readData("string-or-object", "2");
        migrator.migrateData("string-or-object", data);

        Type rootType = graph.getType("string-or-object");
        Collection<? extends Concept> instances = rootType.instances();
        assertEquals(1, instances.size());

        Instance root = instances.iterator().next().asInstance();

        Type thingObjType = graph.getResourceType("the-thing-object");
        Instance thingObj = getResource(root, "the-thing");
        assertEquals(thingObjType, thingObj.type());

        Type aNumberType = graph.getResourceType("a-number");
        Resource aNumber = getResource(thingObj, "a-number");
        assertEquals(aNumberType, aNumber.type());
        assertEquals(4.0, aNumber.getValue());
    }

    private void migrateSchema(String name) {
        Json.Schema schema = Util.readSchema(name);
        new JsonSchemaMigrator().graph(graph).migrateSchema(name, schema);
    }

    private Json readData(String name) {
        return readData(name, "");
    }

    private Json readData(String name, String suffix) {
        return Util.readJson(name + "/data" + suffix + ".json");
    }

    private Instance getProperty(Instance instance, String name) {
        assertEquals(1, getProperties(instance, name).count());
        return getProperties(instance, name).findAny().get();
    }

    private Stream<Instance> getProperties(Instance instance, String name) {
        RoleType roleOwner = graph.getRoleType(name + "-owner");
        RoleType roleOther = graph.getRoleType(name + "-role");

        Collection<Relation> relations = instance.relations(roleOwner);
        return relations.stream().map(r -> r.rolePlayers().get(roleOther));
    }

    private Resource getResource(Instance instance, String name) {
        assertEquals(1, getResources(instance, name).count());
        return getResources(instance, name).findAny().get();
    }

    private Stream<Resource> getResources(Instance instance, String name) {
        RoleType roleOwner = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(name));
        RoleType roleOther = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(name));

        Collection<Relation> relations = instance.relations(roleOwner);
        return relations.stream().map(r -> r.rolePlayers().get(roleOther).asResource());
    }
}
