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

package io.grakn.migration.json;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import io.grakn.MindmapsGraph;
import io.grakn.concept.Entity;
import io.grakn.concept.EntityType;
import io.grakn.concept.Instance;
import io.grakn.concept.Resource;
import io.grakn.engine.MindmapsEngineServer;
import io.grakn.engine.loader.BlockingLoader;
import io.grakn.engine.util.ConfigProperties;
import io.grakn.exception.MindmapsValidationException;
import io.grakn.factory.GraphFactory;
import io.grakn.graql.Graql;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import static io.grakn.migration.json.JsonMigratorUtil.getFile;
import static io.grakn.migration.json.JsonMigratorUtil.getProperties;
import static io.grakn.migration.json.JsonMigratorUtil.getProperty;
import static io.grakn.migration.json.JsonMigratorUtil.getResource;
import static io.grakn.migration.json.JsonMigratorUtil.getResources;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JsonMigratorTest {

    private MindmapsGraph graph;
    private JsonMigrator migrator;

    @BeforeClass
    public static void start(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        MindmapsEngineServer.start();
    }

    @AfterClass
    public static void stop(){
        MindmapsEngineServer.stop();
    }

    @Before
    public void setup(){
        String GRAPH_NAME = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);

        graph = GraphFactory.getInstance().getGraphBatchLoading(GRAPH_NAME);
        BlockingLoader loader = new BlockingLoader(GRAPH_NAME);
        loader.setExecutorSize(1);

        migrator = new JsonMigrator(loader);
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test
    public void testMigrateSimpleSchemaData() {
        load(getFile("simple-schema/schema.gql"));

        String template = "  \n" +
                "$person isa person;\n" +
                " \n" +
                "$address isa address\n" +
                "  has city <address.city>;\n" +
                "\n" +
                "$street isa street-address\n" +
                "   has street <address.streetAddress.street>\n" +
                "   has number <address.streetAddress.number>;\n" +
                "\n" +
                "(address-with-street: $address, street-of-address: $street) isa address-has-street;\n" +
                "\n" +
                "(person-with-address: $person, address-of-person: $address) isa has-address;\n" +
                "\n" +
                "for ( phoneNumber ) do {\n" +
                "  $phone isa phone-number\n" +
                "    has location <location>\n" +
                "    has code <code>;\n" +
                "  \n" +
                "  (person-with-phone: $person, phone-of-person: $phone) isa has-phone;\n" +
                "  \n" +
                "} ";

        migrate(template, getFile("simple-schema/data.json"));

        EntityType personType = graph.getEntityType("person");
        assertEquals(1, personType.instances().size());

        Entity person = personType.instances().iterator().next();

        Entity address = getProperty(graph, person, "has-address").asEntity();

        Entity streetAddress = getProperty(graph, address, "address-has-street").asEntity();

        Resource number = getResource(graph, streetAddress, "number").asResource();
        assertEquals(21L, number.getValue());

        Resource street = getResource(graph, streetAddress, "street").asResource();
        assertEquals("2nd Street", street.getValue());

        Resource city = getResource(graph, address, "city").asResource();
        assertEquals("New York", city.getValue());

        Collection<Instance> phoneNumbers = getProperties(graph, person, "has-phone");
        assertEquals(2, phoneNumbers.size());

        boolean phoneNumbersCorrect = phoneNumbers.stream().allMatch(phoneNumber -> {
            Object location = getResource(graph, phoneNumber, "location").getValue();
            Object code = getResource(graph, phoneNumber, "code").getValue();
            return ((location.equals("home") && code.equals(44L)) || (location.equals("work") && code.equals(45L)));
        });

        assertTrue(phoneNumbersCorrect);
    }

    @Test
    public void testMigrateAllTypesData() {
        load(getFile("all-types/schema.gql"));

        String template = "" +
                "$x isa thing\n" +
                "  has a-boolean <a-boolean>\n" +
                "  has a-number  <a-number>\n" +
                "  for (int in array-of-ints ) do {\n" +
                "  has a-int <int>\n" +
                "  }\n" +
                "  has a-string <a-string>\n" +
                "  if (ne a-null null) do {has a-null <a-null>};";

        migrate(template, getFile("all-types/data.json"));

        EntityType rootType = graph.getEntityType("thing");
        Collection<Entity> things = rootType.instances();
        assertEquals(1, things.size());

        Entity thing = things.iterator().next();

        Collection<Object> integers = getResources(graph, thing, "a-int").map(r -> r.asResource().getValue()).collect(toSet());
        assertEquals(Sets.newHashSet(1L, 2L, 3L), integers);

        Resource aBoolean = getResource(graph, thing, "a-boolean");
        assertEquals(true, aBoolean.getValue());

        Resource aNumber = getResource(graph, thing, "a-number");
        assertEquals(42.1, aNumber.getValue());

        Resource aString = getResource(graph, thing, "a-string");
        assertEquals("hi", aString.getValue());

        assertEquals(0, graph.getResourceType("a-null").instances().size());
    }

    @Test
    public void testMigrateDirectory(){
        load(getFile("string-or-object/schema.gql"));

        String template = "\n" +
                "$thing isa the-thing\n" +
                "        has a-string if (ne the-thing.a-string null) do {<the-thing.a-string>}\n" +
                "        else {<the-thing>} ;";

        migrate(template, getFile("string-or-object/data"));

        EntityType theThing = graph.getEntityType("the-thing");
        assertEquals(2, theThing.instances().size());

        Collection<Entity> things = theThing.instances();
        boolean thingsCorrect = things.stream().allMatch(thing -> {
            Object string = getResource(graph, thing, "a-string").getValue();
            return string.equals("hello") || string.equals("goodbye");
        });

        assertTrue(thingsCorrect);
    }

    @Test
    public void testStringOrObject(){
        load(getFile("string-or-object/schema.gql"));

        String template = "\n" +
                "$thing isa the-thing\n" +
                "        has a-string if (ne the-thing.a-string null) do {<the-thing.a-string>}\n" +
                "        else {<the-thing>} ;";

        migrate(template, getFile("string-or-object/data"));

        EntityType theThing = graph.getEntityType("the-thing");
        assertEquals(2, theThing.instances().size());
    }

    // common class
    private void migrate(String template, File file){
        migrator.migrate(template, file);
    }

    private void load(File ontology) {
        try {
            Graql.withGraph(graph)
                    .parse(Files.readLines(ontology, StandardCharsets.UTF_8).stream().collect(joining("\n")))
                    .execute();

            graph.commit();
        } catch (IOException|MindmapsValidationException e){
            throw new RuntimeException(e);
        }
    }
}
