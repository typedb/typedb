/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.migration.json;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.concept.TypeLabel;
import ai.grakn.migration.base.Migrator;
import ai.grakn.migration.json.JsonMigrator;
import ai.grakn.test.EngineContext;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.Collection;

import static ai.grakn.test.migration.MigratorTestUtils.getFile;
import static ai.grakn.test.migration.MigratorTestUtils.getProperties;
import static ai.grakn.test.migration.MigratorTestUtils.getProperty;
import static ai.grakn.test.migration.MigratorTestUtils.getResource;
import static ai.grakn.test.migration.MigratorTestUtils.getResources;
import static ai.grakn.test.migration.MigratorTestUtils.load;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JsonMigratorTest {

    private Migrator migrator;
    private GraknSession factory;

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Before
    public void setup(){
        factory = engine.factoryWithNewKeyspace();
        GraknGraph graph = factory.open(GraknTxType.WRITE);
        migrator = Migrator.to(Grakn.DEFAULT_URI, graph.getKeyspace());
        graph.close();
    }

    @Test
    public void testMigrateSimpleSchemaData() {
        load(factory, getFile("json", "simple-schema/schema.gql"));

        String template = "  \n" +
                "insert " +
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
                "for ( <phoneNumber> ) do {\n" +
                "  $phone isa phone-number\n" +
                "    has location <location>\n" +
                "    has code <code>;\n" +
                "  \n" +
                "  (person-with-phone: $person, phone-of-person: $phone) isa has-phone;\n" +
                "  \n" +
                "} ";

        declareAndLoad(template, "simple-schema/data.json");

        GraknGraph graph = factory.open(GraknTxType.WRITE);
        EntityType personType = graph.getEntityType("person");
        assertEquals(1, personType.instances().size());

        Entity person = personType.instances().iterator().next();

        Entity address = getProperty(graph, person, "has-address").asEntity();

        Entity streetAddress = getProperty(graph, address, "address-has-street").asEntity();

        Resource number = getResource(graph, streetAddress, TypeLabel.of("number")).asResource();
        assertEquals(21L, number.getValue());

        Resource street = getResource(graph, streetAddress, TypeLabel.of("street")).asResource();
        assertEquals("2nd Street", street.getValue());

        Resource city = getResource(graph, address, TypeLabel.of("city")).asResource();
        assertEquals("New York", city.getValue());

        Collection<Instance> phoneNumbers = getProperties(graph, person, "has-phone");
        assertEquals(2, phoneNumbers.size());

        boolean phoneNumbersCorrect = phoneNumbers.stream().allMatch(phoneNumber -> {
            Object location = getResource(graph, phoneNumber, TypeLabel.of("location")).getValue();
            Object code = getResource(graph, phoneNumber, TypeLabel.of("code")).getValue();
            return ((location.equals("home") && code.equals(44L)) || (location.equals("work") && code.equals(45L)));
        });

        assertTrue(phoneNumbersCorrect);
    }

    @Test
    public void testMigrateAllTypesData() throws FileNotFoundException {
        load(factory, getFile("json", "all-types/schema.gql"));

        String template = "" +
                "insert $x isa thing\n" +
                "  has a-boolean <a-boolean>\n" +
                "  has a-number  <a-number>\n" +
                "  for (int in <array-of-ints> ) do {\n" +
                "  has a-int <int>\n" +
                "  }\n" +
                "  has a-string <a-string>\n" +
                "  if (<a-null> != null) do {has a-null <a-null>};";

        declareAndLoad(template, "all-types/data.json");

        GraknGraph graph = factory.open(GraknTxType.WRITE);
        EntityType rootType = graph.getEntityType("thing");
        Collection<Entity> things = rootType.instances();
        assertEquals(1, things.size());

        Entity thing = things.iterator().next();

        Collection<Object> integers = getResources(graph, thing, TypeLabel.of("a-int")).map(r -> r.asResource().getValue()).collect(toSet());
        assertEquals(Sets.newHashSet(1L, 2L, 3L), integers);

        Resource aBoolean = getResource(graph, thing, TypeLabel.of("a-boolean"));
        assertEquals(true, aBoolean.getValue());

        Resource aNumber = getResource(graph, thing, TypeLabel.of("a-number"));
        assertEquals(42.1, aNumber.getValue());

        Resource aString = getResource(graph, thing, TypeLabel.of("a-string"));
        assertEquals("hi", aString.getValue());

        assertEquals(0, graph.getResourceType("a-null").instances().size());
    }

    @Test
    public void testMigrateDirectory(){
        load(factory, getFile("json", "string-or-object/schema.gql"));

        String template = "\n" +
                "insert $thing isa the-thing\n" +
                "        has a-string if (<the-thing.a-string> != null) do {<the-thing.a-string>}\n" +
                "        else {<the-thing>} ;";

        declareAndLoad(template, "string-or-object/data");

        GraknGraph graph = factory.open(GraknTxType.WRITE);
        EntityType theThing = graph.getEntityType("the-thing");
        assertEquals(2, theThing.instances().size());

        Collection<Entity> things = theThing.instances();
        boolean thingsCorrect = things.stream().allMatch(thing -> {
            Object string = getResource(graph, thing, TypeLabel.of("a-string")).getValue();
            return string.equals("hello") || string.equals("goodbye");
        });

        assertTrue(thingsCorrect);
    }

    @Test
    public void testStringOrObject(){
        load(factory, getFile("json", "string-or-object/schema.gql"));

        String template = "\n" +
                "insert $thing isa the-thing\n" +
                "        has a-string if (<the-thing.a-string> != null) do {<the-thing.a-string>}\n" +
                "        else {<the-thing>} ;";

        declareAndLoad(template, "string-or-object/data");

        GraknGraph graph = factory.open(GraknTxType.WRITE);
        EntityType theThing = graph.getEntityType("the-thing");
        assertEquals(2, theThing.instances().size());
    }

    @Test
    public void testMissingDataDoesNotThrowError(){
        load(factory, getFile("json", "string-or-object/schema.gql"));
        String template = "insert $thing isa the-thing has a-string <the-thing.a-string>;";
        declareAndLoad(template, "string-or-object/data");

        GraknGraph graph = factory.open(GraknTxType.WRITE);
        EntityType theThing = graph.getEntityType("the-thing");
        assertEquals(1, theThing.instances().size());
    }

    private void declareAndLoad(String template, String file){
        try(JsonMigrator m = new JsonMigrator(getFile("json", file))){
            migrator.load(template, m.convert());
        }
    }
}
