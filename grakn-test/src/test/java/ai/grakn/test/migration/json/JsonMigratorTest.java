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

import ai.grakn.migration.json.JsonMigrator;
import ai.grakn.test.migration.AbstractGraknMigratorTest;
import com.google.common.collect.Sets;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collection;

import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JsonMigratorTest extends AbstractGraknMigratorTest {

    @Test
    public void testMigrateSimpleSchemaData() {
        load(getFile("json", "simple-schema/schema.gql"));

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
                "for ( phoneNumber ) do {\n" +
                "  $phone isa phone-number\n" +
                "    has location <location>\n" +
                "    has code <code>;\n" +
                "  \n" +
                "  (person-with-phone: $person, phone-of-person: $phone) isa has-phone;\n" +
                "  \n" +
                "} ";

        migrate(new JsonMigrator(template, getFile("json", "simple-schema/data.json")));

        graph = factory.getGraph();
        EntityType personType = graph.getEntityType("person");
        assertEquals(1, personType.instances().size());

        Entity person = personType.instances().iterator().next();

        Entity address = getProperty(person, "has-address").asEntity();

        Entity streetAddress = getProperty(address, "address-has-street").asEntity();

        Resource number = getResource(streetAddress, "number").asResource();
        assertEquals(21L, number.getValue());

        Resource street = getResource(streetAddress, "street").asResource();
        assertEquals("2nd Street", street.getValue());

        Resource city = getResource(address, "city").asResource();
        assertEquals("New York", city.getValue());

        Collection<Instance> phoneNumbers = getProperties(person, "has-phone");
        assertEquals(2, phoneNumbers.size());

        boolean phoneNumbersCorrect = phoneNumbers.stream().allMatch(phoneNumber -> {
            Object location = getResource(phoneNumber, "location").getValue();
            Object code = getResource(phoneNumber, "code").getValue();
            return ((location.equals("home") && code.equals(44L)) || (location.equals("work") && code.equals(45L)));
        });

        assertTrue(phoneNumbersCorrect);
    }

    @Test
    public void testMigrateAllTypesData() throws FileNotFoundException {
        load(getFile("json", "all-types/schema.gql"));

        String template = "" +
                "insert $x isa thing\n" +
                "  has a-boolean <a-boolean>\n" +
                "  has a-number  <a-number>\n" +
                "  for (int in array-of-ints ) do {\n" +
                "  has a-int <int>\n" +
                "  }\n" +
                "  has a-string <a-string>\n" +
                "  if (ne a-null null) do {has a-null <a-null>};";

        migrate(new JsonMigrator(template, new FileReader(getFile("json", "all-types/data.json"))));

        graph = factory.getGraph();
        EntityType rootType = graph.getEntityType("thing");
        Collection<Entity> things = rootType.instances();
        assertEquals(1, things.size());

        Entity thing = things.iterator().next();

        Collection<Object> integers = getResources(thing, "a-int").map(r -> r.asResource().getValue()).collect(toSet());
        assertEquals(Sets.newHashSet(1L, 2L, 3L), integers);

        Resource aBoolean = getResource(thing, "a-boolean");
        assertEquals(true, aBoolean.getValue());

        Resource aNumber = getResource(thing, "a-number");
        assertEquals(42.1, aNumber.getValue());

        Resource aString = getResource(thing, "a-string");
        assertEquals("hi", aString.getValue());

        assertEquals(0, graph.getResourceType("a-null").instances().size());
    }

    @Test
    public void testMigrateDirectory(){
        load(getFile("json", "string-or-object/schema.gql"));

        String template = "\n" +
                "insert $thing isa the-thing\n" +
                "        has a-string if (ne the-thing.a-string null) do {<the-thing.a-string>}\n" +
                "        else {<the-thing>} ;";

        migrate(new JsonMigrator(template, getFile("json", "string-or-object/data")));

        graph = factory.getGraph();
        EntityType theThing = graph.getEntityType("the-thing");
        assertEquals(2, theThing.instances().size());

        Collection<Entity> things = theThing.instances();
        boolean thingsCorrect = things.stream().allMatch(thing -> {
            Object string = getResource(thing, "a-string").getValue();
            return string.equals("hello") || string.equals("goodbye");
        });

        assertTrue(thingsCorrect);
    }

    @Test
    public void testStringOrObject(){
        load(getFile("json", "string-or-object/schema.gql"));

        String template = "\n" +
                "insert $thing isa the-thing\n" +
                "        has a-string if (ne the-thing.a-string null) do {<the-thing.a-string>}\n" +
                "        else {<the-thing>} ;";

        migrate(new JsonMigrator(template, getFile("json", "string-or-object/data")));

        graph = factory.getGraph();
        EntityType theThing = graph.getEntityType("the-thing");
        assertEquals(2, theThing.instances().size());
    }
}
