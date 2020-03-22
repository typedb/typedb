/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package hypergraph;

import hypergraph.concept.type.AttributeType;
import hypergraph.concept.type.EntityType;
import hypergraph.concept.type.RelationType;
import hypergraph.concept.type.Type;
import hypergraph.core.CoreHypergraph;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HypergraphTest {

    @Test
    public void test_hypergraph() throws IOException {
        Path directory = Paths.get(System.getProperty("user.dir")).resolve("grakn");
        if (Files.exists(directory)) {
            System.out.println("Database directory exists!");
            Files.walk(directory)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            System.out.println("Database directory deleted!");
        }

        Files.createDirectory(directory);

        System.out.println("Database Directory created: " + directory.toString());

        try (Hypergraph graph = CoreHypergraph.open(directory.toString())) {
            graph.keyspaces().create("my_data_keyspace");

            assertTrue(graph.isOpen());
            assertEquals(1, graph.keyspaces().getAll().size());
            assertEquals("my_data_keyspace", graph.keyspaces().getAll().iterator().next().name());

            try (Hypergraph.Session session = graph.session("my_data_keyspace")) {

                assertTrue(session.isOpen());
                assertEquals("my_data_keyspace", session.keyspace().name());

                try (Hypergraph.Transaction transaction = session.transaction(Hypergraph.Transaction.Type.READ)) {
                    assertTrue(transaction.isOpen());
                    assertEquals(CoreHypergraph.Transaction.Type.READ, transaction.type());

                    Type rootType = transaction.concepts().getRootType();
                    EntityType rootEntityType = transaction.concepts().getRootEntityType();
                    RelationType rootRelationType = transaction.concepts().getRootRelationType();
                    AttributeType rootAttributeType = transaction.concepts().getRootAttributeType();
                    notNulls(rootType, rootEntityType, rootRelationType, rootAttributeType);

                    nulls(rootEntityType.sup(), rootRelationType.sup(), rootAttributeType.sup());
                }

                try (Hypergraph.Transaction transaction = session.transaction(Hypergraph.Transaction.Type.WRITE)) {
                    assertTrue(transaction.isOpen());
                    assertEquals(CoreHypergraph.Transaction.Type.WRITE, transaction.type());

                    Type rootType = transaction.concepts().getRootType();
                    EntityType rootEntityType = transaction.concepts().getRootEntityType();
                    RelationType rootRelationType = transaction.concepts().getRootRelationType();
                    AttributeType rootAttributeType = transaction.concepts().getRootAttributeType();
                    notNulls(rootType, rootEntityType, rootRelationType, rootAttributeType);

                    AttributeType name = transaction.concepts().putAttributeType("name");
                    AttributeType age = transaction.concepts().putAttributeType("age");
                    notNulls(name, age);
                    assertEquals(name.sup(), rootAttributeType);
                    assertEquals(age.sup(), rootAttributeType);

                    RelationType marriage = transaction.concepts().putRelationType("marriage");
                    RelationType employment = transaction.concepts().putRelationType("employment");
                    notNulls(marriage, employment);
                    assertEquals(marriage.sup(), rootRelationType);
                    assertEquals(employment.sup(), rootRelationType);

                    EntityType person = transaction.concepts().putEntityType("person");
                    EntityType man = transaction.concepts().putEntityType("man");
                    EntityType woman = transaction.concepts().putEntityType("woman");
                    EntityType company = transaction.concepts().putEntityType("company");
                    notNulls(person, man, woman, company);
                    assertEquals(person.sup(), rootEntityType);
                    assertEquals(man.sup(), rootEntityType);
                    assertEquals(woman.sup(), rootEntityType);
                    assertEquals(company.sup(), rootEntityType);

                    transaction.commit();
                }

                try (Hypergraph.Transaction transaction = session.transaction(Hypergraph.Transaction.Type.READ)) {
                    assertTrue(transaction.isOpen());
                    assertEquals(CoreHypergraph.Transaction.Type.READ, transaction.type());

//                    transaction.read().getConcept(...)
                }
            }
        }


        try (Hypergraph graph = CoreHypergraph.open(directory.toString())) {

            assertTrue(graph.isOpen());
            assertEquals(1, graph.keyspaces().getAll().size());
            assertEquals("my_data_keyspace", graph.keyspaces().getAll().iterator().next().name());

            try (Hypergraph.Session session = graph.session("my_data_keyspace")) {

                assertTrue(session.isOpen());
                assertEquals("my_data_keyspace", session.keyspace().name());

                try (Hypergraph.Transaction transaction = session.transaction(Hypergraph.Transaction.Type.READ)) {
                    assertTrue(transaction.isOpen());
                    assertEquals(CoreHypergraph.Transaction.Type.READ, transaction.type());
                    assertNotNull(transaction.concepts().getRootType());
                    assertNotNull(transaction.concepts().getRootEntityType());
                    assertNotNull(transaction.concepts().getRootRelationType());
                    assertNotNull(transaction.concepts().getRootAttributeType());
//                    transaction.read().getConcept(...)
                }
            }
        }
    }

    private static void notNulls(Object... objects) {
        for (Object object : objects) {
            assertNotNull(object);
        }
    }

    private static void nulls(Object... objects) {
        for (Object object : objects) {
            assertNull(object);
        }
    }
}
