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

package hypergraph.test.integration;

import hypergraph.Hypergraph;
import hypergraph.concept.type.AttributeType;
import hypergraph.concept.type.EntityType;
import hypergraph.concept.type.RelationType;
import hypergraph.concept.type.RoleType;
import hypergraph.concept.type.ThingType;
import hypergraph.core.CoreHypergraph;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BasicTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("grakn");

    private static void resetDirectory() throws IOException {
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
    }

    @Test
    public void loop_test_hypergraph() throws IOException {
        for (int i=0; i<1000; i++) {
            System.out.println(i + " ---- ");
            test_hypergraph();
        }
    }

    @Test
    public void test_hypergraph() throws IOException {
        resetDirectory();

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

                    Stream<Consumer<Hypergraph.Transaction>> rootTypeAssertions = Stream.of(
                            tx -> {
                                ThingType rootType = tx.concepts().getRootType();
                                assertNotNull(rootType);
                            },
                            tx -> {
                                EntityType rootEntityType = tx.concepts().getRootEntityType();
                                assertNotNull(rootEntityType);
                                assertNull(rootEntityType.sup());
                            },
                            tx -> {
                                RelationType rootRelationType = tx.concepts().getRootRelationType();
                                assertNotNull(rootRelationType);
                                assertNull(rootRelationType.sup());
                            },
                            tx -> {
                                AttributeType rootAttributeType = tx.concepts().getRootAttributeType();
                                assertNotNull(rootAttributeType);
                                assertNull(rootAttributeType.sup());
                            }
                    );

                    rootTypeAssertions.parallel().forEach(assertion -> assertion.accept(transaction));
                }

                try (Hypergraph.Transaction transaction = session.transaction(Hypergraph.Transaction.Type.WRITE)) {
                    assertTrue(transaction.isOpen());
                    assertEquals(CoreHypergraph.Transaction.Type.WRITE, transaction.type());

                    ThingType rootType = transaction.concepts().getRootType();
                    EntityType rootEntityType = transaction.concepts().getRootEntityType();
                    RelationType rootRelationType = transaction.concepts().getRootRelationType();
                    AttributeType rootAttributeType = transaction.concepts().getRootAttributeType();
                    notNulls(rootType, rootEntityType, rootRelationType, rootAttributeType);

                    Stream<Consumer<Hypergraph.Transaction>> typeAssertions = Stream.of(
                            tx -> {
                                AttributeType name = tx.concepts().putAttributeTypeString("name");
                                notNulls(name);
                                assertEquals(name.sup(), rootAttributeType);
                            },
                            tx -> {
                                AttributeType.Long age = tx.concepts().putAttributeTypeLong("age");
                                notNulls(age);
                                assertEquals(age.sup(), rootAttributeType);
                            },
                            tx -> {
                                RelationType marriage = tx.concepts().putRelationType("marriage");
                                marriage.relates("husband");
                                marriage.relates("wife");
                                notNulls(marriage);
                                assertEquals(marriage.sup(), rootRelationType);
                            },
                            tx -> {
                                RelationType employment = tx.concepts().putRelationType("employment");
                                employment.relates("employee");
                                employment.relates("employer");
                                notNulls(employment);
                                assertEquals(employment.sup(), rootRelationType);
                            },
                            tx -> {
                                EntityType person = tx.concepts().putEntityType("person");
                                notNulls(person);
                                assertEquals(person.sup(), rootEntityType);

                                Stream<Consumer<Hypergraph.Transaction>> subPersonAssertions = Stream.of(
                                        tx2 -> {
                                            EntityType man = tx2.concepts().putEntityType("man");
                                            man.sup(person);
                                            notNulls(man);
                                            assertEquals(man.sup(), person);
                                        },
                                        tx2 -> {
                                            EntityType woman = tx2.concepts().putEntityType("woman");
                                            woman.sup(person);
                                            notNulls(woman);
                                            assertEquals(woman.sup(), person);
                                        }
                                );
                                subPersonAssertions.parallel().forEach(assertions -> assertions.accept(tx));
                            },
                            tx -> {
                                EntityType company = tx.concepts().putEntityType("company");
                                notNulls(company);
                                assertEquals(company.sup(), rootEntityType);
                            }
                    );

                    typeAssertions.parallel().forEach(assertion -> assertion.accept(transaction));
                    transaction.commit();
                }

                try (Hypergraph.Transaction transaction = session.transaction(Hypergraph.Transaction.Type.READ)) {
                    assertTransactionRead(transaction);
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
                    assertTransactionRead(transaction);
                }

                try (Hypergraph.Transaction transaction = session.transaction(Hypergraph.Transaction.Type.WRITE)) {
                    AttributeType.String gender = transaction.concepts().putAttributeTypeString("gender");
                    EntityType school = transaction.concepts().putEntityType("school");
                    RelationType teaching = transaction.concepts().putRelationType("teaching");
                    teaching.relates("teacher");
                    teaching.relates("student");
                    RoleType teacher = teaching.role("teacher");
                    RoleType student = teaching.role("student");
                    notNulls(gender, school, teaching, teacher, student);
                    transaction.commit();
                }

                try (Hypergraph.Transaction transaction = session.transaction(Hypergraph.Transaction.Type.READ)) {
                    assertTransactionRead(transaction);
                    AttributeType.String gender = transaction.concepts().getAttributeType("gender").asString();
                    EntityType school = transaction.concepts().getEntityType("school");
                    RelationType teaching = transaction.concepts().getRelationType("teaching");
                    RoleType teacher = teaching.role("teacher");
                    RoleType student = teaching.role("student");
                    notNulls(gender, school, teaching, teacher, student);
                }
            }
        }
    }

    private static void assertTransactionRead(Hypergraph.Transaction transaction) {
        assertTrue(transaction.isOpen());
        assertEquals(CoreHypergraph.Transaction.Type.READ, transaction.type());

        ThingType rootType = transaction.concepts().getRootType();
        EntityType rootEntityType = transaction.concepts().getRootEntityType();
        RelationType rootRelationType = transaction.concepts().getRootRelationType();
        AttributeType rootAttributeType = transaction.concepts().getRootAttributeType();
        notNulls(rootType, rootEntityType, rootRelationType, rootAttributeType);

        Stream<Consumer<Hypergraph.Transaction>> typeAssertions = Stream.of(
                tx -> {
                    AttributeType.String name = tx.concepts().getAttributeType("name").asString();
                    notNulls(name);
                    assertEquals(name.sup(), rootAttributeType);
                },
                tx -> {
                    AttributeType.Long age = tx.concepts().getAttributeType("age").asLong();
                    notNulls(age);
                    assertEquals(age.sup(), rootAttributeType);
                },
                tx -> {
                    RelationType marriage = tx.concepts().getRelationType("marriage");
                    RoleType husband = marriage.role("husband");
                    RoleType wife = marriage.role("wife");
                    notNulls(marriage, husband, wife);
                    assertEquals(rootRelationType, marriage.sup());
                    assertEquals(rootRelationType.role("role"), husband.sup());
                    assertEquals(rootRelationType.role("role"), wife.sup());
                },
                tx -> {
                    RelationType employment = tx.concepts().getRelationType("employment");
                    RoleType employee = employment.role("employee");
                    RoleType employer = employment.role("employer");
                    notNulls(employment, employee, employer);
                    assertEquals(employment.sup(), rootRelationType);
                },
                tx -> {
                    EntityType person = tx.concepts().getEntityType("person");
                    notNulls(person);
                    assertEquals(person.sup(), rootEntityType);

                    Stream<Consumer<Hypergraph.Transaction>> subPersonAssertions = Stream.of(
                            tx2 -> {
                                EntityType man = tx2.concepts().getEntityType("man");
                                notNulls(man);
                                assertEquals(man.sup(), person);
                            },
                            tx2 -> {
                                EntityType woman = tx2.concepts().getEntityType("woman");
                                notNulls(woman);
                                assertEquals(woman.sup(), person);
                            }
                    );
                    subPersonAssertions.parallel().forEach(assertions -> assertions.accept(tx));
                },
                tx -> {
                    EntityType company = tx.concepts().getEntityType("company");
                    notNulls(company);
                    assertEquals(company.sup(), rootEntityType);
                }
        );

        typeAssertions.parallel().forEach(assertion -> assertion.accept(transaction));
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
