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
import hypergraph.concept.type.AttributeTypeInt;
import hypergraph.concept.type.AttributeTypeImpl;
import hypergraph.concept.type.EntityTypeInt;
import hypergraph.concept.type.EntityTypeImpl;
import hypergraph.concept.type.RelationTypeInt;
import hypergraph.concept.type.RoleTypeInt;
import hypergraph.concept.type.ThingTypeInt;
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
                                ThingTypeInt rootType = tx.concepts().getRootType();
                                assertNotNull(rootType);
                            },
                            tx -> {
                                EntityTypeInt rootEntityType = tx.concepts().getRootEntityType();
                                assertNotNull(rootEntityType);
                                assertNull(rootEntityType.sup());
                            },
                            tx -> {
                                RelationTypeInt rootRelationType = tx.concepts().getRootRelationType();
                                assertNotNull(rootRelationType);
                                assertNull(rootRelationType.sup());
                            },
                            tx -> {
                                AttributeTypeInt rootAttributeType = tx.concepts().getRootAttributeType();
                                assertNotNull(rootAttributeType);
                                assertNull(rootAttributeType.sup());
                            }
                    );

                    rootTypeAssertions.parallel().forEach(assertion -> assertion.accept(transaction));
                }

                try (Hypergraph.Transaction transaction = session.transaction(Hypergraph.Transaction.Type.WRITE)) {
                    assertTrue(transaction.isOpen());
                    assertEquals(CoreHypergraph.Transaction.Type.WRITE, transaction.type());

                    ThingTypeInt rootType = transaction.concepts().getRootType();
                    EntityTypeInt rootEntityType = transaction.concepts().getRootEntityType();
                    RelationTypeInt rootRelationType = transaction.concepts().getRootRelationType();
                    AttributeTypeInt rootAttributeType = transaction.concepts().getRootAttributeType();
                    notNulls(rootType, rootEntityType, rootRelationType, rootAttributeType);

                    Stream<Consumer<Hypergraph.Transaction>> typeAssertions = Stream.of(
                            tx -> {
                                AttributeTypeInt name = tx.concepts().putAttributeTypeString("name");
                                notNulls(name);
                                assertEquals(name.sup(), rootAttributeType);
                            },
                            tx -> {
                                AttributeTypeImpl.Long age = tx.concepts().putAttributeTypeLong("age");
                                notNulls(age);
                                assertEquals(age.sup(), rootAttributeType);
                            },
                            tx -> {
                                RelationTypeInt marriage = tx.concepts().putRelationType("marriage");
                                marriage.relates("husband");
                                marriage.relates("wife");
                                notNulls(marriage);
                                assertEquals(marriage.sup(), rootRelationType);
                            },
                            tx -> {
                                RelationTypeInt employment = tx.concepts().putRelationType("employment");
                                employment.relates("employee");
                                employment.relates("employer");
                                notNulls(employment);
                                assertEquals(employment.sup(), rootRelationType);
                            },
                            tx -> {
                                EntityTypeInt person = tx.concepts().putEntityType("person");
                                notNulls(person);
                                assertEquals(person.sup(), rootEntityType);

                                Stream<Consumer<Hypergraph.Transaction>> subPersonAssertions = Stream.of(
                                        tx2 -> {
                                            EntityTypeImpl man = tx2.concepts().putEntityType("man");
                                            man.sup(person);
                                            notNulls(man);
                                            assertEquals(man.sup(), person);
                                        },
                                        tx2 -> {
                                            EntityTypeImpl woman = tx2.concepts().putEntityType("woman");
                                            woman.sup(person);
                                            notNulls(woman);
                                            assertEquals(woman.sup(), person);
                                        }
                                );
                                subPersonAssertions.parallel().forEach(assertions -> assertions.accept(tx));
                            },
                            tx -> {
                                EntityTypeInt company = tx.concepts().putEntityType("company");
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
                    AttributeTypeImpl.String gender = transaction.concepts().putAttributeTypeString("gender");
                    EntityTypeInt school = transaction.concepts().putEntityType("school");
                    RelationTypeInt teaching = transaction.concepts().putRelationType("teaching");
                    teaching.relates("teacher");
                    teaching.relates("student");
                    RoleTypeInt teacher = teaching.role("teacher");
                    RoleTypeInt student = teaching.role("student");
                    notNulls(gender, school, teaching, teacher, student);
                    transaction.commit();
                }

                try (Hypergraph.Transaction transaction = session.transaction(Hypergraph.Transaction.Type.READ)) {
                    assertTransactionRead(transaction);
                    AttributeTypeImpl.String gender = transaction.concepts().getAttributeType("gender").asString();
                    EntityTypeInt school = transaction.concepts().getEntityType("school");
                    RelationTypeInt teaching = transaction.concepts().getRelationType("teaching");
                    RoleTypeInt teacher = teaching.role("teacher");
                    RoleTypeInt student = teaching.role("student");
                    notNulls(gender, school, teaching, teacher, student);
                }
            }
        }
    }

    private static void assertTransactionRead(Hypergraph.Transaction transaction) {
        assertTrue(transaction.isOpen());
        assertEquals(CoreHypergraph.Transaction.Type.READ, transaction.type());

        ThingTypeInt rootType = transaction.concepts().getRootType();
        EntityTypeInt rootEntityType = transaction.concepts().getRootEntityType();
        RelationTypeInt rootRelationType = transaction.concepts().getRootRelationType();
        AttributeTypeInt rootAttributeType = transaction.concepts().getRootAttributeType();
        notNulls(rootType, rootEntityType, rootRelationType, rootAttributeType);

        Stream<Consumer<Hypergraph.Transaction>> typeAssertions = Stream.of(
                tx -> {
                    AttributeTypeImpl.String name = tx.concepts().getAttributeType("name").asString();
                    notNulls(name);
                    assertEquals(name.sup(), rootAttributeType);
                },
                tx -> {
                    AttributeTypeImpl.Long age = tx.concepts().getAttributeType("age").asLong();
                    notNulls(age);
                    assertEquals(age.sup(), rootAttributeType);
                },
                tx -> {
                    RelationTypeInt marriage = tx.concepts().getRelationType("marriage");
                    RoleTypeInt husband = marriage.role("husband");
                    RoleTypeInt wife = marriage.role("wife");
                    notNulls(marriage, husband, wife);
                    assertEquals(rootRelationType, marriage.sup());
                    assertEquals(rootRelationType.role("role"), husband.sup());
                    assertEquals(rootRelationType.role("role"), wife.sup());
                },
                tx -> {
                    RelationTypeInt employment = tx.concepts().getRelationType("employment");
                    RoleTypeInt employee = employment.role("employee");
                    RoleTypeInt employer = employment.role("employer");
                    notNulls(employment, employee, employer);
                    assertEquals(employment.sup(), rootRelationType);
                },
                tx -> {
                    EntityTypeInt person = tx.concepts().getEntityType("person");
                    notNulls(person);
                    assertEquals(person.sup(), rootEntityType);

                    Stream<Consumer<Hypergraph.Transaction>> subPersonAssertions = Stream.of(
                            tx2 -> {
                                EntityTypeInt man = tx2.concepts().getEntityType("man");
                                notNulls(man);
                                assertEquals(man.sup(), person);
                            },
                            tx2 -> {
                                EntityTypeInt woman = tx2.concepts().getEntityType("woman");
                                notNulls(woman);
                                assertEquals(woman.sup(), person);
                            }
                    );
                    subPersonAssertions.parallel().forEach(assertions -> assertions.accept(tx));
                },
                tx -> {
                    EntityTypeInt company = tx.concepts().getEntityType("company");
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
