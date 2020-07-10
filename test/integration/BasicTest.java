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

package grakn.core.test.integration;

import grakn.core.Grakn;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.rocks.RocksGrakn;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static grakn.core.test.integration.Util.assertNotNulls;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BasicTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("basic-test");
    private static String database = "basic-test";

    private static void assert_transaction_read(Grakn.Transaction transaction) {
        assertTrue(transaction.isOpen());
        assertEquals(RocksGrakn.Transaction.Type.READ, transaction.type());

        ThingType rootType = transaction.concepts().getRootType();
        EntityType rootEntityType = transaction.concepts().getRootEntityType();
        RelationType rootRelationType = transaction.concepts().getRootRelationType();
        AttributeType rootAttributeType = transaction.concepts().getRootAttributeType();
        Util.assertNotNulls(rootType, rootEntityType, rootRelationType, rootAttributeType);

        Stream<Consumer<Grakn.Transaction>> typeAssertions = Stream.of(
                tx -> {
                    AttributeType.String name = tx.concepts().getAttributeType("name").asString();
                    Util.assertNotNulls(name);
                    assertEquals(rootAttributeType, name.sup());
                },
                tx -> {
                    AttributeType.Long age = tx.concepts().getAttributeType("age").asLong();
                    Util.assertNotNulls(age);
                    assertEquals(rootAttributeType, age.sup());
                },
                tx -> {
                    RelationType marriage = tx.concepts().getRelationType("marriage");
                    RoleType husband = marriage.role("husband");
                    RoleType wife = marriage.role("wife");
                    Util.assertNotNulls(marriage, husband, wife);
                    assertEquals(rootRelationType, marriage.sup());
                    assertEquals(rootRelationType.role("role"), husband.sup());
                    assertEquals(rootRelationType.role("role"), wife.sup());
                },
                tx -> {
                    RelationType employment = tx.concepts().getRelationType("employment");
                    RoleType employee = employment.role("employee");
                    RoleType employer = employment.role("employer");
                    Util.assertNotNulls(employment, employee, employer);
                    assertEquals(rootRelationType, employment.sup());
                },
                tx -> {
                    EntityType person = tx.concepts().getEntityType("person");
                    Util.assertNotNulls(person);
                    assertEquals(rootEntityType, person.sup());

                    Stream<Consumer<Grakn.Transaction>> subPersonAssertions = Stream.of(
                            tx2 -> {
                                EntityType man = tx2.concepts().getEntityType("man");
                                Util.assertNotNulls(man);
                                assertEquals(person, man.sup());
                            },
                            tx2 -> {
                                EntityType woman = tx2.concepts().getEntityType("woman");
                                Util.assertNotNulls(woman);
                                assertEquals(person, woman.sup());
                            }
                    );
                    subPersonAssertions.parallel().forEach(assertions -> assertions.accept(tx));
                },
                tx -> {
                    EntityType company = tx.concepts().getEntityType("company");
                    Util.assertNotNulls(company);
                    assertEquals(rootEntityType, company.sup());
                }
        );

        typeAssertions.parallel().forEach(assertion -> assertion.accept(transaction));
    }

    @Test
    public void write_types_concurrently_repeatedly() throws IOException {
        for (int i = 0; i < 100; i++) {
            System.out.println(i + " ---- ");
            write_types_concurrently();
        }
    }

    @Test
    public void write_types_concurrently() throws IOException {
        Util.resetDirectory(directory);

        try (Grakn graph = RocksGrakn.open(directory.toString())) {
            graph.databases().create("my_data_database");

            assertTrue(graph.isOpen());
            assertEquals(1, graph.databases().getAll().size());
            assertEquals("my_data_database", graph.databases().getAll().iterator().next().name());

            try (Grakn.Session session = graph.session("my_data_database", Grakn.Session.Type.SCHEMA)) {

                assertTrue(session.isOpen());
                assertEquals("my_data_database", session.database().name());

                try (Grakn.Transaction transaction = session.transaction(Grakn.Transaction.Type.READ)) {
                    assertTrue(transaction.isOpen());
                    assertEquals(RocksGrakn.Transaction.Type.READ, transaction.type());

                    Stream<Consumer<Grakn.Transaction>> rootTypeAssertions = Stream.of(
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

                try (Grakn.Transaction transaction = session.transaction(Grakn.Transaction.Type.WRITE)) {
                    assertTrue(transaction.isOpen());
                    assertEquals(RocksGrakn.Transaction.Type.WRITE, transaction.type());

                    ThingType rootType = transaction.concepts().getRootType();
                    EntityType rootEntityType = transaction.concepts().getRootEntityType();
                    RelationType rootRelationType = transaction.concepts().getRootRelationType();
                    AttributeType rootAttributeType = transaction.concepts().getRootAttributeType();
                    Util.assertNotNulls(rootType, rootEntityType, rootRelationType, rootAttributeType);

                    Stream<Consumer<Grakn.Transaction>> typeAssertions = Stream.of(
                            tx -> {
                                AttributeType name = tx.concepts().putAttributeType("name", String.class).asString();
                                Util.assertNotNulls(name);
                                assertEquals(rootAttributeType, name.sup());
                            },
                            tx -> {
                                AttributeType.Long age = tx.concepts().putAttributeType("age", Long.class).asLong();
                                Util.assertNotNulls(age);
                                assertEquals(rootAttributeType, age.sup());
                            },
                            tx -> {
                                RelationType marriage = tx.concepts().putRelationType("marriage");
                                marriage.relates("husband");
                                marriage.relates("wife");
                                Util.assertNotNulls(marriage);
                                assertEquals(rootRelationType, marriage.sup());
                            },
                            tx -> {
                                RelationType employment = tx.concepts().putRelationType("employment");
                                employment.relates("employee");
                                employment.relates("employer");
                                Util.assertNotNulls(employment);
                                assertEquals(rootRelationType, employment.sup());
                            },
                            tx -> {
                                EntityType person = tx.concepts().putEntityType("person");
                                Util.assertNotNulls(person);
                                assertEquals(rootEntityType, person.sup());

                                Stream<Consumer<Grakn.Transaction>> subPersonAssertions = Stream.of(
                                        tx2 -> {
                                            EntityType man = tx2.concepts().putEntityType("man");
                                            man.sup(person);
                                            Util.assertNotNulls(man);
                                            assertEquals(person, man.sup());
                                        },
                                        tx2 -> {
                                            EntityType woman = tx2.concepts().putEntityType("woman");
                                            woman.sup(person);
                                            Util.assertNotNulls(woman);
                                            assertEquals(person, woman.sup());
                                        }
                                );
                                subPersonAssertions.parallel().forEach(assertions -> assertions.accept(tx));
                            },
                            tx -> {
                                EntityType company = tx.concepts().putEntityType("company");
                                Util.assertNotNulls(company);
                                assertEquals(rootEntityType, company.sup());
                            }
                    );

                    typeAssertions.parallel().forEach(assertion -> assertion.accept(transaction));
                    transaction.commit();
                }

                try (Grakn.Transaction transaction = session.transaction(Grakn.Transaction.Type.READ)) {
                    assert_transaction_read(transaction);
                }
            }
        }


        try (Grakn graph = RocksGrakn.open(directory.toString())) {

            assertTrue(graph.isOpen());
            assertEquals(1, graph.databases().getAll().size());
            assertEquals("my_data_database", graph.databases().getAll().iterator().next().name());

            try (Grakn.Session session = graph.session("my_data_database", Grakn.Session.Type.SCHEMA)) {

                assertTrue(session.isOpen());
                assertEquals("my_data_database", session.database().name());

                try (Grakn.Transaction transaction = session.transaction(Grakn.Transaction.Type.READ)) {
                    assert_transaction_read(transaction);
                }

                try (Grakn.Transaction transaction = session.transaction(Grakn.Transaction.Type.WRITE)) {
                    AttributeType.String gender = transaction.concepts().putAttributeType("gender", String.class).asString();
                    EntityType school = transaction.concepts().putEntityType("school");
                    RelationType teaching = transaction.concepts().putRelationType("teaching");
                    teaching.relates("teacher");
                    teaching.relates("student");
                    RoleType teacher = teaching.role("teacher");
                    RoleType student = teaching.role("student");
                    Util.assertNotNulls(gender, school, teaching, teacher, student);
                    transaction.commit();
                }

                try (Grakn.Transaction transaction = session.transaction(Grakn.Transaction.Type.READ)) {
                    assert_transaction_read(transaction);
                    AttributeType.String gender = transaction.concepts().getAttributeType("gender").asString();
                    EntityType school = transaction.concepts().getEntityType("school");
                    RelationType teaching = transaction.concepts().getRelationType("teaching");
                    RoleType teacher = teaching.role("teacher");
                    RoleType student = teaching.role("student");
                    Util.assertNotNulls(gender, school, teaching, teacher, student);
                }
            }
        }
    }

    private void reset_directory_and_create_attribute_types() throws IOException {
        Util.resetDirectory(directory);

        try (Grakn graph = RocksGrakn.open(directory.toString())) {
            graph.databases().create(database);
            try (Grakn.Session session = graph.session(database, Grakn.Session.Type.SCHEMA)) {
                try (Grakn.Transaction txn = session.transaction(Grakn.Transaction.Type.WRITE)) {
                    txn.concepts().putAttributeType("is-alive", Boolean.class);
                    txn.concepts().putAttributeType("age", Long.class);
                    txn.concepts().putAttributeType("score", Double.class);
                    txn.concepts().putAttributeType("name", String.class);
                    txn.concepts().putAttributeType("birth-date", LocalDateTime.class);

                    txn.commit();
                }
            }
        }
    }

    private AttributeType.Boolean isAlive(Grakn.Transaction txn) {
        return txn.concepts().getAttributeType("is-alive").asBoolean();
    }

    private AttributeType.Long age(Grakn.Transaction txn) {
        return txn.concepts().getAttributeType("age").asLong();
    }

    private AttributeType.Double score(Grakn.Transaction txn) {
        return txn.concepts().getAttributeType("score").asDouble();
    }

    private AttributeType.String name(Grakn.Transaction txn) {
        return txn.concepts().getAttributeType("name").asString();
    }

    private AttributeType.DateTime dob(Grakn.Transaction txn) {
        return txn.concepts().getAttributeType("birth-date").asDateTime();
    }

    @Test
    public void write_attributes_successfully_repeatedly() throws IOException {
        for (int i = 0; i < 100; i++) {
            write_attributes_successfully();
        }
    }

    @Test
    public void write_attributes_successfully() throws IOException {
        LocalDateTime date_1991_1_1_0_0 = LocalDateTime.of(1991, 1, 1, 0, 0);
        reset_directory_and_create_attribute_types();

        try (Grakn graph = RocksGrakn.open(directory.toString())) {
            try (Grakn.Session session = graph.session(database)) {
                try (Grakn.Transaction txn = session.transaction(Grakn.Transaction.Type.WRITE)) {
                    isAlive(txn).put(true);
                    age(txn).put(18);
                    score(txn).put(90.5);
                    name(txn).put("alice");
                    dob(txn).put(date_1991_1_1_0_0);

                    assertEquals(1, isAlive(txn).instances().count());
                    assertTrue(isAlive(txn).instances().anyMatch(att -> att.value().equals(true)));

                    assertEquals(1, age(txn).instances().count());
                    assertTrue(age(txn).instances().anyMatch(att -> att.value() == 18));

                    assertEquals(1, score(txn).instances().count());
                    assertTrue(score(txn).instances().anyMatch(att -> att.value() == 90.5));

                    assertEquals(1, name(txn).instances().count());
                    assertTrue(name(txn).instances().anyMatch(att -> att.value().equals("alice")));

                    assertEquals(1, dob(txn).instances().count());
                    assertTrue(dob(txn).instances().anyMatch(att -> att.value().equals(date_1991_1_1_0_0)));

                    txn.commit();
                }

                try (Grakn.Transaction txn = session.transaction(Grakn.Transaction.Type.READ)) {
                    LocalDateTime dateTime = LocalDateTime.of(1991, 1, 1, 0, 0);

                    Attribute.Boolean isAlive = isAlive(txn).get(true);
                    Attribute.Long age = age(txn).get(18);
                    Attribute.Double score = score(txn).get(90.5);
                    Attribute.String name = name(txn).get("alice");
                    Attribute.DateTime dob = dob(txn).get(dateTime);

                    assertNotNulls(isAlive, age, score, name, dob);
                    assertEquals(true, isAlive.value());
                    assertEquals(18, age.value().longValue());
                    assertEquals(90.5, score.value(), 0.001);
                    assertEquals("alice", name.value());
                    assertEquals(dateTime, dob.value());

                    assertEquals(1, isAlive(txn).instances().count());
                    assertTrue(isAlive(txn).instances().anyMatch(att -> att.value().equals(true)));

                    assertEquals(1, age(txn).instances().count());
                    assertTrue(age(txn).instances().anyMatch(att -> att.value() == 18));

                    assertEquals(1, score(txn).instances().count());
                    assertTrue(score(txn).instances().anyMatch(att -> att.value() == 90.5));

                    assertEquals(1, name(txn).instances().count());
                    assertTrue(name(txn).instances().anyMatch(att -> att.value().equals("alice")));

                    assertEquals(1, dob(txn).instances().count());
                    assertTrue(dob(txn).instances().anyMatch(att -> att.value().equals(date_1991_1_1_0_0)));
                }
            }
        }
    }


    @Test
    public void write_different_attributes_in_parallel_successfully_repeatedly() throws IOException {
        for (int i = 0; i < 100; i++) {
            write_different_attributes_in_parallel_successfully();
        }
    }

    @Test
    public void write_different_attributes_in_parallel_successfully() throws IOException {
        LocalDateTime date_1991_2_3_4_5 = LocalDateTime.of(1991, 2, 3, 4, 5);
        LocalDateTime date_1992_3_4_5_6 = LocalDateTime.of(1992, 3, 4, 5, 6);
        LocalDateTime date_1993_4_5_6_7 = LocalDateTime.of(1993, 4, 5, 6, 7);

        reset_directory_and_create_attribute_types();

        try (Grakn graph = RocksGrakn.open(directory.toString())) {
            try (Grakn.Session session = graph.session(database)) {
                Grakn.Transaction txn1 = session.transaction(Grakn.Transaction.Type.WRITE);
                Grakn.Transaction txn2 = session.transaction(Grakn.Transaction.Type.WRITE);
                Grakn.Transaction txn3 = session.transaction(Grakn.Transaction.Type.WRITE);

                isAlive(txn1).put(true);
                isAlive(txn2).put(false);
                age(txn1).put(17);
                age(txn2).put(18);
                age(txn3).put(19);
                score(txn1).put(70.5);
                score(txn2).put(80.6);
                score(txn3).put(90.7);
                name(txn1).put("alice");
                name(txn2).put("bob");
                name(txn3).put("charlie");
                dob(txn1).put(date_1991_2_3_4_5);
                dob(txn2).put(date_1992_3_4_5_6);
                dob(txn3).put(date_1993_4_5_6_7);

                assertEquals(1, isAlive(txn1).instances().count());
                assertTrue(isAlive(txn1).instances().anyMatch(att -> att.value().equals(true)));
                assertEquals(1, isAlive(txn2).instances().count());
                assertTrue(isAlive(txn2).instances().anyMatch(att -> att.value().equals(false)));

                assertEquals(1, age(txn1).instances().count());
                assertTrue(age(txn1).instances().anyMatch(att -> att.value() == 17));
                assertEquals(1, age(txn2).instances().count());
                assertTrue(age(txn2).instances().anyMatch(att -> att.value() == 18));
                assertEquals(1, age(txn3).instances().count());
                assertTrue(age(txn3).instances().anyMatch(att -> att.value() == 19));

                assertEquals(1, score(txn1).instances().count());
                assertTrue(score(txn1).instances().anyMatch(att -> att.value() == 70.5));
                assertEquals(1, score(txn2).instances().count());
                assertTrue(score(txn2).instances().anyMatch(att -> att.value() == 80.6));
                assertEquals(1, score(txn3).instances().count());
                assertTrue(score(txn3).instances().anyMatch(att -> att.value() == 90.7));

                assertEquals(1, name(txn1).instances().count());
                assertTrue(name(txn1).instances().anyMatch(att -> att.value().equals("alice")));
                assertEquals(1, name(txn2).instances().count());
                assertTrue(name(txn2).instances().anyMatch(att -> att.value().equals("bob")));
                assertEquals(1, name(txn3).instances().count());
                assertTrue(name(txn3).instances().anyMatch(att -> att.value().equals("charlie")));

                assertEquals(1, dob(txn1).instances().count());
                assertTrue(dob(txn1).instances().anyMatch(att -> att.value().equals(date_1991_2_3_4_5)));
                assertEquals(1, dob(txn2).instances().count());
                assertTrue(dob(txn2).instances().anyMatch(att -> att.value().equals(date_1992_3_4_5_6)));
                assertEquals(1, dob(txn3).instances().count());
                assertTrue(dob(txn3).instances().anyMatch(att -> att.value().equals(date_1993_4_5_6_7)));

                txn1.commit();
                txn2.commit();
                txn3.commit();

                try (Grakn.Transaction txn = session.transaction(Grakn.Transaction.Type.READ)) {
                    LocalDateTime d1 = LocalDateTime.of(1991, 2, 3, 4, 5);
                    LocalDateTime d2 = LocalDateTime.of(1992, 3, 4, 5, 6);
                    LocalDateTime d3 = LocalDateTime.of(1993, 4, 5, 6, 7);

                    assertEquals(true, isAlive(txn).get(true).value());
                    assertEquals(false, isAlive(txn).get(false).value());
                    assertEquals(17, age(txn).get(17).value().longValue());
                    assertEquals(18, age(txn).get(18).value().longValue());
                    assertEquals(19, age(txn).get(19).value().longValue());
                    assertEquals(70.5, score(txn).get(70.5).value(), 0.001);
                    assertEquals(80.6, score(txn).get(80.6).value(), 0.001);
                    assertEquals(90.7, score(txn).get(90.7).value(), 0.001);
                    assertEquals("alice", name(txn).get("alice").value());
                    assertEquals("bob", name(txn).get("bob").value());
                    assertEquals("charlie", name(txn).get("charlie").value());
                    assertEquals(d1, dob(txn).get(d1).value());
                    assertEquals(d2, dob(txn).get(d2).value());
                    assertEquals(d3, dob(txn).get(d3).value());

                    assertEquals(2, isAlive(txn).instances().count());
                    assertTrue(isAlive(txn).instances().anyMatch(att -> att.value().equals(true)));
                    assertTrue(isAlive(txn).instances().anyMatch(att -> att.value().equals(false)));

                    assertEquals(3, age(txn).instances().count());
                    assertTrue(age(txn).instances().anyMatch(att -> att.value() == 17));
                    assertTrue(age(txn).instances().anyMatch(att -> att.value() == 18));
                    assertTrue(age(txn).instances().anyMatch(att -> att.value() == 19));

                    assertEquals(3, score(txn).instances().count());
                    assertTrue(score(txn).instances().anyMatch(att -> att.value() == 70.5));
                    assertTrue(score(txn).instances().anyMatch(att -> att.value() == 80.6));
                    assertTrue(score(txn).instances().anyMatch(att -> att.value() == 90.7));

                    assertEquals(3, name(txn).instances().count());
                    assertTrue(name(txn).instances().anyMatch(att -> att.value().equals("alice")));
                    assertTrue(name(txn).instances().anyMatch(att -> att.value().equals("bob")));
                    assertTrue(name(txn).instances().anyMatch(att -> att.value().equals("charlie")));

                    assertEquals(3, dob(txn).instances().count());
                    assertTrue(dob(txn).instances().anyMatch(att -> att.value().equals(date_1991_2_3_4_5)));
                    assertTrue(dob(txn).instances().anyMatch(att -> att.value().equals(date_1992_3_4_5_6)));
                    assertTrue(dob(txn).instances().anyMatch(att -> att.value().equals(date_1993_4_5_6_7)));
                }
            }
        }
    }

    @Test
    public void write_identical_attributes_in_parallel_successfully_repeatedly() throws IOException {
        for (int i = 0; i < 100; i++) {
            write_identical_attributes_in_parallel_successfully();
        }
    }

    @Test
    public void write_identical_attributes_in_parallel_successfully() throws IOException {
        reset_directory_and_create_attribute_types();

        LocalDateTime date_1992_2_3_4_5 = LocalDateTime.of(1991, 2, 3, 4, 5);

        try (Grakn graph = RocksGrakn.open(directory.toString())) {
            try (Grakn.Session session = graph.session(database)) {
                Grakn.Transaction txn1 = session.transaction(Grakn.Transaction.Type.WRITE);
                Grakn.Transaction txn2 = session.transaction(Grakn.Transaction.Type.WRITE);
                Grakn.Transaction txn3 = session.transaction(Grakn.Transaction.Type.WRITE);

                isAlive(txn1).put(true);
                isAlive(txn2).put(true);
                isAlive(txn3).put(true);
                age(txn1).put(17);
                age(txn2).put(17);
                age(txn3).put(17);
                score(txn1).put(70.5);
                score(txn2).put(70.5);
                score(txn3).put(70.5);
                name(txn1).put("alice");
                name(txn2).put("alice");
                name(txn3).put("alice");
                dob(txn1).put(date_1992_2_3_4_5);
                dob(txn2).put(date_1992_2_3_4_5);
                dob(txn3).put(date_1992_2_3_4_5);

                assertEquals(1, isAlive(txn1).instances().count());
                assertTrue(isAlive(txn1).instances().anyMatch(att -> att.value().equals(true)));
                assertEquals(1, isAlive(txn2).instances().count());
                assertTrue(isAlive(txn2).instances().anyMatch(att -> att.value().equals(true)));
                assertEquals(1, isAlive(txn3).instances().count());
                assertTrue(isAlive(txn2).instances().anyMatch(att -> att.value().equals(true)));

                assertEquals(1, age(txn1).instances().count());
                assertTrue(age(txn1).instances().anyMatch(att -> att.value() == 17));
                assertEquals(1, age(txn2).instances().count());
                assertTrue(age(txn2).instances().anyMatch(att -> att.value() == 17));
                assertEquals(1, age(txn3).instances().count());
                assertTrue(age(txn3).instances().anyMatch(att -> att.value() == 17));

                assertEquals(1, score(txn1).instances().count());
                assertTrue(score(txn1).instances().anyMatch(att -> att.value() == 70.5));
                assertEquals(1, score(txn2).instances().count());
                assertTrue(score(txn2).instances().anyMatch(att -> att.value() == 70.5));
                assertEquals(1, score(txn3).instances().count());
                assertTrue(score(txn3).instances().anyMatch(att -> att.value() == 70.5));

                assertEquals(1, name(txn1).instances().count());
                assertTrue(name(txn1).instances().anyMatch(att -> att.value().equals("alice")));
                assertEquals(1, name(txn2).instances().count());
                assertTrue(name(txn2).instances().anyMatch(att -> att.value().equals("alice")));
                assertEquals(1, name(txn3).instances().count());
                assertTrue(name(txn3).instances().anyMatch(att -> att.value().equals("alice")));

                assertEquals(1, dob(txn1).instances().count());
                assertTrue(dob(txn1).instances().anyMatch(att -> att.value().equals(date_1992_2_3_4_5)));
                assertEquals(1, dob(txn2).instances().count());
                assertTrue(dob(txn2).instances().anyMatch(att -> att.value().equals(date_1992_2_3_4_5)));
                assertEquals(1, dob(txn3).instances().count());
                assertTrue(dob(txn3).instances().anyMatch(att -> att.value().equals(date_1992_2_3_4_5)));

                txn1.commit();
                txn2.commit();
                txn3.commit();

                try (Grakn.Transaction txn = session.transaction(Grakn.Transaction.Type.READ)) {

                    assertEquals(true, isAlive(txn).get(true).value());
                    assertEquals(17, age(txn).get(17).value().longValue());
                    assertEquals(70.5, score(txn).get(70.5).value(), 0.001);
                    assertEquals("alice", name(txn).get("alice").value());
                    assertEquals(date_1992_2_3_4_5, dob(txn).get(date_1992_2_3_4_5).value());

                    assertEquals(1, isAlive(txn).instances().count());
                    assertTrue(isAlive(txn).instances().anyMatch(att -> att.value().equals(true)));

                    assertEquals(1, age(txn).instances().count());
                    assertTrue(age(txn).instances().anyMatch(att -> att.value() == 17));

                    assertEquals(1, score(txn).instances().count());
                    assertTrue(score(txn).instances().anyMatch(att -> att.value() == 70.5));

                    assertEquals(1, name(txn).instances().count());
                    assertTrue(name(txn).instances().anyMatch(att -> att.value().equals("alice")));

                    assertEquals(1, dob(txn).instances().count());
                    assertTrue(dob(txn).instances().anyMatch(att -> att.value().equals(date_1992_2_3_4_5)));
                }
            }
        }
    }

    @Test
    public void write_and_delete_attributes_concurrently_repeatedly() throws IOException {
        for (int i = 0; i < 100; i++) {
            write_and_delete_attributes_concurrently();
        }
    }

    @Test
    public void write_and_delete_attributes_concurrently() throws IOException {
        reset_directory_and_create_attribute_types();

        try (Grakn graph = RocksGrakn.open(directory.toString())) {
            try (Grakn.Session session = graph.session(database)) {
                Grakn.Transaction txn1 = session.transaction(Grakn.Transaction.Type.WRITE);
                Grakn.Transaction txn2 = session.transaction(Grakn.Transaction.Type.WRITE);

                name(txn1).put("alice");
                name(txn2).put("alice");

                txn1.commit();
                txn2.commit();

                try (Grakn.Transaction txn = session.transaction(Grakn.Transaction.Type.READ)) {
                    assertEquals("alice", name(txn).get("alice").value());
                    assertEquals(1, name(txn).instances().count());
                    assertTrue(name(txn).instances().anyMatch(att -> att.value().equals("alice")));
                }

                txn1 = session.transaction(Grakn.Transaction.Type.WRITE);
                txn2 = session.transaction(Grakn.Transaction.Type.WRITE);

                name(txn1).put("alice");
                name(txn2).get("alice").delete();

                assertEquals(0, name(txn2).instances().count());
                assertEquals(1, name(txn1).instances().count());
                assertTrue(name(txn1).instances().anyMatch(att -> att.value().equals("alice")));

                txn1.commit(); // write before delete
                try {
                    txn2.commit();
                    fail("The delete operation in TX2 is not supposed to succeed");
                } catch (Exception ignored) {
                    assertTrue(true);
                }

                try (Grakn.Transaction txn = session.transaction(Grakn.Transaction.Type.READ)) {
                    assertEquals("alice", name(txn).get("alice").value());
                    assertEquals(1, name(txn).instances().count());
                    assertTrue(name(txn).instances().anyMatch(att -> att.value().equals("alice")));
                }

                txn1 = session.transaction(Grakn.Transaction.Type.WRITE);
                txn2 = session.transaction(Grakn.Transaction.Type.WRITE);

                name(txn1).put("alice");
                name(txn2).get("alice").delete();

                txn2.commit(); // delete before write
                txn1.commit();

                try (Grakn.Transaction txn = session.transaction(Grakn.Transaction.Type.READ)) {
                    assertEquals("alice", name(txn).get("alice").value());
                    assertEquals(1, name(txn).instances().count());
                    assertTrue(name(txn).instances().anyMatch(att -> att.value().equals("alice")));
                }

                txn1 = session.transaction(Grakn.Transaction.Type.WRITE);
                txn2 = session.transaction(Grakn.Transaction.Type.WRITE);

                name(txn1).get("alice").delete();
                name(txn2).get("alice").delete();

                txn1.commit(); // delete concurrently
                try {
                    txn2.commit();
                    fail("The second delete operation in TX2 is not supposed to succeed");
                } catch (Exception ignored) {
                    assertTrue(true);
                }

                try (Grakn.Transaction txn = session.transaction(Grakn.Transaction.Type.READ)) {
                    assertNull(name(txn).get("alice"));
                    assertEquals(0, name(txn).instances().count());
                }
            }
        }
    }
}
