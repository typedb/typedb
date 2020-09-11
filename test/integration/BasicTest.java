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
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.Concepts;
import grakn.core.concept.data.Attribute;
import grakn.core.concept.schema.AttributeType;
import grakn.core.concept.schema.EntityType;
import grakn.core.concept.schema.RelationType;
import grakn.core.concept.schema.RoleType;
import grakn.core.concept.schema.Rule;
import grakn.core.concept.schema.ThingType;
import grakn.core.concept.schema.Type;
import grakn.core.rocks.RocksGrakn;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.set;
import static grakn.core.concept.schema.AttributeType.ValueType.BOOLEAN;
import static grakn.core.concept.schema.AttributeType.ValueType.DATETIME;
import static grakn.core.concept.schema.AttributeType.ValueType.DOUBLE;
import static grakn.core.concept.schema.AttributeType.ValueType.LONG;
import static grakn.core.concept.schema.AttributeType.ValueType.STRING;
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
        assertTrue(transaction.type().isRead());

        ThingType rootType = transaction.concepts().getRootThingType();
        EntityType rootEntityType = transaction.concepts().getRootEntityType();
        RelationType rootRelationType = transaction.concepts().getRootRelationType();
        AttributeType rootAttributeType = transaction.concepts().getRootAttributeType();
        Util.assertNotNulls(rootType, rootEntityType, rootRelationType, rootAttributeType);

        Stream<Consumer<Grakn.Transaction>> typeAssertions = Stream.of(
                tx -> {
                    AttributeType.String name = tx.concepts().getAttributeType("name").asString();
                    Util.assertNotNulls(name);
                    assertEquals(rootAttributeType, name.getSupertype());
                },
                tx -> {
                    AttributeType.Long age = tx.concepts().getAttributeType("age").asLong();
                    Util.assertNotNulls(age);
                    assertEquals(rootAttributeType, age.getSupertype());
                },
                tx -> {
                    RelationType marriage = tx.concepts().getRelationType("marriage");
                    RoleType husband = marriage.getRelates("husband");
                    RoleType wife = marriage.getRelates("wife");
                    Util.assertNotNulls(marriage, husband, wife);
                    assertEquals(rootRelationType, marriage.getSupertype());
                    assertEquals(rootRelationType.getRelates("role"), husband.getSupertype());
                    assertEquals(rootRelationType.getRelates("role"), wife.getSupertype());
                },
                tx -> {
                    RelationType employment = tx.concepts().getRelationType("employment");
                    RoleType employee = employment.getRelates("employee");
                    RoleType employer = employment.getRelates("employer");
                    Util.assertNotNulls(employment, employee, employer);
                    assertEquals(rootRelationType, employment.getSupertype());
                },
                tx -> {
                    EntityType person = tx.concepts().getEntityType("person");
                    Util.assertNotNulls(person);
                    assertEquals(rootEntityType, person.getSupertype());

                    Stream<Consumer<Grakn.Transaction>> subPersonAssertions = Stream.of(
                            tx2 -> {
                                EntityType man = tx2.concepts().getEntityType("man");
                                Util.assertNotNulls(man);
                                assertEquals(person, man.getSupertype());
                            },
                            tx2 -> {
                                EntityType woman = tx2.concepts().getEntityType("woman");
                                Util.assertNotNulls(woman);
                                assertEquals(person, woman.getSupertype());
                            }
                    );
                    subPersonAssertions.parallel().forEach(assertions -> assertions.accept(tx));
                },
                tx -> {
                    EntityType company = tx.concepts().getEntityType("company");
                    Util.assertNotNulls(company);
                    assertEquals(rootEntityType, company.getSupertype());
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

        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);

            assertTrue(grakn.isOpen());
            assertEquals(1, grakn.databases().all().size());
            assertEquals(database, grakn.databases().all().iterator().next().name());

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {

                assertTrue(session.isOpen());
                assertEquals(database, session.database().name());

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {
                    assertTrue(transaction.isOpen());
                    assertTrue(transaction.type().isRead());

                    Stream<Consumer<Grakn.Transaction>> rootTypeAssertions = Stream.of(
                            tx -> {
                                ThingType rootType = tx.concepts().getRootThingType();
                                assertNotNull(rootType);
                            },
                            tx -> {
                                EntityType rootEntityType = tx.concepts().getRootEntityType();
                                assertNotNull(rootEntityType);
                                assertNull(rootEntityType.getSupertype());
                            },
                            tx -> {
                                RelationType rootRelationType = tx.concepts().getRootRelationType();
                                assertNotNull(rootRelationType);
                                assertNull(rootRelationType.getSupertype());
                            },
                            tx -> {
                                AttributeType rootAttributeType = tx.concepts().getRootAttributeType();
                                assertNotNull(rootAttributeType);
                                assertNull(rootAttributeType.getSupertype());
                            }
                    );

                    rootTypeAssertions.parallel().forEach(assertion -> assertion.accept(transaction));
                }

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    assertTrue(transaction.isOpen());
                    assertTrue(transaction.type().isWrite());

                    ThingType rootType = transaction.concepts().getRootThingType();
                    EntityType rootEntityType = transaction.concepts().getRootEntityType();
                    RelationType rootRelationType = transaction.concepts().getRootRelationType();
                    AttributeType rootAttributeType = transaction.concepts().getRootAttributeType();
                    Util.assertNotNulls(rootType, rootEntityType, rootRelationType, rootAttributeType);

                    Stream<Consumer<Grakn.Transaction>> typeAssertions = Stream.of(
                            tx -> {
                                AttributeType name = tx.concepts().putAttributeType("name", STRING).asString();
                                Util.assertNotNulls(name);
                                assertEquals(rootAttributeType, name.getSupertype());
                            },
                            tx -> {
                                AttributeType.Long age = tx.concepts().putAttributeType("age", LONG).asLong();
                                Util.assertNotNulls(age);
                                assertEquals(rootAttributeType, age.getSupertype());
                            },
                            tx -> {
                                RelationType marriage = tx.concepts().putRelationType("marriage");
                                marriage.setRelates("husband");
                                marriage.setRelates("wife");
                                Util.assertNotNulls(marriage);
                                assertEquals(rootRelationType, marriage.getSupertype());
                            },
                            tx -> {
                                RelationType employment = tx.concepts().putRelationType("employment");
                                employment.setRelates("employee");
                                employment.setRelates("employer");
                                Util.assertNotNulls(employment);
                                assertEquals(rootRelationType, employment.getSupertype());
                            },
                            tx -> {
                                EntityType person = tx.concepts().putEntityType("person");
                                Util.assertNotNulls(person);
                                assertEquals(rootEntityType, person.getSupertype());

                                Stream<Consumer<Grakn.Transaction>> subPersonAssertions = Stream.of(
                                        tx2 -> {
                                            EntityType man = tx2.concepts().putEntityType("man");
                                            man.setSupertype(person);
                                            Util.assertNotNulls(man);
                                            assertEquals(person, man.getSupertype());
                                        },
                                        tx2 -> {
                                            EntityType woman = tx2.concepts().putEntityType("woman");
                                            woman.setSupertype(person);
                                            Util.assertNotNulls(woman);
                                            assertEquals(person, woman.getSupertype());
                                        }
                                );
                                subPersonAssertions.parallel().forEach(assertions -> assertions.accept(tx));
                            },
                            tx -> {
                                EntityType company = tx.concepts().putEntityType("company");
                                Util.assertNotNulls(company);
                                assertEquals(rootEntityType, company.getSupertype());
                            }
                    );

                    typeAssertions.parallel().forEach(assertion -> assertion.accept(transaction));
                    transaction.commit();
                }

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {
                    assert_transaction_read(transaction);
                }
            }
        }


        try (Grakn grakn = RocksGrakn.open(directory)) {

            assertTrue(grakn.isOpen());
            assertEquals(1, grakn.databases().all().size());
            assertEquals(database, grakn.databases().all().iterator().next().name());

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {

                assertTrue(session.isOpen());
                assertEquals(database, session.database().name());

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {
                    assert_transaction_read(transaction);
                }

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    AttributeType.String gender = transaction.concepts().putAttributeType("gender", STRING).asString();
                    EntityType school = transaction.concepts().putEntityType("school");
                    RelationType teaching = transaction.concepts().putRelationType("teaching");
                    teaching.setRelates("teacher");
                    teaching.setRelates("student");
                    RoleType teacher = teaching.getRelates("teacher");
                    RoleType student = teaching.getRelates("student");
                    Util.assertNotNulls(gender, school, teaching, teacher, student);
                    transaction.commit();
                }

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {
                    assert_transaction_read(transaction);
                    AttributeType.String gender = transaction.concepts().getAttributeType("gender").asString();
                    EntityType school = transaction.concepts().getEntityType("school");
                    RelationType teaching = transaction.concepts().getRelationType("teaching");
                    RoleType teacher = teaching.getRelates("teacher");
                    RoleType student = teaching.getRelates("student");
                    Util.assertNotNulls(gender, school, teaching, teacher, student);
                }
            }
        }
    }

    private void reset_directory_and_create_attribute_types() throws IOException {
        Util.resetDirectory(directory);

        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    txn.concepts().putAttributeType("is-alive", BOOLEAN);
                    txn.concepts().putAttributeType("age", LONG);
                    txn.concepts().putAttributeType("score", DOUBLE);
                    txn.concepts().putAttributeType("name", STRING);
                    txn.concepts().putAttributeType("birth-date", DATETIME);

                    txn.commit();
                }
            }
        }
    }

    // TODO: re-enable when writing rule edges is implemented
    @Ignore
    @Test
    public void write_and_retrieve_rules() throws IOException {
        Util.resetDirectory(directory);

        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    Concepts concepts = txn.concepts();

                    EntityType person = concepts.putEntityType("person");
                    AttributeType.String name = concepts.putAttributeType("name", STRING).asString();
                    RelationType friendship = concepts.putRelationType("friendship");
                    friendship.setRelates("friend");
                    RoleType friend = friendship.getRelates("friend");
                    concepts.putRule(
                            "friendless-have-names",
                            Graql.parsePattern("{$x isa person; not { (friend: $x) isa friendship; }; }"),
                            Graql.parsePattern("{$x has name \"i have no friends\";}"));
                    txn.commit();
                }
                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    Concepts concepts = txn.concepts();
                    EntityType person = concepts.getEntityType("person");
                    AttributeType.String name = concepts.getAttributeType("name").asString();
                    RelationType friendship = concepts.getRelationType("friendship");
                    RoleType friend = friendship.getRelates("friend");

                    Rule rule = concepts.getRule("friendless-have-names");
                    Pattern when = rule.getWhen();
                    Pattern then = rule.getThen();
                    assertEquals(Graql.parsePattern("{$x isa person; not { (friend: $x) isa friendship; }; }"), when);
                    assertEquals(Graql.parsePattern("{$x has name \"i have no friends\";}"), then);

                    // assert that the rule contains the correct types
                    assertEquals(person, rule.positiveConditionTypes().findFirst().get());
                    Set<Type> negated = set(friend, friendship);
                    assertEquals(negated, rule.negativeConditionTypes().collect(Collectors.toSet()));
                    assertEquals(name, rule.conclusionTypes().findFirst().get());

                    // assert the dependencies are correct
                    assertEquals(rule, person.getPositiveConditionRules().findFirst().get());
                    assertEquals(0, friendship.getPositiveConditionRules().count());
                    assertEquals(0, friend.getPositiveConditionRules().count());
                    assertEquals(0, name.getPositiveConditionRules().count());

                    assertEquals(0, person.getNegativeConditionRules().count());
                    assertEquals(rule, friendship.getNegativeConditionRules().findFirst().get());
                    assertEquals(rule, friend.getNegativeConditionRules().findFirst().get());
                    assertEquals(0, name.getNegativeConditionRules().count());

                    assertEquals(0, person.getConcludingRules().count());
                    assertEquals(0, friendship.getConcludingRules().count());
                    assertEquals(0, friend.getConcludingRules().count());
                    assertEquals(rule, name.getConcludingRules().findFirst().get());
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

        try (Grakn grakn = RocksGrakn.open(directory)) {
            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    isAlive(txn).put(true);
                    age(txn).put(18);
                    score(txn).put(90.5);
                    name(txn).put("alice");
                    dob(txn).put(date_1991_1_1_0_0);

                    assertEquals(1, isAlive(txn).getInstances().count());
                    assertTrue(isAlive(txn).getInstances().anyMatch(att -> att.getValue().equals(true)));

                    assertEquals(1, age(txn).getInstances().count());
                    assertTrue(age(txn).getInstances().anyMatch(att -> att.getValue() == 18));

                    assertEquals(1, score(txn).getInstances().count());
                    assertTrue(score(txn).getInstances().anyMatch(att -> att.getValue() == 90.5));

                    assertEquals(1, name(txn).getInstances().count());
                    assertTrue(name(txn).getInstances().anyMatch(att -> att.getValue().equals("alice")));

                    assertEquals(1, dob(txn).getInstances().count());
                    assertTrue(dob(txn).getInstances().anyMatch(att -> att.getValue().equals(date_1991_1_1_0_0)));

                    txn.commit();
                }

                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    LocalDateTime dateTime = LocalDateTime.of(1991, 1, 1, 0, 0);

                    Attribute.Boolean isAlive = isAlive(txn).get(true);
                    Attribute.Long age = age(txn).get(18);
                    Attribute.Double score = score(txn).get(90.5);
                    Attribute.String name = name(txn).get("alice");
                    Attribute.DateTime dob = dob(txn).get(dateTime);

                    assertNotNulls(isAlive, age, score, name, dob);
                    assertEquals(true, isAlive.getValue());
                    assertEquals(18, age.getValue().longValue());
                    assertEquals(90.5, score.getValue(), 0.001);
                    assertEquals("alice", name.getValue());
                    assertEquals(dateTime, dob.getValue());

                    assertEquals(1, isAlive(txn).getInstances().count());
                    assertTrue(isAlive(txn).getInstances().anyMatch(att -> att.getValue().equals(true)));

                    assertEquals(1, age(txn).getInstances().count());
                    assertTrue(age(txn).getInstances().anyMatch(att -> att.getValue() == 18));

                    assertEquals(1, score(txn).getInstances().count());
                    assertTrue(score(txn).getInstances().anyMatch(att -> att.getValue() == 90.5));

                    assertEquals(1, name(txn).getInstances().count());
                    assertTrue(name(txn).getInstances().anyMatch(att -> att.getValue().equals("alice")));

                    assertEquals(1, dob(txn).getInstances().count());
                    assertTrue(dob(txn).getInstances().anyMatch(att -> att.getValue().equals(date_1991_1_1_0_0)));
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

        try (Grakn grakn = RocksGrakn.open(directory)) {
            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.DATA)) {
                Grakn.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
                Grakn.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
                Grakn.Transaction txn3 = session.transaction(Arguments.Transaction.Type.WRITE);

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

                assertEquals(1, isAlive(txn1).getInstances().count());
                assertTrue(isAlive(txn1).getInstances().anyMatch(att -> att.getValue().equals(true)));
                assertEquals(1, isAlive(txn2).getInstances().count());
                assertTrue(isAlive(txn2).getInstances().anyMatch(att -> att.getValue().equals(false)));

                assertEquals(1, age(txn1).getInstances().count());
                assertTrue(age(txn1).getInstances().anyMatch(att -> att.getValue() == 17));
                assertEquals(1, age(txn2).getInstances().count());
                assertTrue(age(txn2).getInstances().anyMatch(att -> att.getValue() == 18));
                assertEquals(1, age(txn3).getInstances().count());
                assertTrue(age(txn3).getInstances().anyMatch(att -> att.getValue() == 19));

                assertEquals(1, score(txn1).getInstances().count());
                assertTrue(score(txn1).getInstances().anyMatch(att -> att.getValue() == 70.5));
                assertEquals(1, score(txn2).getInstances().count());
                assertTrue(score(txn2).getInstances().anyMatch(att -> att.getValue() == 80.6));
                assertEquals(1, score(txn3).getInstances().count());
                assertTrue(score(txn3).getInstances().anyMatch(att -> att.getValue() == 90.7));

                assertEquals(1, name(txn1).getInstances().count());
                assertTrue(name(txn1).getInstances().anyMatch(att -> att.getValue().equals("alice")));
                assertEquals(1, name(txn2).getInstances().count());
                assertTrue(name(txn2).getInstances().anyMatch(att -> att.getValue().equals("bob")));
                assertEquals(1, name(txn3).getInstances().count());
                assertTrue(name(txn3).getInstances().anyMatch(att -> att.getValue().equals("charlie")));

                assertEquals(1, dob(txn1).getInstances().count());
                assertTrue(dob(txn1).getInstances().anyMatch(att -> att.getValue().equals(date_1991_2_3_4_5)));
                assertEquals(1, dob(txn2).getInstances().count());
                assertTrue(dob(txn2).getInstances().anyMatch(att -> att.getValue().equals(date_1992_3_4_5_6)));
                assertEquals(1, dob(txn3).getInstances().count());
                assertTrue(dob(txn3).getInstances().anyMatch(att -> att.getValue().equals(date_1993_4_5_6_7)));

                txn1.commit();
                txn2.commit();
                txn3.commit();

                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    LocalDateTime d1 = LocalDateTime.of(1991, 2, 3, 4, 5);
                    LocalDateTime d2 = LocalDateTime.of(1992, 3, 4, 5, 6);
                    LocalDateTime d3 = LocalDateTime.of(1993, 4, 5, 6, 7);

                    assertEquals(true, isAlive(txn).get(true).getValue());
                    assertEquals(false, isAlive(txn).get(false).getValue());
                    assertEquals(17, age(txn).get(17).getValue().longValue());
                    assertEquals(18, age(txn).get(18).getValue().longValue());
                    assertEquals(19, age(txn).get(19).getValue().longValue());
                    assertEquals(70.5, score(txn).get(70.5).getValue(), 0.001);
                    assertEquals(80.6, score(txn).get(80.6).getValue(), 0.001);
                    assertEquals(90.7, score(txn).get(90.7).getValue(), 0.001);
                    assertEquals("alice", name(txn).get("alice").getValue());
                    assertEquals("bob", name(txn).get("bob").getValue());
                    assertEquals("charlie", name(txn).get("charlie").getValue());
                    assertEquals(d1, dob(txn).get(d1).getValue());
                    assertEquals(d2, dob(txn).get(d2).getValue());
                    assertEquals(d3, dob(txn).get(d3).getValue());

                    assertEquals(2, isAlive(txn).getInstances().count());
                    assertTrue(isAlive(txn).getInstances().anyMatch(att -> att.getValue().equals(true)));
                    assertTrue(isAlive(txn).getInstances().anyMatch(att -> att.getValue().equals(false)));

                    assertEquals(3, age(txn).getInstances().count());
                    assertTrue(age(txn).getInstances().anyMatch(att -> att.getValue() == 17));
                    assertTrue(age(txn).getInstances().anyMatch(att -> att.getValue() == 18));
                    assertTrue(age(txn).getInstances().anyMatch(att -> att.getValue() == 19));

                    assertEquals(3, score(txn).getInstances().count());
                    assertTrue(score(txn).getInstances().anyMatch(att -> att.getValue() == 70.5));
                    assertTrue(score(txn).getInstances().anyMatch(att -> att.getValue() == 80.6));
                    assertTrue(score(txn).getInstances().anyMatch(att -> att.getValue() == 90.7));

                    assertEquals(3, name(txn).getInstances().count());
                    assertTrue(name(txn).getInstances().anyMatch(att -> att.getValue().equals("alice")));
                    assertTrue(name(txn).getInstances().anyMatch(att -> att.getValue().equals("bob")));
                    assertTrue(name(txn).getInstances().anyMatch(att -> att.getValue().equals("charlie")));

                    assertEquals(3, dob(txn).getInstances().count());
                    assertTrue(dob(txn).getInstances().anyMatch(att -> att.getValue().equals(date_1991_2_3_4_5)));
                    assertTrue(dob(txn).getInstances().anyMatch(att -> att.getValue().equals(date_1992_3_4_5_6)));
                    assertTrue(dob(txn).getInstances().anyMatch(att -> att.getValue().equals(date_1993_4_5_6_7)));
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

        try (Grakn grakn = RocksGrakn.open(directory)) {
            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.DATA)) {
                Grakn.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
                Grakn.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);
                Grakn.Transaction txn3 = session.transaction(Arguments.Transaction.Type.WRITE);

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

                assertEquals(1, isAlive(txn1).getInstances().count());
                assertTrue(isAlive(txn1).getInstances().anyMatch(att -> att.getValue().equals(true)));
                assertEquals(1, isAlive(txn2).getInstances().count());
                assertTrue(isAlive(txn2).getInstances().anyMatch(att -> att.getValue().equals(true)));
                assertEquals(1, isAlive(txn3).getInstances().count());
                assertTrue(isAlive(txn2).getInstances().anyMatch(att -> att.getValue().equals(true)));

                assertEquals(1, age(txn1).getInstances().count());
                assertTrue(age(txn1).getInstances().anyMatch(att -> att.getValue() == 17));
                assertEquals(1, age(txn2).getInstances().count());
                assertTrue(age(txn2).getInstances().anyMatch(att -> att.getValue() == 17));
                assertEquals(1, age(txn3).getInstances().count());
                assertTrue(age(txn3).getInstances().anyMatch(att -> att.getValue() == 17));

                assertEquals(1, score(txn1).getInstances().count());
                assertTrue(score(txn1).getInstances().anyMatch(att -> att.getValue() == 70.5));
                assertEquals(1, score(txn2).getInstances().count());
                assertTrue(score(txn2).getInstances().anyMatch(att -> att.getValue() == 70.5));
                assertEquals(1, score(txn3).getInstances().count());
                assertTrue(score(txn3).getInstances().anyMatch(att -> att.getValue() == 70.5));

                assertEquals(1, name(txn1).getInstances().count());
                assertTrue(name(txn1).getInstances().anyMatch(att -> att.getValue().equals("alice")));
                assertEquals(1, name(txn2).getInstances().count());
                assertTrue(name(txn2).getInstances().anyMatch(att -> att.getValue().equals("alice")));
                assertEquals(1, name(txn3).getInstances().count());
                assertTrue(name(txn3).getInstances().anyMatch(att -> att.getValue().equals("alice")));

                assertEquals(1, dob(txn1).getInstances().count());
                assertTrue(dob(txn1).getInstances().anyMatch(att -> att.getValue().equals(date_1992_2_3_4_5)));
                assertEquals(1, dob(txn2).getInstances().count());
                assertTrue(dob(txn2).getInstances().anyMatch(att -> att.getValue().equals(date_1992_2_3_4_5)));
                assertEquals(1, dob(txn3).getInstances().count());
                assertTrue(dob(txn3).getInstances().anyMatch(att -> att.getValue().equals(date_1992_2_3_4_5)));

                txn1.commit();
                txn2.commit();
                txn3.commit();

                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.READ)) {

                    assertEquals(true, isAlive(txn).get(true).getValue());
                    assertEquals(17, age(txn).get(17).getValue().longValue());
                    assertEquals(70.5, score(txn).get(70.5).getValue(), 0.001);
                    assertEquals("alice", name(txn).get("alice").getValue());
                    assertEquals(date_1992_2_3_4_5, dob(txn).get(date_1992_2_3_4_5).getValue());

                    assertEquals(1, isAlive(txn).getInstances().count());
                    assertTrue(isAlive(txn).getInstances().anyMatch(att -> att.getValue().equals(true)));

                    assertEquals(1, age(txn).getInstances().count());
                    assertTrue(age(txn).getInstances().anyMatch(att -> att.getValue() == 17));

                    assertEquals(1, score(txn).getInstances().count());
                    assertTrue(score(txn).getInstances().anyMatch(att -> att.getValue() == 70.5));

                    assertEquals(1, name(txn).getInstances().count());
                    assertTrue(name(txn).getInstances().anyMatch(att -> att.getValue().equals("alice")));

                    assertEquals(1, dob(txn).getInstances().count());
                    assertTrue(dob(txn).getInstances().anyMatch(att -> att.getValue().equals(date_1992_2_3_4_5)));
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

        try (Grakn grakn = RocksGrakn.open(directory)) {
            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.DATA)) {
                Grakn.Transaction txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
                Grakn.Transaction txn2 = session.transaction(Arguments.Transaction.Type.WRITE);

                name(txn1).put("alice");
                name(txn2).put("alice");

                txn1.commit();
                txn2.commit();

                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    assertEquals("alice", name(txn).get("alice").getValue());
                    assertEquals(1, name(txn).getInstances().count());
                    assertTrue(name(txn).getInstances().anyMatch(att -> att.getValue().equals("alice")));
                }

                txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
                txn2 = session.transaction(Arguments.Transaction.Type.WRITE);

                name(txn1).put("alice");
                name(txn2).get("alice").delete();

                assertEquals(0, name(txn2).getInstances().count());
                assertEquals(1, name(txn1).getInstances().count());
                assertTrue(name(txn1).getInstances().anyMatch(att -> att.getValue().equals("alice")));

                txn1.commit(); // write before delete
                try {
                    txn2.commit();
                    fail("The delete operation in TX2 is not supposed to succeed");
                } catch (Exception ignored) {
                    assertTrue(true);
                }

                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    assertEquals("alice", name(txn).get("alice").getValue());
                    assertEquals(1, name(txn).getInstances().count());
                    assertTrue(name(txn).getInstances().anyMatch(att -> att.getValue().equals("alice")));
                }

                txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
                txn2 = session.transaction(Arguments.Transaction.Type.WRITE);

                name(txn1).put("alice");
                name(txn2).get("alice").delete();

                txn2.commit(); // delete before write
                txn1.commit();

                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    assertEquals("alice", name(txn).get("alice").getValue());
                    assertEquals(1, name(txn).getInstances().count());
                    assertTrue(name(txn).getInstances().anyMatch(att -> att.getValue().equals("alice")));
                }

                txn1 = session.transaction(Arguments.Transaction.Type.WRITE);
                txn2 = session.transaction(Arguments.Transaction.Type.WRITE);

                name(txn1).get("alice").delete();
                name(txn2).get("alice").delete();

                txn1.commit(); // delete concurrently
                try {
                    txn2.commit();
                    fail("The second delete operation in TX2 is not supposed to succeed");
                } catch (Exception ignored) {
                    assertTrue(true);
                }

                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    assertNull(name(txn).get("alice"));
                    assertEquals(0, name(txn).getInstances().count());
                }
            }
        }
    }
}
