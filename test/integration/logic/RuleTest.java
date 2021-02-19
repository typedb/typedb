/*
 * Copyright (C) 2021 Grakn Labs
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
 */

package grakn.core.logic;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Label;
import grakn.core.common.parameters.Options.Database;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.graph.GraphManager;
import grakn.core.graph.structure.RuleStructure;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.variable.Variable;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import grakn.core.traversal.common.Identifier;
import graql.lang.Graql;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.RuleWrite.RULE_CANNOT_BE_SATISFIED;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.test.Util.assertNotThrows;
import static grakn.core.common.test.Util.assertThrows;
import static grakn.core.common.test.Util.assertThrowsGraknException;
import static grakn.core.common.test.Util.assertThrowsWithMessage;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class RuleTest {
    private static Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("rule-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).logsDir(logDir);
    private static String database = "rule-test";

    private Variable getVariable(Set<Variable> vars, Identifier identifier) {
        return iterate(vars).filter(v -> v.id().equals(identifier)).next();
    }

    @Test
    public void rule_relation_materialises_when_missing() throws IOException {
        Util.resetDirectory(dataDir);

        try (RocksGrakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    LogicManager logicMgr = txn.logic();

                    EntityType person = conceptMgr.putEntityType("person");
                    RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    RelationType marriage = conceptMgr.putRelationType("marriage");
                    marriage.setRelates("spouse");
                    person.setPlays(friendship.getRelates("friend"));
                    person.setPlays(marriage.getRelates("spouse"));
                    logicMgr.putRule(
                            "marriage-is-friendship",
                            Graql.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    txn.commit();
                }
            }
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    txn.query().insert(Graql.parseQuery("insert $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage;").asInsert());

                    EntityType person = conceptMgr.getEntityType("person");
                    List<Entity> people = person.getInstances().collect(Collectors.toList());
                    assertEquals(2, people.size());

                    Rule rule = txn.logic().getRule("marriage-is-friendship");
                    ConceptMap whenAnswer = new ConceptMap(map(pair(Identifier.Variable.name("x"), people.get(0)),
                                                               pair(Identifier.Variable.name("y"), people.get(1))));

                    List<Map<Identifier.Variable, Concept>> materialisations = rule.conclusion().materialise(whenAnswer, txn.traversal(), conceptMgr).toList();
                    assertEquals(1, materialisations.size());
                    assertEquals(5, materialisations.get(0).size());

                    RelationType friendship = conceptMgr.getRelationType("friendship");
                    List<Relation> friendshipInstances = friendship.getInstances().collect(Collectors.toList());
                    assertEquals(1, friendshipInstances.size());
                    assertEquals(set(people.get(0), people.get(1)), friendshipInstances.get(0).getPlayers().collect(Collectors.toSet()));
                }
            }
        }
    }

    @Test
    public void rule_relation_does_not_materialise_when_present() throws IOException {
        Util.resetDirectory(dataDir);

        try (RocksGrakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    LogicManager logicMgr = txn.logic();

                    EntityType person = conceptMgr.putEntityType("person");
                    RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    RelationType marriage = conceptMgr.putRelationType("marriage");
                    marriage.setRelates("spouse");
                    person.setPlays(friendship.getRelates("friend"));
                    person.setPlays(marriage.getRelates("spouse"));
                    logicMgr.putRule(
                            "marriage-is-friendship",
                            Graql.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    txn.commit();
                }
            }
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    RelationType friendship = conceptMgr.getRelationType("friendship");
                    txn.query().insert(Graql.parseQuery("insert $x isa person; $y isa person; " +
                                                                "(spouse: $x, spouse: $y) isa marriage;" +
                                                                "(friend: $x, friend: $y) isa friendship;").asInsert());
                    List<Relation> friendshipInstances = friendship.getInstances().collect(Collectors.toList());
                    assertEquals(1, friendshipInstances.size());

                    EntityType person = conceptMgr.getEntityType("person");
                    List<Entity> people = person.getInstances().collect(Collectors.toList());
                    assertEquals(2, people.size());

                    Rule rule = txn.logic().getRule("marriage-is-friendship");
                    ConceptMap whenAnswer = new ConceptMap(map(pair(Identifier.Variable.name("x"), people.get(0)),
                                                               pair(Identifier.Variable.name("y"), people.get(1))));

                    List<Map<Identifier.Variable, Concept>> materialisations = rule.conclusion().materialise(whenAnswer, txn.traversal(), conceptMgr).toList();
                    assertEquals(1, materialisations.size());
                    assertEquals(5, materialisations.get(0).size());
                    friendshipInstances = friendship.getInstances().collect(Collectors.toList());
                    assertEquals(1, friendshipInstances.size());
                    assertEquals(friendshipInstances.get(0), materialisations.get(0).get(Identifier.Variable.anon(0)));
                    assertEquals(friendship, materialisations.get(0).get(Identifier.Variable.label("friendship")));
                }
            }
        }
    }

    @Test
    public void rule_has_variable_materialises_when_missing() throws IOException {
        Util.resetDirectory(dataDir);

        try (RocksGrakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();

                    EntityType milk = conceptMgr.putEntityType("milk");
                    AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                    AttributeType isStillGood = conceptMgr.putAttributeType("is-still-good", AttributeType.ValueType.BOOLEAN);
                    milk.setOwns(ageInDays);
                    milk.setOwns(isStillGood);
                    txn.logic().putRule(
                            "old-milk-is-not-good",
                            Graql.parsePattern("{ $x isa milk; $a 10 isa age-in-days; }").asConjunction(),
                            Graql.parseVariable("$x has $a").asThing());
                    txn.commit();
                }
            }
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();

                    EntityType milk = conceptMgr.getEntityType("milk");
                    AttributeType ageInDays = conceptMgr.getAttributeType("age-in-days");

                    Entity milkInst = milk.create();
                    Attribute.Long ageInDays10 = ageInDays.asLong().put(10L);

                    Rule rule = txn.logic().getRule("old-milk-is-not-good");
                    ConceptMap whenAnswer = new ConceptMap(map(pair(Identifier.Variable.name("x"), milkInst),
                                                               pair(Identifier.Variable.name("a"), ageInDays10)));
                    List<Map<Identifier.Variable, Concept>> materialisations = rule.conclusion().materialise(whenAnswer, txn.traversal(), conceptMgr).toList();
                    assertEquals(1, materialisations.size());
                    assertEquals(2, materialisations.get(0).size());

                    List<? extends Attribute> ageInDaysOwned = milkInst.getHas(ageInDays).collect(Collectors.toList());
                    assertEquals(1, ageInDaysOwned.size());
                    assertEquals(ageInDays10, ageInDaysOwned.get(0));
                }
            }
        }
    }

    @Test
    public void rule_has_explicit_materialises_when_missing() throws IOException {
        Util.resetDirectory(dataDir);

        try (RocksGrakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();

                    EntityType milk = conceptMgr.putEntityType("milk");
                    AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                    AttributeType isStillGood = conceptMgr.putAttributeType("is-still-good", AttributeType.ValueType.BOOLEAN);
                    milk.setOwns(ageInDays);
                    milk.setOwns(isStillGood);
                    txn.logic().putRule(
                            "old-milk-is-not-good",
                            Graql.parsePattern("{ $x isa milk, has age-in-days $a; $a >= 10; }").asConjunction(),
                            Graql.parseVariable("$x has is-still-good false").asThing());
                    txn.commit();
                }
            }
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();

                    EntityType milk = conceptMgr.getEntityType("milk");
                    AttributeType ageInDays = conceptMgr.getAttributeType("age-in-days");

                    Entity milkInst = milk.create();
                    milkInst.setHas(ageInDays.asLong().put(20L));

                    Rule rule = txn.logic().getRule("old-milk-is-not-good");
                    ConceptMap whenAnswer = new ConceptMap(map(pair(Identifier.Variable.name("x"), milkInst)));
                    List<Map<Identifier.Variable, Concept>> materialisations = rule.conclusion().materialise(whenAnswer, txn.traversal(), conceptMgr).toList();
                    assertEquals(1, materialisations.size());
                    assertEquals(3, materialisations.get(0).size());

                    AttributeType isStillGood = conceptMgr.getAttributeType("is-still-good");
                    List<? extends Attribute> isStillGoodOwned = milkInst.getHas(isStillGood).collect(Collectors.toList());
                    assertEquals(1, isStillGoodOwned.size());
                    assertEquals(isStillGood.asBoolean().getInstances().findFirst().get(), isStillGoodOwned.get(0));
                }
            }
        }
    }

    // ------------ Rule conclusion indexing ------------

    @Test
    public void rule_indexes_created_and_readable() throws IOException {
        Util.resetDirectory(dataDir);

        try (RocksGrakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    LogicManager logicMgr = txn.logic();

                    EntityType person = conceptMgr.putEntityType("person");
                    RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    RelationType marriage = conceptMgr.putRelationType("marriage");
                    AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                    AttributeType age = conceptMgr.putAttributeType("age", AttributeType.ValueType.LONG);
                    marriage.setRelates("spouse");
                    person.setPlays(friendship.getRelates("friend"));
                    person.setPlays(marriage.getRelates("spouse"));
                    person.setOwns(name);
                    person.setOwns(age);
                    Rule marriageFriendsRule = logicMgr.putRule(
                            "marriage-is-friendship",
                            Graql.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    Conjunction marriageFriendsThen = marriageFriendsRule.then();
                    Variable marriageFriendsRelation = getVariable(marriageFriendsThen.variables(), Identifier.Variable.anon(0));
                    assertEquals(set(Label.of("friendship")), marriageFriendsRelation.resolvedTypes());

                    Rule allFriendsRule = logicMgr.putRule(
                            "all-people-are-friends",
                            Graql.parsePattern("{ $x isa person; $y isa person; $t type friendship; }").asConjunction(),
                            Graql.parseVariable("(friend: $x, friend: $y) isa $t").asThing());
                    Conjunction allFriendsThen = allFriendsRule.then();
                    Variable allFriendsRelation = getVariable(allFriendsThen.variables(), Identifier.Variable.anon(0));
                    assertEquals(set(Label.of("friendship")), allFriendsRelation.resolvedTypes());

                    Rule marriageSameName = logicMgr.putRule(
                            "marriage-same-name",
                            Graql.parsePattern("{ $x isa person, has name $a; $y isa person; (spouse:$x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("$y has $a").asThing());
                    Conjunction sameName = marriageSameName.then();
                    Variable nameAttr = getVariable(sameName.variables(), Identifier.Variable.name("a"));
                    assertEquals(set(Label.of("name")), nameAttr.resolvedTypes());

                    Rule peopleHaveAge10 = logicMgr.putRule(
                            "people-have-age-10",
                            Graql.parsePattern("{ $x isa person; }").asConjunction(),
                            Graql.parseVariable("$x has age 10").asThing()
                    );
                    Conjunction age10 = peopleHaveAge10.then();
                    Variable ageAttr = getVariable(age10.variables(), Identifier.Variable.anon(0));
                    assertEquals(set(Label.of("age")), ageAttr.resolvedTypes());

                    txn.commit();
                }
            }
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    LogicManager logicMgr = txn.logic();

                    Set<Rule> friendshipRules = logicMgr.rulesConcluding(Label.of("friendship")).toSet();
                    Rule marriageFriendsRule = txn.logic().getRule("marriage-is-friendship");
                    Rule allFriendsRule = txn.logic().getRule("all-people-are-friends");
                    assertEquals(set(marriageFriendsRule, allFriendsRule), friendshipRules);

                    Set<Rule> hasNameRules = logicMgr.rulesConcludingHas(Label.of("name")).toSet();
                    Rule marriageSameName = txn.logic().getRule("marriage-same-name");
                    assertEquals(set(marriageSameName), hasNameRules);

                    Set<Rule> hasAgeRules = logicMgr.rulesConcludingHas(Label.of("age")).toSet();
                    Set<Rule> ageRules = logicMgr.rulesConcluding(Label.of("age")).toSet();
                    Rule peopleHaveAge10 = txn.logic().getRule("people-have-age-10");
                    assertEquals(set(peopleHaveAge10), hasAgeRules);
                    assertEquals(set(peopleHaveAge10), ageRules);
                }
            }
        }
    }

    @Test
    public void rule_indexes_update_on_rule_delete() throws IOException {
        Util.resetDirectory(dataDir);

        try (RocksGrakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    LogicManager logicMgr = txn.logic();

                    EntityType person = conceptMgr.putEntityType("person");
                    RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    RelationType marriage = conceptMgr.putRelationType("marriage");
                    AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                    marriage.setRelates("spouse");
                    person.setPlays(friendship.getRelates("friend"));
                    person.setPlays(marriage.getRelates("spouse"));
                    person.setOwns(name);
                    Rule marriageFriendsRule = logicMgr.putRule(
                            "marriage-is-friendship",
                            Graql.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    Conjunction marriageFriendsThen = marriageFriendsRule.then();
                    Variable marriageFriendsRelation = getVariable(marriageFriendsThen.variables(), Identifier.Variable.anon(0));
                    assertEquals(set(Label.of("friendship")), marriageFriendsRelation.resolvedTypes());

                    Rule allFriendsRule = logicMgr.putRule(
                            "all-people-are-friends",
                            Graql.parsePattern("{ $x isa person; $y isa person; $t type friendship; }").asConjunction(),
                            Graql.parseVariable("(friend: $x, friend: $y) isa $t").asThing());
                    Conjunction allFriendsThen = allFriendsRule.then();
                    Variable allFriendsRelation = getVariable(allFriendsThen.variables(), Identifier.Variable.anon(0));
                    assertEquals(set(Label.of("friendship")), allFriendsRelation.resolvedTypes());

                    Rule marriageSameName = logicMgr.putRule(
                            "marriage-same-name",
                            Graql.parsePattern("{ $x isa person, has name $a; $y isa person; (spouse:$x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("$y has $a").asThing());
                    Conjunction sameName = marriageSameName.then();
                    Variable nameAttr = getVariable(sameName.variables(), Identifier.Variable.name("a"));
                    assertEquals(set(Label.of("name")), nameAttr.resolvedTypes());

                    txn.commit();
                }
                // check index after commit, and delete some rules
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    LogicManager logicMgr = txn.logic();
                    Set<Rule> friendshipRules = logicMgr.rulesConcluding(Label.of("friendship")).toSet();
                    Rule marriageFriendsRule = txn.logic().getRule("marriage-is-friendship");
                    Rule allFriendsRule = txn.logic().getRule("all-people-are-friends");
                    assertEquals(set(marriageFriendsRule, allFriendsRule), friendshipRules);
                    allFriendsRule.delete();

                    Set<Rule> hasNameRules = logicMgr.rulesConcludingHas(Label.of("name")).toSet();
                    Rule marriageSameName = txn.logic().getRule("marriage-same-name");
                    assertEquals(set(marriageSameName), hasNameRules);
                    marriageSameName.delete();

                    txn.commit();
                }
            }
            // check indexed types, should only includes rules that are still present
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    LogicManager logicMgr = txn.logic();
                    Set<Rule> friendshipRules = logicMgr.rulesConcluding(Label.of("friendship")).toSet();
                    Rule marriageFriendsRule = txn.logic().getRule("marriage-is-friendship");
                    assertEquals(set(marriageFriendsRule), friendshipRules);

                    Set<Rule> hasNameRules = logicMgr.rulesConcludingHas(Label.of("name")).toSet();
                    assertEquals(set(), hasNameRules);
                }
            }
        }
    }

    @Test
    public void new_type_updates_rule_conclusion_index() throws IOException {
        Util.resetDirectory(dataDir);

        try (RocksGrakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    LogicManager logicMgr = txn.logic();

                    EntityType person = conceptMgr.putEntityType("person");
                    RelationType marriage = conceptMgr.putRelationType("marriage");
                    AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                    name.setAbstract();
                    AttributeType firstName = conceptMgr.putAttributeType("first-name", AttributeType.ValueType.STRING);
                    firstName.setSupertype(name);
                    marriage.setRelates("spouse");
                    person.setPlays(marriage.getRelates("spouse"));
                    person.setOwns(firstName);

                    Rule marriageSameName = logicMgr.putRule(
                            "marriage-same-name",
                            Graql.parsePattern("{ $x isa person, has name $a; $y isa person; (spouse:$x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("$y has $a").asThing());
                    Conjunction sameName = marriageSameName.then();
                    Variable nameAttr = getVariable(sameName.variables(), Identifier.Variable.name("a"));
                    assertEquals(set(Label.of("first-name")), nameAttr.resolvedTypes());

                    txn.commit();
                }
                // add a new subtype of an attribute in a rule
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    AttributeType lastName = conceptMgr.putAttributeType("last-name", AttributeType.ValueType.STRING);
                    lastName.setSupertype(conceptMgr.getAttributeType("name"));
                    conceptMgr.getEntityType("person").setOwns(lastName);
                    txn.commit();
                }
                // check the new attribute type is re-indexed in the conclusions index
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    LogicManager logicMgr = txn.logic();
                    Rule marriageSameName = logicMgr.getRule("marriage-same-name");
                    assertEquals(set(marriageSameName), logicMgr.rulesConcludingHas(Label.of("last-name")).toSet());
                }
            }
        }
    }

    // ------------ Rule type labels indexing ------------

    private void assertIndexTypesContainRule(Set<Label> types, String requiredRule, GraphManager graphMgr) {
        types.forEach(t -> {
            Set<String> rules = graphMgr.schema().rules().references().get(graphMgr.schema().getType(t)).map(RuleStructure::label).toSet();
            assertTrue(rules.contains(requiredRule));
        });
    }

    @Test
    public void rule_contains_indexes_prevent_undefining_contained_types() throws IOException {
        Util.resetDirectory(dataDir);

        try (RocksGrakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    LogicManager logicMgr = txn.logic();

                    EntityType person = conceptMgr.putEntityType("person");
                    RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    RelationType marriage = conceptMgr.putRelationType("marriage");
                    AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                    marriage.setRelates("spouse");
                    person.setPlays(friendship.getRelates("friend"));
                    person.setPlays(marriage.getRelates("spouse"));
                    person.setOwns(name);
                    Rule marriageFriendsRule = logicMgr.putRule(
                            "marriage-is-friendship",
                            Graql.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    Conjunction marriageFriendsThen = marriageFriendsRule.then();
                    Variable marriageFriendsRelation = getVariable(marriageFriendsThen.variables(), Identifier.Variable.anon(0));
                    assertEquals(set(Label.of("friendship")), marriageFriendsRelation.resolvedTypes());

                    Rule marriageSameName = logicMgr.putRule(
                            "marriage-same-name",
                            Graql.parsePattern("{ $x isa person, has name $a; $y isa person; (spouse:$x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("$y has $a").asThing());
                    Conjunction sameName = marriageSameName.then();
                    Variable nameAttr = getVariable(sameName.variables(), Identifier.Variable.name("a"));
                    assertEquals(set(Label.of("name")), nameAttr.resolvedTypes());

                    txn.commit();
                }
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    EntityType person = conceptMgr.getEntityType("person");
                    assertThrowsGraknException(person::delete, ErrorMessage.TypeWrite.TYPE_REFERENCED_IN_RULES.code());
                }
            }
        }
    }

    @Test
    public void rule_contains_indexes_allow_deleting_type_after_deleting_rule() throws IOException {
        Util.resetDirectory(dataDir);

        try (RocksGrakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    LogicManager logicMgr = txn.logic();
                    GraphManager graphMgr = logicMgr.graph();

                    EntityType person = conceptMgr.putEntityType("person");
                    RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    RelationType marriage = conceptMgr.putRelationType("marriage");
                    AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                    marriage.setRelates("spouse");
                    person.setPlays(friendship.getRelates("friend"));
                    person.setPlays(marriage.getRelates("spouse"));
                    person.setOwns(name);
                    Rule marriageFriendsRule = logicMgr.putRule(
                            "marriage-is-friendship",
                            Graql.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    assertIndexTypesContainRule(set(Label.of("person"), Label.of("spouse", "marriage"),
                                                    Label.of("marriage"), Label.of("friend", "friendship"), Label.of("friendship")),
                                                marriageFriendsRule.getLabel(),
                                                graphMgr
                    );

                    Rule marriageSameName = logicMgr.putRule(
                            "marriage-same-name",
                            Graql.parsePattern("{ $x isa person, has name $a; $y isa person; (spouse:$x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("$y has $a").asThing());
                    assertIndexTypesContainRule(set(Label.of("person"), Label.of("spouse", "marriage"), Label.of("marriage"), Label.of("name")),
                                                marriageSameName.getLabel(),
                                                graphMgr
                    );

                    txn.commit();
                }
                // check the rule index is still established after commit
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    Rule marriageFriendsRule = txn.logic().getRule("marriage-is-friendship");
                    assertIndexTypesContainRule(set(Label.of("person"), Label.of("spouse", "marriage"),
                                                    Label.of("marriage"), Label.of("friend", "friendship"), Label.of("friendship")),
                                                marriageFriendsRule.getLabel(), txn.logic().graph());

                    Rule marriageSameName = txn.logic().getRule("marriage-same-name");
                    assertIndexTypesContainRule(set(Label.of("person"), Label.of("spouse", "marriage"), Label.of("marriage"), Label.of("name")),
                                                marriageSameName.getLabel(), txn.logic().graph()
                    );
                }
                // deleting a relation type used in a rule should throw
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    RelationType friendship = conceptMgr.getRelationType("friendship");
                    assertThrowsGraknException(friendship::delete, ErrorMessage.TypeWrite.TYPE_REFERENCED_IN_RULES.code());
                    assertTrue(!txn.isOpen());
                }
                // deleting an attribute type used in a rule should throw
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    AttributeType name = conceptMgr.getAttributeType("name");
                    assertThrowsGraknException(name::delete, ErrorMessage.TypeWrite.TYPE_REFERENCED_IN_RULES.code());
                    assertTrue(!txn.isOpen());
                }
                // deleting a rule, then an attribute type used in the rule is allowed
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    AttributeType name = conceptMgr.getAttributeType("name");

                    LogicManager logicMgr = txn.logic();
                    Rule marriageSameName = logicMgr.getRule("marriage-same-name");
                    marriageSameName.delete();
                    assertNotThrows(name::delete);
                    txn.commit();
                }
                // deleting a rule, then an entity type used in the rule is allowed
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    RelationType person = conceptMgr.getRelationType("friendship");
                    LogicManager logicMgr = txn.logic();
                    Rule marriageIsFriendship = logicMgr.getRule("marriage-is-friendship");
                    marriageIsFriendship.delete();
                    assertNotThrows(person::delete);
                    txn.commit();
                }
                // after all rules are deleted, no rules should exist in the index
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    LogicManager logicMgr = txn.logic();
                    GraphManager graphMgr = logicMgr.graph();
                    // no types should be in the index
                    graphMgr.schema().thingTypes().forEachRemaining(type -> {
                        assertFalse(graphMgr.schema().rules().references().get(graphMgr.schema().getType(type.properLabel())).hasNext());
                    });
                }
            }
        }
    }

    @Test
    public void rule_that_cannot_be_satisfied_throws_an_error() throws IOException {
        Util.resetDirectory(dataDir);

        try (RocksGrakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();

                    final EntityType person = conceptMgr.putEntityType("person");
                    final EntityType dog = conceptMgr.putEntityType("dog");
                    final AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                    person.setOwns(name);

                    assertThrowsWithMessage(() -> txn.logic().putRule(
                            "dogs-are-named-fido",
                            Graql.parsePattern("{$x isa dog;}").asConjunction(),
                            Graql.parseVariable("$x has name 'fido'").asThing()),
                                            GraknException.of(RULE_CANNOT_BE_SATISFIED, "dogs-are-named-fido", "$x").getMessage());
                }
            }
        }
    }

    @Test
    public void rule_that_cannot_be_inserted_throws_an_error() throws IOException {
        Util.resetDirectory(dataDir);

        try (RocksGrakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();

                    final EntityType animal = conceptMgr.putEntityType("animal");
                    final EntityType person = conceptMgr.putEntityType("person");
                    final EntityType dog = conceptMgr.putEntityType("dog");
                    final AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                    person.setOwns(name);
                    person.setSupertype(animal);
                    dog.setSupertype(animal);

                    assertThrows(() -> txn.logic().putRule(
                            "animals-are-named-fido",
                            Graql.parsePattern("{$x isa animal;}").asConjunction(),
                            Graql.parseVariable("$x has name 'fido'").asThing()));
                }
            }
        }
    }
}
