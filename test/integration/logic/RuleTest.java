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

import grakn.core.Grakn;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Label;
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
import grakn.core.logic.resolvable.Concludable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.variable.Variable;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import grakn.core.traversal.common.Identifier;
import graql.lang.Graql;
import graql.lang.pattern.variable.Reference;
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
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.test.Util.assertNotThrows;
import static grakn.core.common.test.Util.assertThrowsGraknException;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class RuleTest {
    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("rule-test");
    private static String database = "rule-test";

    private long isaConcludablesCount(Set<Concludable> concludables) {
        return concludables.stream().filter(Concludable::isIsa).count();
    }

    private long hasConcludablesCount(Set<Concludable> concludables) {
        return concludables.stream().filter(Concludable::isHas).count();
    }

    private long relationConcludablesCount(Set<Concludable> concludables) {
        return concludables.stream().filter(Concludable::isRelation).count();
    }

    private long attributeConcludablesCount(Set<Concludable> concludables) {
        return concludables.stream().filter(Concludable::isAttribute).count();
    }

    private Variable getVariable(Set<Variable> vars, Identifier identifier) {
        return iterate(vars).filter(v -> v.id().equals(identifier)).next();
    }

    @Test
    public void rule_concludables_built_correctly_from_rule_concerning_relation() throws IOException {
        Util.resetDirectory(directory);

        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();
                    final LogicManager logicMgr = txn.logic();

                    final EntityType person = conceptMgr.putEntityType("person");
                    final RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    final RelationType marriage = conceptMgr.putRelationType("marriage");
                    marriage.setRelates("spouse");
                    person.setPlays(friendship.getRelates("friend"));
                    person.setPlays(marriage.getRelates("spouse"));
                    logicMgr.putRule(
                            "marriage-is-friendship",
                            Graql.parsePattern("{$x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    txn.commit();
                }
                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    final LogicManager logicMgr = txn.logic();
                    final Rule rule = logicMgr.getRule("marriage-is-friendship");

                    assertTrue(rule.conclusion().isRelation());
                    Set<Concludable> bodyConcludables = rule.whenConcludables();
                    assertEquals(2, isaConcludablesCount(bodyConcludables));
                    assertEquals(0, hasConcludablesCount(bodyConcludables));
                    assertEquals(1, relationConcludablesCount(bodyConcludables));
                    assertEquals(0, attributeConcludablesCount(bodyConcludables));
                }
            }
        }
    }

    @Test
    public void rule_concludables_built_correctly_from_rule_concerning_has_isa_value() throws IOException {
        Util.resetDirectory(directory);

        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();
                    final LogicManager logicMgr = txn.logic();

                    final EntityType milk = conceptMgr.putEntityType("milk");
                    final AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                    final AttributeType isStillGood = conceptMgr.putAttributeType("is-still-good", AttributeType.ValueType.BOOLEAN);
                    milk.setOwns(ageInDays);
                    milk.setOwns(isStillGood);
                    logicMgr.putRule(
                            "old-milk-is-not-good",
                            Graql.parsePattern("{ $x isa milk, has age-in-days >= 10; }").asConjunction(),
                            Graql.parseVariable("$x has is-still-good false").asThing());
                    txn.commit();
                }
                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    final LogicManager logicMgr = txn.logic();
                    final Rule rule = logicMgr.getRule("old-milk-is-not-good");

                    assertTrue(rule.conclusion().isExplicitHas());
                    Set<Concludable> bodyConcludables = rule.whenConcludables();
                    assertEquals(1, isaConcludablesCount(bodyConcludables));
                    assertEquals(1, hasConcludablesCount(bodyConcludables));
                    assertEquals(0, relationConcludablesCount(bodyConcludables));
                    assertEquals(0, attributeConcludablesCount(bodyConcludables));
                }
            }
        }
    }

    @Test
    public void rule_concludables_built_correctly_from_rule_concerning_has() throws IOException {
        Util.resetDirectory(directory);

        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();
                    final LogicManager logicMgr = txn.logic();

                    final EntityType milk = conceptMgr.putEntityType("milk");
                    final AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                    final AttributeType isStillGood = conceptMgr.putAttributeType("is-still-good", AttributeType.ValueType.BOOLEAN);
                    milk.setOwns(ageInDays);
                    milk.setOwns(isStillGood);
                    logicMgr.putRule(
                            "old-milk-is-not-good",
                            Graql.parsePattern("{ $x isa milk; $a 10 isa age-in-days; }").asConjunction(),
                            Graql.parseVariable("$x has $a").asThing());
                    txn.commit();
                }
                try (Grakn.Transaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    final LogicManager logicMgr = txn.logic();
                    final Rule rule = logicMgr.getRule("old-milk-is-not-good");

                    assertTrue(rule.conclusion().isVariableHas());
                    Set<Concludable> bodyConcludables = rule.whenConcludables();
                    assertEquals(2, isaConcludablesCount(bodyConcludables));
                    assertEquals(0, hasConcludablesCount(bodyConcludables));
                    assertEquals(0, relationConcludablesCount(bodyConcludables));
                    assertEquals(0, attributeConcludablesCount(bodyConcludables));
                }
            }
        }
    }

    // ------------ materialisation test ------------

    @Test
    public void rule_relation_materialises_when_missing() throws IOException {
        Util.resetDirectory(directory);

        try (RocksGrakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();
                    final LogicManager logicMgr = txn.logic();

                    final EntityType person = conceptMgr.putEntityType("person");
                    final RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    final RelationType marriage = conceptMgr.putRelationType("marriage");
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
                    final ConceptManager conceptMgr = txn.concepts();
                    txn.query().insert(Graql.parseQuery("insert $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage;").asInsert());

                    EntityType person = conceptMgr.getEntityType("person");
                    List<Entity> people = person.getInstances().collect(Collectors.toList());
                    assertEquals(2, people.size());

                    Rule rule = txn.logic().getRule("marriage-is-friendship");
                    ConceptMap whenAnswer = new ConceptMap(map(pair(Reference.name("x"), people.get(0)),
                            pair(Reference.name("y"), people.get(1))));

                    Map<Identifier, Concept> thenConcepts = rule.putConclusion(whenAnswer, txn.traversal(), conceptMgr);
                    assertEquals(4, thenConcepts.size());

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
        Util.resetDirectory(directory);

        try (RocksGrakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();
                    final LogicManager logicMgr = txn.logic();

                    final EntityType person = conceptMgr.putEntityType("person");
                    final RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    final RelationType marriage = conceptMgr.putRelationType("marriage");
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
                    final ConceptManager conceptMgr = txn.concepts();
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
                    ConceptMap whenAnswer = new ConceptMap(map(pair(Reference.name("x"), people.get(0)),
                            pair(Reference.name("y"), people.get(1))));

                    Map<Identifier, Concept> thenConcepts = rule.putConclusion(whenAnswer, txn.traversal(), conceptMgr);
                    assertEquals(4, thenConcepts.size());
                    friendshipInstances = friendship.getInstances().collect(Collectors.toList());
                    assertEquals(1, friendshipInstances.size());
                    assertEquals(friendshipInstances.get(0), thenConcepts.get(Identifier.Variable.label("friendship")));
                }
            }
        }
    }

    @Test
    public void rule_has_variable_materialises_when_missing() throws IOException {
        Util.resetDirectory(directory);

        try (RocksGrakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();

                    final EntityType milk = conceptMgr.putEntityType("milk");
                    final AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                    final AttributeType isStillGood = conceptMgr.putAttributeType("is-still-good", AttributeType.ValueType.BOOLEAN);
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
                    final ConceptManager conceptMgr = txn.concepts();

                    final EntityType milk = conceptMgr.getEntityType("milk");
                    final AttributeType ageInDays = conceptMgr.getAttributeType("age-in-days");

                    Entity milkInst = milk.create();
                    Attribute.Long ageInDays10 = ageInDays.asLong().put(10L);

                    Rule rule = txn.logic().getRule("old-milk-is-not-good");
                    ConceptMap whenAnswer = new ConceptMap(map(pair(Reference.name("x"), milkInst),
                            pair(Reference.name("a"), ageInDays10)));
                    Map<Identifier, Concept> thenConcepts = rule.putConclusion(whenAnswer, txn.traversal(), conceptMgr);
                    assertEquals(2, thenConcepts.size());

                    List<? extends Attribute> ageInDaysOwned = milkInst.getHas(ageInDays).collect(Collectors.toList());
                    assertEquals(1, ageInDaysOwned.size());
                    assertEquals(ageInDays10, ageInDaysOwned.get(0));
                }
            }
        }
    }

    @Test
    public void rule_has_explicit_materialises_when_missing() throws IOException {
        Util.resetDirectory(directory);

        try (RocksGrakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();

                    final EntityType milk = conceptMgr.putEntityType("milk");
                    final AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                    final AttributeType isStillGood = conceptMgr.putAttributeType("is-still-good", AttributeType.ValueType.BOOLEAN);
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
                    final ConceptManager conceptMgr = txn.concepts();

                    final EntityType milk = conceptMgr.getEntityType("milk");
                    final AttributeType ageInDays = conceptMgr.getAttributeType("age-in-days");

                    Entity milkInst = milk.create();
                    milkInst.setHas(ageInDays.asLong().put(20L));

                    Rule rule = txn.logic().getRule("old-milk-is-not-good");
                    ConceptMap whenAnswer = new ConceptMap(map(pair(Reference.name("x"), milkInst)));
                    Map<Identifier, Concept> thenConcepts = rule.putConclusion(whenAnswer, txn.traversal(), conceptMgr);
                    assertEquals(3, thenConcepts.size());

                    final AttributeType isStillGood = conceptMgr.getAttributeType("is-still-good");
                    List<? extends Attribute> isStillGoodOwned = milkInst.getHas(isStillGood).collect(Collectors.toList());
                    assertEquals(1, isStillGoodOwned.size());
                    assertEquals(isStillGood.asBoolean().getInstances().findFirst().get(), isStillGoodOwned.get(0));
                }
            }
        }
    }

    // ------------ concludable types indexing test (do these belong in a LogicManagerTest?) ------------

    @Test
    public void rule_indexes_created_and_readable() throws IOException {
        Util.resetDirectory(directory);

        try (RocksGrakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();
                    final LogicManager logicMgr = txn.logic();

                    final EntityType person = conceptMgr.putEntityType("person");
                    final RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    final RelationType marriage = conceptMgr.putRelationType("marriage");
                    final AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
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
            }
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    LogicManager logicMgr = txn.logic();

                    Set<Rule> friendshipRules = logicMgr.rulesConcluding(Label.of("friendship")).toSet();
                    Rule marriageFriendsRule = txn.logic().getRule("marriage-is-friendship");
                    Rule allFriendsRule = txn.logic().getRule("all-people-are-friends");
                    assertEquals(set(marriageFriendsRule, allFriendsRule), friendshipRules);

                    Set<Rule> hasNameRules = logicMgr.rulesConcludingHasAttribute(Label.of("name")).toSet();
                    Rule marriageSameName = txn.logic().getRule("marriage-same-name");
                    assertEquals(set(marriageSameName), hasNameRules);
                }
            }
        }
    }

    @Test
    public void rule_indexes_update_on_rule_delete() throws IOException {
        Util.resetDirectory(directory);

        try (RocksGrakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();
                    final LogicManager logicMgr = txn.logic();

                    final EntityType person = conceptMgr.putEntityType("person");
                    final RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    final RelationType marriage = conceptMgr.putRelationType("marriage");
                    final AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
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
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    LogicManager logicMgr = txn.logic();
                    Set<Rule> friendshipRules = logicMgr.rulesConcluding(Label.of("friendship")).toSet();
                    Rule marriageFriendsRule = txn.logic().getRule("marriage-is-friendship");
                    Rule allFriendsRule = txn.logic().getRule("all-people-are-friends");
                    assertEquals(set(marriageFriendsRule, allFriendsRule), friendshipRules);
                    allFriendsRule.delete();

                    Set<Rule> hasNameRules = logicMgr.rulesConcludingHasAttribute(Label.of("name")).toSet();
                    Rule marriageSameName = txn.logic().getRule("marriage-same-name");
                    assertEquals(set(marriageSameName), hasNameRules);
                    marriageSameName.delete();

                    txn.commit();
                }
            }
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    LogicManager logicMgr = txn.logic();
                    Set<Rule> friendshipRules = logicMgr.rulesConcluding(Label.of("friendship")).toSet();
                    Rule marriageFriendsRule = txn.logic().getRule("marriage-is-friendship");
                    assertEquals(set(marriageFriendsRule), friendshipRules);

                    Set<Rule> hasNameRules = logicMgr.rulesConcludingHasAttribute(Label.of("name")).toSet();
                    assertEquals(set(), hasNameRules);
                }
            }
        }
    }

    // TODO test: adding a new type updates conclusion index

    // ------------ mentioned types indexing test (do these belong in higher level test?) ------------

    private void assertIndexTypesContainRule(Set<Label> types, String requiredRule, GraphManager graphMgr) {
        types.forEach(t -> {
            Set<String> rules = graphMgr.schema().ruleIndex().rulesContaining(t).map(RuleStructure::label).toSet();
            assertTrue(rules.contains(requiredRule));
        });
    }

    @Test
    public void rule_contains_indexes_prevent_undefining_contained_types() throws IOException {
        Util.resetDirectory(directory);

        try (RocksGrakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();
                    final LogicManager logicMgr = txn.logic();

                    final EntityType person = conceptMgr.putEntityType("person");
                    final RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    final RelationType marriage = conceptMgr.putRelationType("marriage");
                    final AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
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
                    assertThrowsGraknException(person::delete, ErrorMessage.TypeWrite.TYPE_PRESENT_IN_RULES.code());
                }
            }
        }
    }

    @Test
    public void rule_contains_indexes_allow_deleting_type_after_deleting_rule() throws IOException {
        Util.resetDirectory(directory);

        try (RocksGrakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);
            try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();
                    final LogicManager logicMgr = txn.logic();
                    final GraphManager graphMgr = logicMgr.graph();

                    final EntityType person = conceptMgr.putEntityType("person");
                    final RelationType friendship = conceptMgr.putRelationType("friendship");
                    friendship.setRelates("friend");
                    final RelationType marriage = conceptMgr.putRelationType("marriage");
                    final AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
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
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.READ)){
                    LogicManager logicMgr = txn.logic();
                    Rule marriageFriendsRule = logicMgr.putRule(
                            "marriage-is-friendship",
                            Graql.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    assertIndexTypesContainRule(set(Label.of("person"), Label.of("spouse", "marriage"),
                                                    Label.of("marriage"), Label.of("friend", "friendship"), Label.of("friendship")),
                                                marriageFriendsRule.getLabel(),
                                                logicMgr.graph()
                    );
                    Rule marriageSameName = logicMgr.putRule(
                            "marriage-same-name",
                            Graql.parsePattern("{ $x isa person, has name $a; $y isa person; (spouse:$x, spouse: $y) isa marriage; }").asConjunction(),
                            Graql.parseVariable("$y has $a").asThing());
                    assertIndexTypesContainRule(set(Label.of("person"), Label.of("spouse", "marriage"), Label.of("marriage"), Label.of("name")),
                                                marriageSameName.getLabel(),
                                                logicMgr.graph()
                    );
                }
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    RelationType friendship = conceptMgr.getRelationType("friendship");
                    assertThrowsGraknException(friendship::delete, ErrorMessage.TypeWrite.TYPE_PRESENT_IN_RULES.code());
                }
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    AttributeType name = conceptMgr.getAttributeType("name");
                    assertThrowsGraknException(name::delete, ErrorMessage.TypeWrite.TYPE_PRESENT_IN_RULES.code());
                }
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    EntityType person = conceptMgr.getEntityType("person");
                    AttributeType name = conceptMgr.getAttributeType("name");

                    LogicManager logicMgr = txn.logic();
                    Rule marriageSameName = logicMgr.getRule("marriage-same-name");
                    marriageSameName.delete();
                    assertNotThrows(name::delete);
                    txn.commit();
                }
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    RelationType person = conceptMgr.getRelationType("friendship");
                    LogicManager logicMgr = txn.logic();
                    Rule marriageIsFriendship = logicMgr.getRule("marriage-is-friendship");
                    marriageIsFriendship.delete();
                    assertNotThrows(person::delete);
                    txn.commit();
                }
                try (RocksTransaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
                    LogicManager logicMgr = txn.logic();
                    GraphManager graphMgr = logicMgr.graph();
                    // no types should be in the index
                    graphMgr.schema().thingTypes().forEachRemaining(type -> {
                        assertFalse(graphMgr.schema().ruleIndex().rulesContaining(type.properLabel()).hasNext());
                    });
                }
            }
        }
    }
}
