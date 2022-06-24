/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.logic;

import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.structure.RuleStructure;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.CONTRADICTORY_RULE_CYCLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.RULE_THEN_CANNOT_BE_SATISFIED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.RULE_WHEN_CANNOT_BE_SATISFIED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.test.Util.assertNotThrows;
import static com.vaticle.typedb.core.common.test.Util.assertThrows;
import static com.vaticle.typedb.core.common.test.Util.assertThrowsTypeDBException;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class RuleTest {
    private static Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("rule-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageIndexCacheSize(MB).storageDataCacheSize(MB);
    private static String database = "rule-test";

    private Variable getVariable(Set<Variable> vars, Identifier identifier) {
        return iterate(vars).filter(v -> v.id().equals(identifier)).next();
    }

    @Test
    public void rule_relation_materialises_when_missing() throws IOException {
        Util.resetDirectory(dataDir);

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
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
                            TypeQL.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            TypeQL.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    txn.commit();
                }
            }
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    txn.query().insert(TypeQL.parseQuery("insert $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage;").asInsert());

                    EntityType person = conceptMgr.getEntityType("person");
                    List<? extends Entity> people = person.getInstances().toList();
                    assertEquals(2, people.size());

                    Rule rule = txn.logic().getRule("marriage-is-friendship");
                    ConceptMap whenAnswer = new ConceptMap(map(pair(Identifier.Variable.name("x"), people.get(0)),
                                                               pair(Identifier.Variable.name("y"), people.get(1))));

                    Optional<Map<Identifier.Variable, Concept>> materialisation = rule.conclusion().materialiseAndBind(whenAnswer, txn.traversal(), conceptMgr);
                    assertTrue(materialisation.isPresent());
                    assertEquals(5, materialisation.get().size());

                    RelationType friendship = conceptMgr.getRelationType("friendship");
                    List<? extends Relation> friendshipInstances = friendship.getInstances().toList();
                    assertEquals(1, friendshipInstances.size());
                    assertEquals(set(people.get(0), people.get(1)), friendshipInstances.get(0).getPlayers().toSet());
                }
            }
        }
    }

    @Test
    public void rule_relation_does_not_materialise_when_present() throws IOException {
        Util.resetDirectory(dataDir);

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
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
                            TypeQL.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            TypeQL.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    txn.commit();
                }
            }
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    RelationType friendship = conceptMgr.getRelationType("friendship");
                    txn.query().insert(TypeQL.parseQuery("insert $x isa person; $y isa person; " +
                                                                 "(spouse: $x, spouse: $y) isa marriage;" +
                                                                 "(friend: $x, friend: $y) isa friendship;").asInsert());
                    List<? extends Relation> friendshipInstances = friendship.getInstances().toList();
                    assertEquals(1, friendshipInstances.size());

                    EntityType person = conceptMgr.getEntityType("person");
                    List<? extends Entity> people = person.getInstances().toList();
                    assertEquals(2, people.size());

                    Rule rule = txn.logic().getRule("marriage-is-friendship");
                    ConceptMap whenAnswer = new ConceptMap(map(pair(Identifier.Variable.name("x"), people.get(0)),
                                                               pair(Identifier.Variable.name("y"), people.get(1))));

                    Optional<Map<Identifier.Variable, Concept>> materialisation = rule.conclusion().materialiseAndBind(whenAnswer, txn.traversal(), conceptMgr);
                    assertFalse(materialisation.isPresent());
                }
            }
        }
    }

    @Test
    public void rule_has_variable_materialises_when_missing() throws IOException {
        Util.resetDirectory(dataDir);

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();

                    EntityType milk = conceptMgr.putEntityType("milk");
                    AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                    AttributeType isStillGood = conceptMgr.putAttributeType("is-still-good", AttributeType.ValueType.BOOLEAN);
                    milk.setOwns(ageInDays);
                    milk.setOwns(isStillGood);
                    txn.logic().putRule(
                            "old-milk-is-not-good",
                            TypeQL.parsePattern("{ $x isa milk; $a 10 isa age-in-days; }").asConjunction(),
                            TypeQL.parseVariable("$x has $a").asThing());
                    txn.commit();
                }
            }
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();

                    EntityType milk = conceptMgr.getEntityType("milk");
                    AttributeType ageInDays = conceptMgr.getAttributeType("age-in-days");

                    Entity milkInst = milk.create();
                    Attribute.Long ageInDays10 = ageInDays.asLong().put(10L);

                    Rule rule = txn.logic().getRule("old-milk-is-not-good");
                    ConceptMap whenAnswer = new ConceptMap(map(pair(Identifier.Variable.name("x"), milkInst),
                                                               pair(Identifier.Variable.name("a"), ageInDays10)));
                    Optional<Map<Identifier.Variable, Concept>> materialisation = rule.conclusion().materialiseAndBind(whenAnswer, txn.traversal(), conceptMgr);
                    assertTrue(materialisation.isPresent());
                    assertEquals(2, materialisation.get().size());

                    List<? extends Attribute> ageInDaysOwned = milkInst.getHas(ageInDays).toList();
                    assertEquals(1, ageInDaysOwned.size());
                    assertEquals(ageInDays10, ageInDaysOwned.get(0));
                }
            }
        }
    }

    @Test
    public void rule_has_explicit_materialises_when_missing() throws IOException {
        Util.resetDirectory(dataDir);

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();

                    EntityType milk = conceptMgr.putEntityType("milk");
                    AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                    AttributeType isStillGood = conceptMgr.putAttributeType("is-still-good", AttributeType.ValueType.BOOLEAN);
                    milk.setOwns(ageInDays);
                    milk.setOwns(isStillGood);
                    txn.logic().putRule(
                            "old-milk-is-not-good",
                            TypeQL.parsePattern("{ $x isa milk, has age-in-days $a; $a >= 10; }").asConjunction(),
                            TypeQL.parseVariable("$x has is-still-good false").asThing());
                    txn.commit();
                }
            }
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();

                    EntityType milk = conceptMgr.getEntityType("milk");
                    AttributeType ageInDays = conceptMgr.getAttributeType("age-in-days");

                    Entity milkInst = milk.create();
                    milkInst.setHas(ageInDays.asLong().put(20L));

                    Rule rule = txn.logic().getRule("old-milk-is-not-good");
                    ConceptMap whenAnswer = new ConceptMap(map(pair(Identifier.Variable.name("x"), milkInst)));
                    Optional<Map<Identifier.Variable, Concept>> materialisation = rule.conclusion().materialiseAndBind(whenAnswer, txn.traversal(), conceptMgr);
                    assertTrue(materialisation.isPresent());
                    assertEquals(3, materialisation.get().size());

                    AttributeType isStillGood = conceptMgr.getAttributeType("is-still-good");
                    List<? extends Attribute> isStillGoodOwned = milkInst.getHas(isStillGood).toList();
                    assertEquals(1, isStillGoodOwned.size());
                    assertEquals(isStillGood.asBoolean().getInstances().first().get(), isStillGoodOwned.get(0));
                }
            }
        }
    }

    // ------------ Rule conclusion indexing ------------

    @Test
    public void rule_indexes_created_and_readable() throws IOException {
        Util.resetDirectory(dataDir);

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
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
                            TypeQL.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            TypeQL.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    Conjunction marriageFriendsThen = marriageFriendsRule.then();
                    Variable marriageFriendsRelation = getVariable(marriageFriendsThen.variables(), Identifier.Variable.anon(0));
                    assertEquals(set(Label.of("friendship")), marriageFriendsRelation.inferredTypes());

                    Rule allFriendsRule = logicMgr.putRule(
                            "all-people-are-friends",
                            TypeQL.parsePattern("{ $x isa person; $y isa person; $t type friendship; }").asConjunction(),
                            TypeQL.parseVariable("(friend: $x, friend: $y) isa $t").asThing());
                    Conjunction allFriendsThen = allFriendsRule.then();
                    Variable allFriendsRelation = getVariable(allFriendsThen.variables(), Identifier.Variable.anon(0));
                    assertEquals(set(Label.of("friendship")), allFriendsRelation.inferredTypes());

                    Rule marriageSameName = logicMgr.putRule(
                            "marriage-same-name",
                            TypeQL.parsePattern("{ $x isa person, has name $a; $y isa person; (spouse:$x, spouse: $y) isa marriage; }").asConjunction(),
                            TypeQL.parseVariable("$y has $a").asThing());
                    Conjunction sameName = marriageSameName.then();
                    Variable nameAttr = getVariable(sameName.variables(), Identifier.Variable.name("a"));
                    assertEquals(set(Label.of("name")), nameAttr.inferredTypes());

                    Rule peopleHaveAge10 = logicMgr.putRule(
                            "people-have-age-10",
                            TypeQL.parsePattern("{ $x isa person; }").asConjunction(),
                            TypeQL.parseVariable("$x has age 10").asThing()
                    );
                    Conjunction age10 = peopleHaveAge10.then();
                    Variable ageAttr = getVariable(age10.variables(), Identifier.Variable.anon(0));
                    assertEquals(set(Label.of("age")), ageAttr.inferredTypes());

                    txn.commit();
                }
            }
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
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

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
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
                            TypeQL.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            TypeQL.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    Conjunction marriageFriendsThen = marriageFriendsRule.then();
                    Variable marriageFriendsRelation = getVariable(marriageFriendsThen.variables(), Identifier.Variable.anon(0));
                    assertEquals(set(Label.of("friendship")), marriageFriendsRelation.inferredTypes());

                    Rule allFriendsRule = logicMgr.putRule(
                            "all-people-are-friends",
                            TypeQL.parsePattern("{ $x isa person; $y isa person; $t type friendship; }").asConjunction(),
                            TypeQL.parseVariable("(friend: $x, friend: $y) isa $t").asThing());
                    Conjunction allFriendsThen = allFriendsRule.then();
                    Variable allFriendsRelation = getVariable(allFriendsThen.variables(), Identifier.Variable.anon(0));
                    assertEquals(set(Label.of("friendship")), allFriendsRelation.inferredTypes());

                    Rule marriageSameName = logicMgr.putRule(
                            "marriage-same-name",
                            TypeQL.parsePattern("{ $x isa person, has name $a; $y isa person; (spouse:$x, spouse: $y) isa marriage; }").asConjunction(),
                            TypeQL.parseVariable("$y has $a").asThing());
                    Conjunction sameName = marriageSameName.then();
                    Variable nameAttr = getVariable(sameName.variables(), Identifier.Variable.name("a"));
                    assertEquals(set(Label.of("name")), nameAttr.inferredTypes());

                    txn.commit();
                }
                // check index after commit, and delete some rules
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
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
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
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

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
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
                            TypeQL.parsePattern("{ $x isa person, has name $a; $y isa person; (spouse:$x, spouse: $y) isa marriage; }").asConjunction(),
                            TypeQL.parseVariable("$y has $a").asThing());
                    Conjunction sameName = marriageSameName.then();
                    Variable nameAttr = getVariable(sameName.variables(), Identifier.Variable.name("a"));
                    assertEquals(set(Label.of("first-name")), nameAttr.inferredTypes());

                    txn.commit();
                }
                // add a new subtype of an attribute in a rule
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    AttributeType lastName = conceptMgr.putAttributeType("last-name", AttributeType.ValueType.STRING);
                    lastName.setSupertype(conceptMgr.getAttributeType("name"));
                    conceptMgr.getEntityType("person").setOwns(lastName);
                    txn.commit();
                }
                // check the new attribute type is re-indexed in the conclusions index
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
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

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
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
                            TypeQL.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            TypeQL.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    Conjunction marriageFriendsThen = marriageFriendsRule.then();
                    Variable marriageFriendsRelation = getVariable(marriageFriendsThen.variables(), Identifier.Variable.anon(0));
                    assertEquals(set(Label.of("friendship")), marriageFriendsRelation.inferredTypes());

                    Rule marriageSameName = logicMgr.putRule(
                            "marriage-same-name",
                            TypeQL.parsePattern("{ $x isa person, has name $a; $y isa person; (spouse:$x, spouse: $y) isa marriage; }").asConjunction(),
                            TypeQL.parseVariable("$y has $a").asThing());
                    Conjunction sameName = marriageSameName.then();
                    Variable nameAttr = getVariable(sameName.variables(), Identifier.Variable.name("a"));
                    assertEquals(set(Label.of("name")), nameAttr.inferredTypes());

                    txn.commit();
                }
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    EntityType person = conceptMgr.getEntityType("person");
                    assertThrowsTypeDBException(person::delete, ErrorMessage.TypeWrite.TYPE_REFERENCED_IN_RULES.code());
                }
            }
        }
    }

    @Test
    public void rule_contains_indexes_allow_deleting_type_after_deleting_rule() throws IOException {
        Util.resetDirectory(dataDir);

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
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
                            TypeQL.parsePattern("{ $x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                            TypeQL.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                    assertIndexTypesContainRule(set(Label.of("person"), Label.of("spouse", "marriage"),
                                                    Label.of("marriage"), Label.of("friend", "friendship"), Label.of("friendship")),
                                                marriageFriendsRule.getLabel(),
                                                graphMgr
                    );

                    Rule marriageSameName = logicMgr.putRule(
                            "marriage-same-name",
                            TypeQL.parsePattern("{ $x isa person, has name $a; $y isa person; (spouse:$x, spouse: $y) isa marriage; }").asConjunction(),
                            TypeQL.parseVariable("$y has $a").asThing());
                    assertIndexTypesContainRule(set(Label.of("person"), Label.of("spouse", "marriage"), Label.of("marriage"), Label.of("name")),
                                                marriageSameName.getLabel(),
                                                graphMgr
                    );

                    txn.commit();
                }
                // check the rule index is still established after commit
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
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
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    RelationType friendship = conceptMgr.getRelationType("friendship");
                    assertThrowsTypeDBException(friendship::delete, ErrorMessage.TypeWrite.TYPE_REFERENCED_IN_RULES.code());
                    assertTrue(!txn.isOpen());
                }
                // deleting an attribute type used in a rule should throw
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    AttributeType name = conceptMgr.getAttributeType("name");
                    assertThrowsTypeDBException(name::delete, ErrorMessage.TypeWrite.TYPE_REFERENCED_IN_RULES.code());
                    assertTrue(!txn.isOpen());
                }
                // deleting a rule, then an attribute type used in the rule is allowed
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    AttributeType name = conceptMgr.getAttributeType("name");

                    LogicManager logicMgr = txn.logic();
                    Rule marriageSameName = logicMgr.getRule("marriage-same-name");
                    marriageSameName.delete();
                    assertNotThrows(name::delete);
                    txn.commit();
                }
                // deleting a rule, then an entity type used in the rule is allowed
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    ConceptManager conceptMgr = txn.concepts();
                    RelationType person = conceptMgr.getRelationType("friendship");
                    LogicManager logicMgr = txn.logic();
                    Rule marriageIsFriendship = logicMgr.getRule("marriage-is-friendship");
                    marriageIsFriendship.delete();
                    assertNotThrows(person::delete);
                    txn.commit();
                }
                // after all rules are deleted, no rules should exist in the index
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.READ)) {
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
    public void rule_then_that_cannot_be_satisfied_throws_an_error() throws IOException {
        Util.resetDirectory(dataDir);

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();

                    final EntityType person = conceptMgr.putEntityType("person");
                    final EntityType dog = conceptMgr.putEntityType("dog");
                    final AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                    person.setOwns(name);

                    ThingVariable<?> then = TypeQL.parseVariable("$x has name 'fido'").asThing();
                    assertThrowsTypeDBException(() -> txn.logic().putRule(
                            "dogs-are-named-fido",
                            TypeQL.parsePattern("{$x isa dog;}").asConjunction(),
                            then
                    ), RULE_THEN_CANNOT_BE_SATISFIED.code());
                }
            }
        }
    }

    @Test
    public void rule_when_that_cannot_be_satisfied_throws_an_error() throws IOException {
        Util.resetDirectory(dataDir);

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final ConceptManager conceptMgr = txn.concepts();

                    final EntityType person = conceptMgr.putEntityType("person");
                    final EntityType dog = conceptMgr.putEntityType("dog");
                    final AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                    person.setOwns(name);

                    // a when using an illegal comparator
                    assertThrowsTypeDBException(() -> txn.logic().putRule(
                            "two-unique-dogs-exist-called-fido",
                            TypeQL.parsePattern("{$x isa dog; $y isa dog; $x != $y;}").asConjunction(),
                            TypeQL.parseVariable("$x has name 'fido'").asThing()
                    ), RULE_WHEN_CANNOT_BE_SATISFIED.code());
                }
            }
        }
    }

    @Test
    public void rule_that_cannot_be_inserted_throws_an_error() throws IOException {
        Util.resetDirectory(dataDir);

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
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
                            TypeQL.parsePattern("{$x isa animal;}").asConjunction(),
                            TypeQL.parseVariable("$x has name 'fido'").asThing()));
                }
            }
        }
    }

    @Test
    public void rule_with_ambiguous_then_throws() throws IOException {
        Util.resetDirectory(dataDir);

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    txn.query().define(TypeQL.parseQuery("define " +
                            "person sub entity, plays marriage:husband, plays marriage:wife;" +
                            "marriage sub relation, relates husband, relates wife;"));

                    assertThrows(() -> txn.logic().putRule(
                            "invalid-marriage-insertion",
                            TypeQL.parsePattern("{$x isa person;}").asConjunction(),
                            TypeQL.parseVariable("(role: $x) isa marriage;").asThing()));
                }
            }
        }
    }

    @Test
    public void rule_with_negated_cycle_throws_an_error() throws IOException {
        Util.resetDirectory(dataDir);

        try (CoreDatabaseManager databaseMgr = CoreDatabaseManager.open(options)) {
            databaseMgr.create(database);
            try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
                try (CoreTransaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {

                    txn.query().define(TypeQL.parseQuery("define " +
                                                                 "person sub entity, owns is-starting-school, owns grade;" +
                                                                 "is-starting-school sub attribute, value boolean;" +
                                                                 "grade sub attribute, value long;" +
                                                                 "rule person-starting-school: when {" +
                                                                 "  $x isa person;" +
                                                                 "  not { $x has is-starting-school true; };" +
                                                                 "} then {" +
                                                                 "  $x has grade 1;" +
                                                                 "};" +
                                                                 "" +
                                                                 "rule person-with-grade-is-in-school: when {" +
                                                                 "  $x isa person, has grade 1;" +
                                                                 "} then {" +
                                                                 "  $x has is-starting-school true;" +
                                                                 "};").asDefine());
                    assertThrowsTypeDBException(txn::commit, CONTRADICTORY_RULE_CYCLE.code());
                }
            }
        }
    }
}
