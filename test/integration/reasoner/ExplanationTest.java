/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.reasoner.resolution.answer.Explanation;
import com.vaticle.typedb.core.rocks.RocksDatabaseManager;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.concept.answer.ConceptMap.Explainable.NOT_IDENTIFIED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExplanationTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageIndexCacheSize(MB).storageDataCacheSize(MB);
    private static final String database = "explanation-test";
    private static RocksDatabaseManager typedb;

    private RocksTransaction singleThreadElgTransaction(RocksSession session, Arguments.Transaction.Type transactionType) {
        return singleThreadElgTransaction(session, transactionType, new Options.Transaction().infer(true));
    }

    private RocksTransaction singleThreadElgTransaction(RocksSession session, Arguments.Transaction.Type transactionType, Options.Transaction options) {
        RocksTransaction transaction = session.transaction(transactionType, options.infer(true));
        ActorExecutorGroup service = new ActorExecutorGroup(1, new NamedThreadFactory("typedb-actor"));
        transaction.reasoner().resolverRegistry().setExecutorService(service);
        return transaction;
    }

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        typedb = RocksDatabaseManager.open(options);
        typedb.create(database);
    }

    @After
    public void tearDown() {
        typedb.close();
    }

    @Test
    public void test_disjunction_explainable() {
        try (RocksSession session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                ConceptManager conceptMgr = txn.concepts();
                LogicManager logicMgr = txn.logic();

                EntityType person = conceptMgr.putEntityType("person");
                AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                person.setOwns(name);
                RelationType friendship = conceptMgr.putRelationType("friendship");
                friendship.setRelates("friend");
                RelationType marriage = conceptMgr.putRelationType("marriage");
                marriage.setRelates("husband");
                marriage.setRelates("wife");
                person.setPlays(friendship.getRelates("friend"));
                person.setPlays(marriage.getRelates("husband"));
                person.setPlays(marriage.getRelates("wife"));
                logicMgr.putRule(
                        "marriage-is-friendship",
                        TypeQL.parsePattern("{ $x isa person; $y isa person; (husband: $x, wife: $y) isa marriage; }").asConjunction(),
                        TypeQL.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                txn.commit();
            }
        }
        try (RocksSession session = typedb.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Zack'; $y isa person, has name 'Yasmin'; (husband: $x, wife: $y) isa marriage;").asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ, (new Options.Transaction().explain(true)))) {
                List<ConceptMap> ans = txn.query().match(TypeQL.parseQuery(
                        "match $p1 isa person; { (friend: $p1, friend: $p2) isa friendship;} or { $p1 has name 'Zack'; }; "
                ).asMatch()).toList();
                assertEquals(3, ans.size());

                ConceptMap withExplainable;
                ConceptMap withoutExplainable;
                if (ans.get(0).contains("p2")) {
                    withExplainable = ans.get(0);
                    if (!ans.get(1).contains("p2")) withoutExplainable = ans.get(1);
                    else withoutExplainable = ans.get(2);
                } else if (ans.get(1).contains("p2")) {
                    withExplainable = ans.get(1);
                    if (!ans.get(0).contains("p2")) withoutExplainable = ans.get(0);
                    else withoutExplainable = ans.get(2);
                } else {
                    withExplainable = ans.get(2);
                    if (!ans.get(0).contains("p2")) withoutExplainable = ans.get(0);
                    else withoutExplainable = ans.get(1);
                }

                assertEquals(3, withExplainable.concepts().size());
                assertEquals(2, withoutExplainable.concepts().size());

                assertFalse(withExplainable.explainables().isEmpty());
                assertTrue(withoutExplainable.explainables().isEmpty());

                assertSingleExplainableExplanations(withExplainable, 1, 1, 1, txn);
            }
        }
    }

    @Test
    public void test_relation_explainable() {
        try (RocksSession session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                ConceptManager conceptMgr = txn.concepts();
                LogicManager logicMgr = txn.logic();

                EntityType person = conceptMgr.putEntityType("person");
                AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                person.setOwns(name);
                RelationType friendship = conceptMgr.putRelationType("friendship");
                friendship.setRelates("friend");
                RelationType marriage = conceptMgr.putRelationType("marriage");
                marriage.setRelates("husband");
                marriage.setRelates("wife");
                person.setPlays(friendship.getRelates("friend"));
                person.setPlays(marriage.getRelates("husband"));
                person.setPlays(marriage.getRelates("wife"));
                logicMgr.putRule(
                        "marriage-is-friendship",
                        TypeQL.parsePattern("{ $x isa person; $y isa person; (husband: $x, wife: $y) isa marriage; }").asConjunction(),
                        TypeQL.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                txn.commit();
            }
        }
        try (RocksSession session = typedb.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Zack'; $y isa person, has name 'Yasmin'; (husband: $x, wife: $y) isa marriage;").asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ, (new Options.Transaction().explain(true)))) {
                List<ConceptMap> ans = txn.query().match(TypeQL.parseQuery("match (friend: $p1, friend: $p2) isa friendship; $p1 has name $na;").asMatch()).toList();
                assertEquals(2, ans.size());

                assertFalse(ans.get(0).explainables().isEmpty());
                assertFalse(ans.get(1).explainables().isEmpty());

                assertSingleExplainableExplanations(ans.get(0), 1, 1, 1, txn);
                assertSingleExplainableExplanations(ans.get(1), 1, 1, 1, txn);
            }
        }
    }

    @Test
    public void test_relation_explainable_multiple_ways() {
        try (RocksSession session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                ConceptManager conceptMgr = txn.concepts();
                LogicManager logicMgr = txn.logic();

                EntityType person = conceptMgr.putEntityType("person");
                AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                person.setOwns(name);
                RelationType friendship = conceptMgr.putRelationType("friendship");
                friendship.setRelates("friend");
                RelationType marriage = conceptMgr.putRelationType("marriage");
                marriage.setRelates("husband");
                marriage.setRelates("wife");
                person.setPlays(friendship.getRelates("friend"));
                person.setPlays(marriage.getRelates("husband"));
                person.setPlays(marriage.getRelates("wife"));
                logicMgr.putRule(
                        "marriage-is-friendship",
                        TypeQL.parsePattern("{ $x isa person; $y isa person; (husband: $x, wife: $y) isa marriage; }").asConjunction(),
                        TypeQL.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                logicMgr.putRule(
                        "everyone-is-friends",
                        TypeQL.parsePattern("{ $x isa person; $y isa person; not { $x is $y; }; }").asConjunction(),
                        TypeQL.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                txn.commit();
            }
        }
        try (RocksSession session = typedb.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa person, has name 'Zack'; $y isa person, has name 'Yasmin'; (husband: $x, wife: $y) isa marriage;").asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ, (new Options.Transaction().explain(true)))) {
                List<ConceptMap> ans = txn.query().match(TypeQL.parseQuery("match (friend: $p1, friend: $p2) isa friendship; $p1 has name $na;").asMatch()).toList();
                assertEquals(2, ans.size());

                assertFalse(ans.get(0).explainables().isEmpty());
                assertFalse(ans.get(1).explainables().isEmpty());

                assertSingleExplainableExplanations(ans.get(0), 1, 1, 3, txn);
                assertSingleExplainableExplanations(ans.get(1), 1, 1, 3, txn);
            }
        }
    }

    @Test
    public void test_has_explicit_explainable_two_ways() {
        try (RocksSession session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                ConceptManager conceptMgr = txn.concepts();
                LogicManager logicMgr = txn.logic();

                EntityType milk = conceptMgr.putEntityType("milk");
                AttributeType ageInDays = conceptMgr.putAttributeType("age-in-days", AttributeType.ValueType.LONG);
                AttributeType isStillGood = conceptMgr.putAttributeType("is-still-good", AttributeType.ValueType.BOOLEAN);
                milk.setOwns(ageInDays);
                milk.setOwns(isStillGood);
                logicMgr.putRule(
                        "old-milk-is-not-good",
                        TypeQL.parsePattern("{ $x isa milk, has age-in-days <= 10; }").asConjunction(),
                        TypeQL.parseVariable("$x has is-still-good true").asThing());
                logicMgr.putRule(
                        "all-milk-is-good",
                        TypeQL.parsePattern("{ $x isa milk; }").asConjunction(),
                        TypeQL.parseVariable("$x has is-still-good true").asThing());
                txn.commit();
            }
        }

        try (RocksSession session = typedb.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 5;").asInsert());
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 10;").asInsert());
                txn.query().insert(TypeQL.parseQuery("insert $x isa milk, has age-in-days 15;").asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ, (new Options.Transaction().explain(true)))) {
                List<ConceptMap> ans = txn.query().match(TypeQL.parseQuery("match $x has is-still-good $a;").asMatch()).toList();
                assertEquals(3, ans.size());

                assertFalse(ans.get(0).explainables().isEmpty());
                assertFalse(ans.get(1).explainables().isEmpty());
                assertFalse(ans.get(2).explainables().isEmpty());

                AttributeType ageInDays = txn.concepts().getAttributeType("age-in-days");
                if (ans.get(0).get("x").asThing().getHas(ageInDays).first().get().asLong().getValue().equals(15L)) {
                    assertSingleExplainableExplanations(ans.get(0), 0, 1, 1, txn);
                } else assertSingleExplainableExplanations(ans.get(0), 0, 1, 2, txn);

                if (ans.get(1).get("x").asThing().getHas(ageInDays).first().get().asLong().getValue().equals(15L)) {
                    assertSingleExplainableExplanations(ans.get(1), 0, 1, 1, txn);
                } else assertSingleExplainableExplanations(ans.get(1), 0, 1, 2, txn);

                if (ans.get(2).get("x").asThing().getHas(ageInDays).first().get().asLong().getValue().equals(15L)) {
                    assertSingleExplainableExplanations(ans.get(2), 0, 1, 1, txn);
                } else assertSingleExplainableExplanations(ans.get(2), 0, 1, 2, txn);
            }
        }
    }

    @Test
    public void test_has_variable_explainable_two_ways() {
        try (RocksSession session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                LogicManager logicMgr = txn.logic();
                txn.query().define(TypeQL.parseQuery("define " +
                                                             "user sub entity, " +
                                                             "  plays group-membership:member, " +
                                                             "  owns permission; " +
                                                             "user-group sub entity," +
                                                             "  plays group-membership:user-group," +
                                                             "  owns permission," +
                                                             "  owns name; " +
                                                             "group-membership sub relation," +
                                                             "  relates member," +
                                                             "  relates user-group; " +
                                                             "permission sub attribute, value string;" +
                                                             "name sub attribute, value string;"
                ).asDefine());
                logicMgr.putRule(
                        "admin-group-gives-permissions",
                        TypeQL.parsePattern("{ $x isa user; ($x, $g) isa group-membership; $g isa user-group, has name \"admin\", has permission $p; }").asConjunction(),
                        TypeQL.parseVariable("$x has $p").asThing());
                logicMgr.putRule(
                        "writer-group-gives-permissions",
                        TypeQL.parsePattern("{ $x isa user; ($x, $g) isa group-membership; $g isa user-group, has name \"write\", has permission $p; }").asConjunction(),
                        TypeQL.parseVariable("$x has $p").asThing());
                txn.commit();
            }
        }

        try (RocksSession session = typedb.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert " +
                                                             "$x isa user; " +
                                                             "$wg isa user-group, has name \"write\", has permission \"write\";" +
                                                             "(member: $x, user-group: $wg) isa group-membership;" +
                                                             "$admin isa user-group, has name \"admin\", has permission \"write\", has permission \"delete\";" +
                                                             "(member: $x, user-group: $admin) isa group-membership;"
                ).asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ, (new Options.Transaction().explain(true)))) {
                List<ConceptMap> ans = txn.query().match(TypeQL.parseQuery("match $x isa user, has permission \"write\";").asMatch()).toList();
                assertEquals(1, ans.size());

                assertFalse(ans.get(0).explainables().isEmpty());
                assertSingleExplainableExplanations(ans.get(0), 1, 1, 2, txn);
            }
        }
    }

    @Test
    public void test_all_transitive_explanations() {
        try (RocksSession session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                LogicManager logicMgr = txn.logic();
                txn.query().define(TypeQL.parseQuery("define " +
                                                             "location sub entity, " +
                                                             "  plays location-hierarchy:superior, " +
                                                             "  plays location-hierarchy:subordinate; " +
                                                             "location-hierarchy sub relation," +
                                                             "  relates superior," +
                                                             "  relates subordinate;"
                ).asDefine());
                logicMgr.putRule(
                        "transitive-location",
                        TypeQL.parsePattern("{ (subordinate: $x, superior: $y) isa location-hierarchy;" +
                                                    "(subordinate: $y, superior: $z) isa location-hierarchy; }").asConjunction(),
                        TypeQL.parseVariable("(subordinate: $x, superior: $z) isa location-hierarchy").asThing());
                txn.commit();
            }
        }

        try (RocksSession session = typedb.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert " +
                                                             "(subordinate: $a, superior: $b) isa location-hierarchy; " +
                                                             "(subordinate: $b, superior: $c) isa location-hierarchy; " +
                                                             "(subordinate: $c, superior: $d) isa location-hierarchy; " +
                                                             "(subordinate: $d, superior: $e) isa location-hierarchy; " +
                                                             "$a isa location; $b isa location; $c isa location;" +
                                                             "$d isa location; $e isa location;"

                ).asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ, (new Options.Transaction().explain(true)))) {
                List<ConceptMap> ans = txn.query().match(TypeQL.parseQuery("match $r isa location-hierarchy;").asMatch()).toList();
                assertEquals(10, ans.size());

                List<ConceptMap> explainableMaps = iterate(ans).filter(answer -> !answer.explainables().isEmpty()).toList();
                assertEquals(6, explainableMaps.size());

                Map<Pair<ConceptMap, ConceptMap.Explainable>, List<Explanation>> allExplanations = new HashMap<>();
                for (ConceptMap explainableMap : explainableMaps) {
                    List<ConceptMap.Explainable> explainables = explainableMap.explainables().iterator().toList();
                    assertEquals(1, explainables.size());
                    List<Explanation> explanations = txn.query().explain(explainables.get(0).id()).toList();
                    allExplanations.put(new Pair<>(explainableMap, explainables.get(0)), explanations);
                }

                int oneExplanation = 0;
                int twoExplanations = 0;
                int threeExplanations = 0;
                for (Map.Entry<Pair<ConceptMap, ConceptMap.Explainable>, List<Explanation>> entry : allExplanations.entrySet()) {
                    List<Explanation> explanations = entry.getValue();
                    if (explanations.size() == 1) oneExplanation++;
                    else if (explanations.size() == 2) twoExplanations++;
                    else if (explanations.size() == 3) threeExplanations++;
                    else fail();
                }
                assertEquals(3, oneExplanation);
                assertEquals(2, twoExplanations);
                assertEquals(1, threeExplanations);
            }
        }
    }

    @Test
    public void test_nested_explanations() {
        try (RocksSession session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                ConceptManager conceptMgr = txn.concepts();
                LogicManager logicMgr = txn.logic();

                EntityType person = conceptMgr.putEntityType("person");
                AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
                AttributeType gender = conceptMgr.putAttributeType("gender", AttributeType.ValueType.STRING);
                EntityType city = conceptMgr.putEntityType("city");
                person.setOwns(name);
                person.setOwns(gender);
                RelationType friendship = conceptMgr.putRelationType("friendship");
                friendship.setRelates("friend");
                RelationType marriage = conceptMgr.putRelationType("marriage");
                marriage.setRelates("husband");
                marriage.setRelates("wife");
                RelationType wedding = conceptMgr.putRelationType("wedding");
                wedding.setRelates("male");
                wedding.setRelates("female");
                wedding.setRelates("location");
                person.setPlays(friendship.getRelates("friend"));
                person.setPlays(marriage.getRelates("husband"));
                person.setPlays(marriage.getRelates("wife"));
                person.setPlays(wedding.getRelates("male"));
                person.setPlays(wedding.getRelates("female"));
                city.setPlays(wedding.getRelates("location"));

                logicMgr.putRule(
                        "wedding-implies-marriage",
                        TypeQL.parsePattern("{ $x isa person, has gender \"male\"; $y isa person, has gender \"female\"; " +
                                                    "$l isa city; (male: $x, female: $y, location: $l) isa wedding; }").asConjunction(),
                        TypeQL.parseVariable("(husband: $x, wife: $y) isa marriage").asThing());
                logicMgr.putRule(
                        "marriage-is-friendship",
                        TypeQL.parsePattern("{ $a isa person; $b isa person; (husband: $a, wife: $b) isa marriage; }").asConjunction(),
                        TypeQL.parseVariable("(friend: $a, friend: $b) isa friendship").asThing());
                txn.commit();
            }
        }

        try (RocksSession session = typedb.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(TypeQL.parseQuery("insert " +
                                                             "(male: $x, female: $y, location: $l) isa wedding;" +
                                                             "$x isa person, has gender \"male\";" +
                                                             "$y isa person, has gender \"female\";" +
                                                             "$l isa city;"
                ).asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ, (new Options.Transaction().explain(true)))) {
                List<ConceptMap> ans = txn.query().match(TypeQL.parseQuery("match ($x) isa friendship;").asMatch()).toList();
                assertEquals(2, ans.size());

                assertFalse(ans.get(0).explainables().isEmpty());
                List<Explanation> explanations = assertSingleExplainableExplanations(ans.get(0), 1, 1, 1, txn);
                Explanation explanation = explanations.get(0);

                assertEquals(txn.logic().getRule("marriage-is-friendship"), explanation.rule());
                assertEquals(2, explanation.variableMapping().size());
                assertEquals(3, explanation.conclusionAnswer().concepts().size());

                ConceptMap marriageIsFriendshipAnswer = explanation.conditionAnswer();
                assertEquals(1, marriageIsFriendshipAnswer.explainables().iterator().count());
                assertSingleExplainableExplanations(marriageIsFriendshipAnswer, 1, 1, 1, txn);
            }
        }
    }

    private List<Explanation> assertSingleExplainableExplanations(ConceptMap ans, int anonymousConcepts, int explainablesCount,
                                                                  int explanationsCount, RocksTransaction txn) {
        List<ConceptMap.Explainable> explainables = ans.explainables().iterator().toList();
        assertEquals(anonymousConcepts, iterate(ans.concepts().keySet()).filter(Identifier::isAnonymous).count());
        assertEquals(explainablesCount, explainables.size());
        ConceptMap.Explainable explainable = explainables.iterator().next();
        assertNotEquals(NOT_IDENTIFIED, explainable.id());
        List<Explanation> explanations = txn.query().explain(explainable.id()).toList();
        assertEquals(explanationsCount, explanations.size());

        explanations.forEach(explanation -> {
            Map<Retrievable, Set<Retrievable>> mapping = explanation.variableMapping();
            ConceptMap projected = applyMapping(mapping, ans);
            projected.concepts().forEach((var, concept) -> {
                assertTrue(explanation.conclusionAnswer().contains(var));
                assertEquals(explanation.conclusionAnswer().get(var), concept);
            });
        });
        return explanations;
    }

    private ConceptMap applyMapping(Map<Retrievable, Set<Retrievable>> mapping, ConceptMap completeMap) {
        Map<Retrievable, Concept> concepts = new HashMap<>();
        mapping.forEach((from, tos) -> {
            assertTrue(completeMap.contains(from));
            Concept concept = completeMap.get(from);
            tos.forEach(mapped -> {
                assertTrue(!concepts.containsKey(mapped) || concepts.get(mapped).equals(concept));
                concepts.put(mapped, concept);
            });
        });
        return new ConceptMap(concepts);
    }
}
