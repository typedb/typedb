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

package grakn.core.reasoner;

import grakn.common.concurrent.NamedThreadFactory;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ExplainableAnswer;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concurrent.actor.ActorExecutorGroup;
import grakn.core.logic.LogicManager;
import grakn.core.reasoner.resolution.answer.Explanation;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.Identifier.Variable.Retrievable;
import graql.lang.Graql;
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

import static grakn.core.concept.answer.ExplainableAnswer.Explainable.NOT_IDENTIFIED;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ExplanationTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logDir);
    private static final String database = "explanation-test";
    private static RocksGrakn grakn;

    private RocksTransaction singleThreadElgTransaction(RocksSession session, Arguments.Transaction.Type transactionType) {
        RocksTransaction transaction = session.transaction(transactionType, new Options.Transaction().infer(true));
        ActorExecutorGroup service = new ActorExecutorGroup(1, new NamedThreadFactory("grakn-core-actor"));
        transaction.reasoner().resolverRegistry().setExecutorService(service);
        return transaction;
    }

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        grakn = RocksGrakn.open(options);
        grakn.databases().create(database);
    }

    @After
    public void tearDown() {
        grakn.close();
    }

    @Test
    public void test_relation_explainable() {
        try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
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
                        Graql.parsePattern("{ $x isa person; $y isa person; (husband: $x, wife: $y) isa marriage; }").asConjunction(),
                        Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                txn.commit();
            }
        }
        try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(Graql.parseQuery("insert $x isa person, has name 'Zack'; $y isa person, has name 'Yasmin'; (husband: $x, wife: $y) isa marriage;").asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                List<ConceptMap> ans = txn.query().match(Graql.parseQuery("match (friend: $p1, friend: $p2) isa friendship; $p1 has name $na;").asMatch()).toList();
                assertEquals(2, ans.size());

                assertTrue(ans.get(0).explainableAnswer().isPresent());
                assertTrue(ans.get(1).explainableAnswer().isPresent());

                assertSingleExplainableExplanations(ans.get(0), 1, 1, 1, txn);
                assertSingleExplainableExplanations(ans.get(1), 1, 1, 1, txn);
            }
        }
    }

    @Test
    public void test_relation_explainable_two_ways() {
        try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
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
                        Graql.parsePattern("{ $x isa person; $y isa person; (husband: $x, wife: $y) isa marriage; }").asConjunction(),
                        Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                logicMgr.putRule(
                        "everyone-is-friends",
                        Graql.parsePattern("{ $x isa person; $y isa person; not { $x is $y; }; }").asConjunction(),
                        Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
                txn.commit();
            }
        }
        try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(Graql.parseQuery("insert $x isa person, has name 'Zack'; $y isa person, has name 'Yasmin'; (husband: $x, wife: $y) isa marriage;").asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                List<ConceptMap> ans = txn.query().match(Graql.parseQuery("match (friend: $p1, friend: $p2) isa friendship; $p1 has name $na;").asMatch()).toList();
                assertEquals(2, ans.size());

                assertTrue(ans.get(0).explainableAnswer().isPresent());
                assertTrue(ans.get(1).explainableAnswer().isPresent());

                assertSingleExplainableExplanations(ans.get(0), 1, 1, 3, txn);
                assertSingleExplainableExplanations(ans.get(1), 1, 1, 3, txn);
            }
        }
    }

    @Test
    public void test_has_explicit_explainable_two_ways() {
        try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
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
                        Graql.parsePattern("{ $x isa milk, has age-in-days <= 10; }").asConjunction(),
                        Graql.parseVariable("$x has is-still-good true").asThing());
                logicMgr.putRule(
                        "all-milk-is-good",
                        Graql.parsePattern("{ $x isa milk; }").asConjunction(),
                        Graql.parseVariable("$x has is-still-good true").asThing());
                txn.commit();
            }
        }

        try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(Graql.parseQuery("insert $x isa milk, has age-in-days 5;").asInsert());
                txn.query().insert(Graql.parseQuery("insert $x isa milk, has age-in-days 10;").asInsert());
                txn.query().insert(Graql.parseQuery("insert $x isa milk, has age-in-days 15;").asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                List<ConceptMap> ans = txn.query().match(Graql.parseQuery("match $x has is-still-good $a;").asMatch()).toList();
                assertEquals(3, ans.size());

                assertTrue(ans.get(0).explainableAnswer().isPresent());
                assertTrue(ans.get(1).explainableAnswer().isPresent());
                assertTrue(ans.get(2).explainableAnswer().isPresent());

                AttributeType ageInDays = txn.concepts().getAttributeType("age-in-days");
                if (ans.get(0).get("x").asThing().getHas(ageInDays).findFirst().get().asLong().getValue().equals(15L)) {
                    assertSingleExplainableExplanations(ans.get(0), 0, 1, 1, txn);
                } else assertSingleExplainableExplanations(ans.get(0), 0, 1, 2, txn);

                if (ans.get(1).get("x").asThing().getHas(ageInDays).findFirst().get().asLong().getValue().equals(15L)) {
                    assertSingleExplainableExplanations(ans.get(1), 0, 1, 1, txn);
                } else assertSingleExplainableExplanations(ans.get(1), 0, 1, 2, txn);

                if (ans.get(2).get("x").asThing().getHas(ageInDays).findFirst().get().asLong().getValue().equals(15L)) {
                    assertSingleExplainableExplanations(ans.get(2), 0, 1, 1, txn);
                } else assertSingleExplainableExplanations(ans.get(2), 0, 1, 2, txn);
            }
        }
    }

    @Test
    public void test_has_variable_explainable_two_ways() {
        try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                LogicManager logicMgr = txn.logic();
                txn.query().define(Graql.parseQuery("define " +
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
                        Graql.parsePattern("{ $x isa user; ($x, $g) isa group-membership; $g isa user-group, has name \"admin\", has permission $p; }").asConjunction(),
                        Graql.parseVariable("$x has $p").asThing());
                logicMgr.putRule(
                        "writer-group-gives-permissions",
                        Graql.parsePattern("{ $x isa user; ($x, $g) isa group-membership; $g isa user-group, has name \"write\", has permission $p; }").asConjunction(),
                        Graql.parseVariable("$x has $p").asThing());
                txn.commit();
            }
        }

        try (RocksSession session = grakn.session(database, Arguments.Session.Type.DATA)) {
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.WRITE)) {
                txn.query().insert(Graql.parseQuery("insert " +
                                                            "$x isa user; " +
                                                            "$wg isa user-group, has name \"write\", has permission \"write\";" +
                                                            "(member: $x, user-group: $wg) isa group-membership;" +
                                                            "$admin isa user-group, has name \"admin\", has permission \"write\", has permission \"delete\";" +
                                                            "(member: $x, user-group: $admin) isa group-membership;"
                ).asInsert());
                txn.commit();
            }
            try (RocksTransaction txn = singleThreadElgTransaction(session, Arguments.Transaction.Type.READ)) {
                List<ConceptMap> ans = txn.query().match(Graql.parseQuery("match $x isa user, has permission \"write\";").asMatch()).toList();
                assertEquals(1, ans.size());

                assertTrue(ans.get(0).explainableAnswer().isPresent());
                assertSingleExplainableExplanations(ans.get(0), 1, 1, 2, txn);
            }
        }
    }

    private void assertSingleExplainableExplanations(ConceptMap ans, int anonymousConcepts, int explainablesCount, int explanationsCount, RocksTransaction txn) {
        ExplainableAnswer explainableAnswer = ans.explainableAnswer().get();
        assertEquals(ans.concepts().size() + anonymousConcepts, explainableAnswer.completeMap().concepts().size());
        assertEquals(explainablesCount, explainableAnswer.explainables().size());
        ExplainableAnswer.Explainable explainable = explainableAnswer.explainables().iterator().next();
        assertNotEquals(NOT_IDENTIFIED, explainable.explainableId());
        FunctionalIterator<Explanation> explanations = txn.query().explain(explainable.explainableId(), explainableAnswer.completeMap());
        List<Explanation> explList = explanations.toList();
        assertEquals(explanationsCount, explList.size());

        explList.forEach(explanation -> {
            Map<Retrievable, Set<Retrievable>> mapping = explanation.variableMapping();
            ConceptMap completeMap = explainableAnswer.completeMap();
            ConceptMap projected = applyMapping(mapping, completeMap);
            projected.concepts().forEach((var, concept) -> {
                assertTrue(explanation.conclusionAnswer().answer().contains(var));
                assertEquals(explanation.conclusionAnswer().answer().get(var), concept);
            });
        });
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
