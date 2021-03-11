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
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertEquals;
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

                ExplainableAnswer explainableAnswer = ans.get(0).explainableAnswer().get();
                assertEquals(ans.get(0).concepts().size() +  1, explainableAnswer.completeMap().concepts().size());
                assertEquals(1, explainableAnswer.explainables().size());
                // TODO use txn.query().explain() after we have a good toString or indexing method
                FunctionalIterator<Explanation> explanations = txn.reasoner().explain(explainableAnswer.explainables().iterator().next(), explainableAnswer.completeMap(), txn.query().getDefaultContext());
                List<Explanation> explList = explanations.toList();
                assertEquals(1, explList.size());

                ExplainableAnswer explainableAnswer2 = ans.get(1).explainableAnswer().get();
                assertEquals(ans.get(1).concepts().size() +  1, explainableAnswer2.completeMap().concepts().size());
                assertEquals(1, explainableAnswer2.explainables().size());
                FunctionalIterator<Explanation> explanations2 = txn.reasoner().explain(explainableAnswer2.explainables().iterator().next(), explainableAnswer2.completeMap(), txn.query().getDefaultContext());
                List<Explanation> explList2 = explanations2.toList();
                assertEquals(1, explList2.size());
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

                ExplainableAnswer explainableAnswer = ans.get(0).explainableAnswer().get();
                assertEquals(ans.get(0).concepts().size() +  1, explainableAnswer.completeMap().concepts().size());
                assertEquals(1, explainableAnswer.explainables().size());
                // TODO use txn.query().explain() after we have a good toString or indexing method
                FunctionalIterator<Explanation> explanations = txn.reasoner().explain(explainableAnswer.explainables().iterator().next(), explainableAnswer.completeMap(), txn.query().getDefaultContext());
                List<Explanation> explList = explanations.toList();
                assertEquals(3, explList.size());

                ExplainableAnswer explainableAnswer2 = ans.get(1).explainableAnswer().get();
                assertEquals(ans.get(1).concepts().size() +  1, explainableAnswer2.completeMap().concepts().size());
                assertEquals(1, explainableAnswer2.explainables().size());
                FunctionalIterator<Explanation> explanations2 = txn.reasoner().explain(explainableAnswer2.explainables().iterator().next(), explainableAnswer2.completeMap(), txn.query().getDefaultContext());
                List<Explanation> explList2 = explanations2.toList();
                assertEquals(3, explList2.size());
            }
        }
    }
}
