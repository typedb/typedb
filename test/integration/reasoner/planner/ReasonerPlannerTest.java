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
 *
 */
package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.processor.reactive.Monitor;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ReasonerPlannerTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("reasoner-planner-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB);
    private static final String database = "reasoner-planner-test";
    private static CoreDatabaseManager databaseMgr;
    private static CoreSession session;
    private static CoreTransaction transaction;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(options);
        databaseMgr.create(database);
        session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA);
        transaction = session.transaction(Arguments.Transaction.Type.WRITE);

        transaction.query().define(TypeQL.parseQuery("define " +
                "nid sub attribute, value long;\n" +
                "node sub entity, owns nid @key,\n" +
                "   plays edge:from, plays edge:to, plays edge:through,\n" +
                "   plays path:from, plays path:to;\n" +
                "edge sub relation, relates from, relates to, relates through;\n" +
                "path sub relation, relates from, relates to;\n" +
                "\n" +
                "rule path-base:\n" +
                "when { (from: $n1, to: $n2) isa edge; }\n" +
                "then { (from: $n1, to: $n2) isa path; };\n" +
                "rule path-recursive:\n" +
                "when { (from: $n1, to: $n2) isa path; (from: $n2, to: $n3) isa edge; }\n" +
                "then { (from: $n1, to: $n3) isa path; };\n"));
        transaction.commit();
        session.close();

        session = databaseMgr.session(database, Arguments.Session.Type.DATA);
        transaction = session.transaction(Arguments.Transaction.Type.WRITE);
        transaction.query().insert(TypeQL.parseQuery("insert " +
                "$n1 isa node, has nid 1;" +
                "$n2 isa node, has nid 2;" +
                "   (from: $n2, to: $n1, through: $n2) isa edge;" +
                "$n3 isa node, has nid 3;" +
                "   (from: $n3, to: $n1, through: $n3) isa edge;" +
                "$n4 isa node, has nid 4;" +
                "   (from: $n4, to: $n2, through: $n2) isa edge;" +
                "$n5 isa node, has nid 5;" +
                "   (from: $n5, to: $n1, through: $n5) isa edge;" +
                "$n6 isa node, has nid 6;" +
                "   (from: $n6, to: $n3, through: $n2) isa edge;" +
                "$n7 isa node, has nid 7;" +
                "   (from: $n7, to: $n1, through: $n7) isa edge;" +
                "$n8 isa node, has nid 8;" +
                "   (from: $n8, to: $n4, through: $n2) isa edge;" +
                "$n9 isa node, has nid 9;" +
                "   (from: $n9, to: $n3, through: $n3) isa edge;" +
                "$n10 isa node, has nid 10;" +
                "   (from: $n10, to: $n5, through: $n2) isa edge;" +
                "$n11 isa node, has nid 11;" +
                "   (from: $n11, to: $n1, through: $n11) isa edge;" +
                "$n12 isa node, has nid 12;" +
                "   (from: $n12, to: $n6, through: $n2) isa edge;" +
                "$n13 isa node, has nid 13;" +
                "   (from: $n13, to: $n1, through: $n13) isa edge;" +
                "$n14 isa node, has nid 14;" +
                "   (from: $n14, to: $n7, through: $n2) isa edge;" +
                "$n15 isa node, has nid 15;" +
                "   (from: $n15, to: $n5, through: $n3) isa edge;"));
        transaction.commit();
        session.close();
        session = databaseMgr.session(database, Arguments.Session.Type.DATA);
    }

    private CoreTransaction reasoningTransaction() {
        if (transaction != null) transaction.close();
        transaction = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true).perfCounters(true));
        return transaction;
    }

    private List<ConceptMap> runQuery(CoreTransaction transaction, String query) {
        TypeQLMatch typeQLQuery = TypeQL.parseQuery(query).asMatch();
        return transaction.query().match(typeQLQuery).toList();
    }

    @After
    public void tearDown() {
        if (transaction != null) transaction.close();
        session.close();
        databaseMgr.close();
    }

    @Test
    public void test_answers_flew() {
        long expectedMessagesForSingleHop = 12; // Update this if we introduce an overhead in the reasoner.
        double acceptableRatio = 1.1;
        long overheadForSingleHop = 4;
        long messagesForSingleHop;
        {
            CoreTransaction baselineTx = reasoningTransaction();
            List<ConceptMap> oneHopeOneAnswer = runQuery(baselineTx, "match $n2 isa node, has nid 2; (from: $n2, to: $nx) isa path;");
            assertEquals(1L, oneHopeOneAnswer.size()); // For now the retrieval cost is just the answer-count

            long messagesForSingleHopAndOverhead = baselineTx.context().perfCounter().get(Monitor.PERFCOUNTER_KEY_ANSWERSCREATED);
            assertTrue("calibration failed. See comment", messagesForSingleHopAndOverhead == expectedMessagesForSingleHop);

            messagesForSingleHop = messagesForSingleHopAndOverhead - overheadForSingleHop;
        }

        {
            CoreTransaction tx = reasoningTransaction();
            List<ConceptMap> answers = runQuery(tx, "match $n2 isa node, has nid 2; $n1 isa node, has nid 1; (from: $n2, to: $n1) isa path;");
            assertEquals(1L, answers.size()); // For now the retrieval cost is just the answer-count
            long messagesSent = tx.context().perfCounter().get(Monitor.PERFCOUNTER_KEY_ANSWERSCREATED);
            long expectedHops = 1;
            long expectedOverhead = 4 + overheadForSingleHop; // to bind $n1
            assertTrue(String.format("%d < %.1f", messagesSent, acceptableRatio * expectedHops * messagesForSingleHop + expectedOverhead),
                    (double)messagesSent < acceptableRatio * expectedHops * messagesForSingleHop + expectedOverhead);
        }

        {
            CoreTransaction tx = reasoningTransaction();
            List<ConceptMap> answers = runQuery(tx, "match $n12 isa node, has nid 12; $n1 isa node, has nid 1; (from: $n12, to: $n1) isa path;");
            assertEquals(1L, answers.size()); // For now the retrieval cost is just the answer-count
            long messagesSent = tx.context().perfCounter().get(Monitor.PERFCOUNTER_KEY_ANSWERSCREATED);
            long expectedHops = 3;
            long expectedOverhead = 4 + overheadForSingleHop; // to bind $n1
            assertTrue(String.format("%d < %.1f", messagesSent, acceptableRatio * expectedHops * messagesForSingleHop + expectedOverhead),
                    (double)messagesSent < acceptableRatio * expectedHops * messagesForSingleHop + expectedOverhead);
        }
    }

    private static Conjunction resolvedConjunction(String query, LogicManager logicMgr) {
        Disjunction disjunction = resolvedDisjunction(query, logicMgr);
        assert disjunction.conjunctions().size() == 1;
        return disjunction.conjunctions().get(0);
    }

    private static Disjunction resolvedDisjunction(String query, LogicManager logicMgr) {
        Disjunction disjunction = Disjunction.create(TypeQL.parsePattern(query).asConjunction().normalise());
        logicMgr.typeInference().applyCombination(disjunction);
        return disjunction;
    }
}
