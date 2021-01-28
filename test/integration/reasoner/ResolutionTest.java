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

import grakn.core.common.parameters.Arguments;
import grakn.core.concurrent.actor.Actor;
import grakn.core.concurrent.actor.EventLoopGroup;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.resolver.RootResolver;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static grakn.core.reasoner.resolution.answer.AnswerState.DownstreamVars;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class ResolutionTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("resolution-test");
    private static String database = "resolution-test";
    private static RocksGrakn grakn;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);
    }

    @After
    public void tearDown() {
        grakn.close();
    }

    @Test
    public void single_retrievable() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age;" +
                                "age sub attribute, value long;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ $p1 has age 24; }");
                createRootAndAssertResponses(transaction, conjunctionPattern, 3L);
            }
        }
    }

    @Test
    public void single_retrievable_with_relation() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24; $t(twin1: $p1, twin2: $p2) isa twins; $p2 isa person;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ $t(twin1: $p1, twin2: $p2) isa twins; $p1 has age $a; }");
                createRootAndAssertResponses(transaction, conjunctionPattern, 3L);
            }
        }
    }

    @Test
    public void test_retrievable_with_no_answers() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "twins sub relation, relates twin1, relates twin2;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.query().insert(Graql.parseQuery("insert $p1 isa person, has age 24;"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ $t(twin1: $p1, twin2: $p2) isa twins; " +
                        "$p1 has age $a; }");
                createRootAndAssertResponses(transaction, conjunctionPattern, 0L);
            }
        }
    }

    @Test
    public void simple_rule() throws InterruptedException {
        // TODO We would like to check that:
        //  - 3 answers come from the direct traversal at the root,
        //  - 3 answers come from the concludable via the rule and its retrievable, checking their sent/received messages
        //  are consistent with our expectation.
        String conjunction1 = "$p1 isa person, has name \"Bob\";";
        String conjunction2 = "$p1 isa person, has age 42;";
        String rulePattern = "rule bobs-are-42: when { $p1 isa person, has name \"Bob\"; } then { $p1 has age 42; };";

        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns name, owns age, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "name sub attribute, value string;" +
                                "twins sub relation, relates twin1, relates twin2;" +
                                rulePattern));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert " + conjunction1));
                transaction.query().insert(Graql.parseQuery("insert " + conjunction1));
                transaction.query().insert(Graql.parseQuery("insert " + conjunction1));
                transaction.query().insert(Graql.parseQuery("insert " + conjunction2));
                transaction.query().insert(Graql.parseQuery("insert " + conjunction2));
                transaction.query().insert(Graql.parseQuery("insert " + conjunction2));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ " + conjunction2 + " }");
                createRootAndAssertResponses(transaction, conjunctionPattern, 6L);
            }
        }
    }

    @Ignore // TODO Un-ignore, ignored until rules are ready to use
    @Test
    public void concludableChainWithRule() throws InterruptedException {
        String conjunction1 = "$p1 isa person, has name \"Bob\"; $p2 isa person; (twin1: $p1, twin2: $p2) isa twins;";
        String conjunction2 = "$p1 isa person, has age 42;";
        String conjunction3 = "$p1 isa person; $p2 isa person; (twin1: $p1, twin2: $p2) isa twins;";
        String rule = "rule bobs-are-42: when { $p1 has name \"Bob\"; } then { $p1 has age 42; };";
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, owns name, plays twins:twin1, plays twins:twin2;" +
                                "age sub attribute, value long;" +
                                "name sub attribute, value string;" +
                                "twins sub relation, relates twin1, relates twin2;" +
                                rule));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert " + conjunction1));
                transaction.query().insert(Graql.parseQuery("insert " + conjunction1));
                transaction.query().insert(Graql.parseQuery("insert " + conjunction1));
                transaction.query().insert(Graql.parseQuery("insert " + conjunction2));
                transaction.query().insert(Graql.parseQuery("insert " + conjunction2));
                transaction.query().insert(Graql.parseQuery("insert " + conjunction2));
                transaction.query().insert(Graql.parseQuery("insert " + conjunction3));
                transaction.query().insert(Graql.parseQuery("insert " + conjunction3));
                transaction.query().insert(Graql.parseQuery("insert " + conjunction3));
                transaction.commit();
            }
        }
        long conjunction1TraversalAnswerCount = 3L;
        long conjunction2TraversalAnswerCount = 3L;
        long conjunction3TraversalAnswerCount = 3L;
        long ruleRetrievbleAnswerCount = 3L;
        long rootTraversalAnswerCount = 0L;
        long answerCount = rootTraversalAnswerCount + (conjunction3TraversalAnswerCount * (conjunction2TraversalAnswerCount
                + ruleRetrievbleAnswerCount + conjunction1TraversalAnswerCount));

        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {

                String rootConjunction = "{ $p1 isa person, has age 42; $p2 isa person; (twin1: $p1, twin2: $p2) isa twins; }";
                Conjunction conjunctionPattern = parseConjunction(transaction, rootConjunction);
                createRootAndAssertResponses(transaction, conjunctionPattern, answerCount);
            }
        }
    }

    @Test
    public void shallowRerequestChain() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define woman sub entity, plays marriage:wife, plays friendship:friend;" +
                                "man sub entity, plays marriage:husband, plays friendship:friend;" +
                                "friendship sub relation, relates friend;" +
                                "marriage sub relation, relates husband, relates wife;" +
                                "rule marriage-is-friendship: when {$x isa man; $y isa woman; " +
                                "(husband: $x, wife: $y) isa marriage; } then { (friend: $x, friend: $y) isa friendship; }; "));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                String insert  = "insert $y isa woman; $x isa man; (husband: $x, wife: $y) isa marriage;";
                transaction.query().insert(Graql.parseQuery(insert));
                transaction.query().insert(Graql.parseQuery(insert));

                String insert2  = "insert $y isa woman; $x isa woman; (wife: $x, wife: $y) isa marriage;";
                transaction.query().insert(Graql.parseQuery(insert2));
                transaction.query().insert(Graql.parseQuery(insert2));

                String insert3  = "insert $y isa man;";
                transaction.query().insert(Graql.parseQuery(insert3));
                transaction.query().insert(Graql.parseQuery(insert3));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                String rootConjunction  = "{ $a isa woman; $b isa man; $f(friend: $a, friend: $b) isa friendship; }";
                Conjunction conjunctionPattern = parseConjunction(transaction, rootConjunction);
                createRootAndAssertResponses(transaction, conjunctionPattern, 2L);
            }
        }
    }

    @Test
    public void deepRerequestChain() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define woman sub entity, plays marriage:wife, plays friendship:friend, plays employment:employee, plays association:associated;" +
                                "man sub entity, plays marriage:husband, plays friendship:friend;" +
                                "friendship sub relation, relates friend;" +
                                "company sub entity, plays employment:employer, plays association:associated;" +
                                "marriage sub relation, relates husband, relates wife;" +
                                "employment sub relation, relates employee, relates employer;" +
                                "association sub relation, relates associated;" +
                                "rule marriage-is-friendship: when {$x isa man; $y isa woman; " +
                                "(husband: $x, wife: $y) isa marriage; } then { (friend: $x, friend: $y) isa friendship; }; " +
                                "rule employment-is-association: when {$x isa woman; $y isa company; " +
                                "(employee: $x, employer: $y) isa employment; } then { (associated: $x, associated: $y) isa association; }; "));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Stream.of(
                        "insert $y isa woman; $x isa man; (husband: $x, wife: $y) isa marriage; (employee: $y, employer: $z) isa employment; $z isa company;",
                        "insert $y isa woman; $x isa man; (husband: $x, wife: $y) isa marriage;",
                        "insert $y isa woman; $x isa woman; (wife: $x, wife: $y) isa marriage;",
                        "insert $y isa man;",
                        "insert $y isa woman;",
                        "insert $y isa woman; (employee: $y, employer: $z) isa employment; $z isa company;",
                        "insert $z isa company;"
                ).forEach(q -> transaction.query().insert(Graql.parseQuery(q)));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ $x isa man; " +
                        "(friend: $x, friend: $y) isa friendship; $y isa woman; (associated: $y, associated: $z) isa association; $z isa company; }");
                createRootAndAssertResponses(transaction, conjunctionPattern, 1L);
            }
        }
    }

    @Ignore // TODO Failing
    @Test
    public void recursiveTerminationAndDeduplication() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define location sub entity, plays containment:container, plays containment:contained;" +
                                "containment sub relation, relates contained, relates container;" +
                                "rule transitive-containment: when {" +
                                "(container:$x, contained:$y) isa containment;" +
                                "(container:$y, contained:$z) isa containment;" +
                                "} then {" +
                                "(container:$x, contained:$z) isa containment;" +
                                "};"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(
                        Graql.parseQuery(
                                "insert " +
                                        "$l1 isa location; $l2 isa location; $l3 isa location; $l4 isa location; " +
                                        "(container:$l1, contained:$l2) isa containment;" +
                                        "(container:$l2, contained:$l3) isa containment;" +
                                        "(container:$l3, contained:$l4) isa containment;"
                        ));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ (container:$l3, contained:$l4) isa containment; }");
                createRootAndAssertResponses(transaction, conjunctionPattern, 6L);
            }
        }
    }

    @Ignore // TODO Un-ignore, ignored until explanations are ready to use
    @Test
    public void answerRecorderTest() throws InterruptedException {
        String atomic1 = "$p1 isa person, has name \"Bob\";";
        String atomic2 = "$p1 isa person, has age 42;";
        String rule = "rule bobs-are-42: when { $p1 has name \"Bob\"; } then { $p1 has age 42; };";
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define person sub entity, owns age, owns name;" +
                                "age sub attribute, value long;" +
                                "name sub attribute, value string;" +
                                rule));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.query().insert(Graql.parseQuery("insert " + atomic1));
                transaction.commit();
            }
        }

        long atomic1TraversalAnswerCount = 3L;
        long ruleTraversalAnswerCount = 0L;
        long atomic2TraversalAnswerCount = 0L;
        long conjunctionTraversalAnswerCount = 3L;
        long answerCount = conjunctionTraversalAnswerCount + atomic2TraversalAnswerCount + ruleTraversalAnswerCount
                + atomic1TraversalAnswerCount;

        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ " + atomic2 + " }");
                ResolverRegistry registry = transaction.reasoner().resolverRegistry();
                LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
                AtomicLong doneReceived = new AtomicLong(0L);
                Actor<RootResolver> root = registry.createRoot(conjunctionPattern, responses::add,
                                                               iterDone -> doneReceived.incrementAndGet());

                for (int i = 0; i < answerCount; i++) {
                    root.tell(actor ->
                                      actor.receiveRequest(
                                              Request.create(new Request.Path(root), DownstreamVars.Root.create(), null),
                                              0)
                    );
                    ResolutionAnswer answer = responses.take();

                    // TODO write more meaningful explanation tests
                    System.out.println(answer);
                }
            }
        }
    }

    /*
    This test is intended to force "reiteration" to find the complete set of answers
    It's actually quite hard to find a case that should always reiterate... it may depend on the order of resolution
    and implementation details of materialisations while holding open iterators
    As a result, this test may or may not be the one we want to examine!
     */
    @Ignore
    @Test
    public void testTransitiveReiteration() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define number-of-letters sub attribute, value long, owns  number-of-letters;" +
                                "rule nol: when {" +
                                "$n 5 isa number-of-letters;" +
                                "} then {" +
                                "$n has number-of-letters 10;" +
                                "};" +
                                "rule nol2: when {" +
                                "$n 10 isa number-of-letters;" +
                                "} then {" +
                                "$n has number-of-letters 5;" +
                                "};"));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $n 5 isa number-of-letters;"));
                transaction.commit();
            }
        }

        int answerCount = 8;

        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ $n1 has $n2; }");
                ResolverRegistry registry = transaction.reasoner().resolverRegistry();
                LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
                int[] iteration = {0};
                int[] doneInIteration = {0};
                boolean[] receivedInferredAnswer = {false};

                Actor<RootResolver> root = registry.createRoot(conjunctionPattern, answer -> {
                    if (answer.isInferred()) receivedInferredAnswer[0] = true;
                    responses.add(answer);
                }, iterDone -> {
                    assert iteration[0] == iterDone;
                    doneInIteration[0]++;
                });

                for (int i = 0; i < answerCount; i++) {
                    root.tell(actor ->
                                      actor.receiveRequest(
                                              Request.create(new Request.Path(root), DownstreamVars.Root.create(), null),
                                              iteration[0])
                    );
                }

                // ONE answer (maybe) only be available in the next iteration, so taking the last answer here would block
                for (int i = 0; i < answerCount - 1; i++) {
                    responses.take();
                }
                Thread.sleep(1000); // allow Exhausted message to propagate top toplevel

                assertTrue(receivedInferredAnswer[0]);
                assertEquals(1, doneInIteration[0]);

                // reset for next iteration
                iteration[0]++;
                receivedInferredAnswer[0] = false;
                doneInIteration[0] = 0;
                // get last answer and a exhausted message
                for (int i = 0; i < 2; i++) {
                    root.tell(actor ->
                                      actor.receiveRequest(
                                              Request.create(new Request.Path(root), DownstreamVars.Root.create(), null),
                                              iteration[0])
                    );
                }
                responses.take();
                Thread.sleep(1000); // allow Exhausted message to propagate top toplevel

                assertTrue(receivedInferredAnswer[0]);
                assertEquals(1, doneInIteration[0]);

                // reset for next iteration
                iteration[0]++;
                receivedInferredAnswer[0] = false;
                doneInIteration[0] = 0;
                // confirm there are no more answers
                root.tell(actor ->
                                  actor.receiveRequest(
                                          Request.create(new Request.Path(root), DownstreamVars.Root.create(), null),
                                          iteration[0])
                );
                Thread.sleep(1000); // allow Exhausted message to propagate to top level
                assertFalse(receivedInferredAnswer[0]);
                assertEquals(1, doneInIteration[0]);
            }
        }
    }

    private Conjunction parseConjunction(RocksTransaction transaction, String query) {
        return transaction.logic().typeResolver().resolve(
                Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next());
    }

    private RocksSession schemaSession() {
        return grakn.session(database, Arguments.Session.Type.SCHEMA);
    }

    private RocksSession dataSession() {
        return grakn.session(database, Arguments.Session.Type.DATA);
    }

    private RocksTransaction singleThreadElgTransaction(RocksSession session) {
        RocksTransaction transaction = session.transaction(Arguments.Transaction.Type.WRITE);
        transaction.reasoner().resolverRegistry().setEventLoopGroup(new EventLoopGroup(1, "grakn-elg"));
        return transaction;
    }

    private void createRootAndAssertResponses(RocksTransaction transaction, Conjunction conjunctionPattern, long answerCount) throws InterruptedException {
        ResolverRegistry registry = transaction.reasoner().resolverRegistry();
        LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
        AtomicLong doneReceived = new AtomicLong(0L);
        Actor<RootResolver> root = registry.createRoot(transaction.logic().typeResolver().resolve(conjunctionPattern), responses::add, iterDone -> doneReceived.incrementAndGet());
        assertResponses(root, responses, doneReceived, answerCount);
    }

    private void assertResponses(Actor<RootResolver> root, LinkedBlockingQueue<ResolutionAnswer> responses,
                                 AtomicLong doneReceived, long answerCount)
            throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long n = answerCount + 1; //total number of traversal answers, plus one expected Exhausted (-1 answer)
        for (int i = 0; i < n; i++) {
            root.tell(actor ->
                              actor.receiveRequest(
                                      Request.create(new Request.Path(root), DownstreamVars.Root.create(), ResolutionAnswer.Derivation.EMPTY),
                                      0)
            );
        }
        int answersFound = 0;
        for (int i = 0; i < n - 1; i++) {
            ResolutionAnswer answer = responses.poll(1000, TimeUnit.MILLISECONDS); // polling prevents the test hanging
            if (answer != null) answersFound += 1;

//            ResolutionAnswer answer = responses.take();
//            answersFound += 1;
        }
        Thread.sleep(1000);
        assertEquals(answerCount, answersFound);
        assertEquals(1, doneReceived.get());
        assertTrue(responses.isEmpty());
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
    }
}
