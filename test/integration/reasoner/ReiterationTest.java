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
import grakn.core.reasoner.resolution.answer.AnswerState;
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
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ReiterationTest {

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

    /*
    This test is intended to force "reiteration" to find the complete set of answers
    It's actually quite hard to find a case that should always reiterate... it may depend on the order of resolution
    and implementation details of materialisations while holding open iterators
    As a result, this test may or may not be the one we want to examine!
     */
    @Test
    public void test_infinite_recursion_in_second_iteration() throws InterruptedException {
        try (RocksSession session = schemaSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().define(Graql.parseQuery(
                        "define " +
                                "X sub relation, relates item, plays Y:item;" +
                                "Y sub relation, relates item, plays X:item;" +
                                "object sub entity, plays X:item, plays Y:item;" +

                                "rule rule-b: when {" +
                                "$r(item:$x) isa X;" +
                                "} then {" +
                                "(item:$r) isa Y;" +
                                "};" +

                                "rule rule-a: when {" +
                                "$r(item:$x) isa Y;" +
                                "} then {" +
                                "(item:$r) isa X;" +
                                "};" +

                                "rule rule-c: when {" +
                                "$o isa object;" +
                                "} then {" +
                                "(item:$o) isa X;" +
                                "};"
                ));
                transaction.commit();
            }
        }
        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                transaction.query().insert(Graql.parseQuery("insert $o isa object;"));
                transaction.commit();
            }
        }

        try (RocksSession session = dataSession()) {
            try (RocksTransaction transaction = singleThreadElgTransaction(session)) {
                Conjunction conjunctionPattern = parseConjunction(transaction, "{ $y isa Y; }");
//                Conjunction conjunctionPattern = parseConjunction(transaction, "{ $y(item:$x) isa Y; }"); // TODO This hangs
                ResolverRegistry registry = transaction.reasoner().resolverRegistry();
                LinkedBlockingQueue<ResolutionAnswer> responses = new LinkedBlockingQueue<>();
                LinkedBlockingQueue<Integer> exhausted = new LinkedBlockingQueue<>();
                int[] iteration = {0};
                int[] doneInIteration = {0};
                boolean[] receivedInferredAnswer = {false};

                Actor<RootResolver> root = registry.createRoot(conjunctionPattern, answer -> {
                    if (answer.isInferred()) receivedInferredAnswer[0] = true;
                    responses.add(answer);
                }, iterDone -> {
                    assert iteration[0] == iterDone;
                    doneInIteration[0]++;
                    exhausted.add(iterDone);
                });

                Set<ResolutionAnswer> answers = new HashSet<>();
                // =========  iteration 0  ============================================================================

                root.tell(actor ->
                                  actor.receiveRequest(
                                          Request.create(new Request.Path(root), AnswerState.DownstreamVars.Root.create(), null),
                                          iteration[0])
                );
                answers.add(responses.take());
                root.tell(actor ->
                                  actor.receiveRequest(
                                          Request.create(new Request.Path(root), AnswerState.DownstreamVars.Root.create(), null),
                                          iteration[0])
                );
                exhausted.take();

//                Thread.sleep(1000); // allow Exhausted message to propagate top toplevel
                assertTrue(receivedInferredAnswer[0]);
                assertEquals(1, doneInIteration[0]);

                // =========  iteration 1  ============================================================================

                // reset for next iteration
                iteration[0]++;
                receivedInferredAnswer[0] = false;
                doneInIteration[0] = 0;

                root.tell(actor ->
                                  actor.receiveRequest(
                                          Request.create(new Request.Path(root), AnswerState.DownstreamVars.Root.create(), null),
                                          iteration[0])
                );
                answers.add(responses.take());
                root.tell(actor ->
                                  actor.receiveRequest(
                                          Request.create(new Request.Path(root), AnswerState.DownstreamVars.Root.create(), null),
                                          iteration[0])
                );
                answers.add(responses.take()); // TODO Should be exhausted here if it weren't for the infinite recursion
//                exhausted.take();
//                for (int i = 2; i <= 6; i++) {
//                    // =============================
//                    // Add more requests than should have answers
//                    root.tell(actor ->
//                                      actor.receiveRequest(
//                                              Request.create(new Request.Path(root), DownstreamVars.Root.create(), null),
//                                              iteration[0])
//                    );
//                    answers.add(responses.take());
//                    assertEquals(i, answers.size());
//                    // =============================
//                }


//                // get last answer and a exhausted message
//                for (int i = 0; i <= answerCount; i++) {
//                    root.tell(actor ->
//                                      actor.receiveRequest(
//                                              Request.create(new Request.Path(root), DownstreamVars.Root.create(), null),
//                                              iteration[0])
//                    );
//                    if (i != answerCount) responses.take();
//                }
//                Thread.sleep(50000); // allow Exhausted message to propagate top toplevel

                assertTrue(receivedInferredAnswer[0]);
                assertEquals(0, doneInIteration[0]);

//                // reset for next iteration
//                iteration[0]++;
//                receivedInferredAnswer[0] = false;
//                doneInIteration[0] = 0;
//                // confirm there are no more answers
//                root.tell(actor ->
//                                  actor.receiveRequest(
//                                          Request.create(new Request.Path(root), DownstreamVars.Root.create(), null),
//                                          iteration[0])
//                );
//                Thread.sleep(1000); // allow Exhausted message to propagate to top level
//                assertFalse(receivedInferredAnswer[0]);
//                assertEquals(1, doneInIteration[0]);
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

}
