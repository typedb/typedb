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
import org.junit.Ignore;
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

    @Ignore
    @Test
    public void test_first_iteration_exhausts_and_second_iteration_recurses_infinitely() throws InterruptedException {
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
                // iteration 0
                sendRootRequest(root, iteration[0]);
                answers.add(responses.take());
                sendRootRequest(root, iteration[0]);
                exhausted.take(); // Block and wait for an exhausted message
                assertTrue(receivedInferredAnswer[0]);
                assertEquals(1, doneInIteration[0]);

                // iteration 1
                // reset for next iteration
                iteration[0]++;
                receivedInferredAnswer[0] = false;
                doneInIteration[0] = 0;

                for (int i = 0; i <= 4; i++) {
                    sendRootRequest(root, iteration[0]);
                    answers.add(responses.take());
                    assertEquals(2 + i, answers.size());
                }
                sendRootRequest(root, iteration[0]);
                exhausted.take();

                // iteration 2
                // reset for next iteration
                iteration[0]++;
                receivedInferredAnswer[0] = false;
                doneInIteration[0] = 0;

                for (int i = 0; i <= 3; i++) {
                    sendRootRequest(root, iteration[0]);
                    answers.add(responses.take());
                }
                sendRootRequest(root, iteration[0]);
                exhausted.take();

                // iteration 3 onwards
                for (int j = 0; j <= 10; j++) {
                    // iteration 3
                    // reset for next iteration
                    iteration[0]++;
                    receivedInferredAnswer[0] = false;
                    doneInIteration[0] = 0;

                    for (int i = 0; i <= 3; i++) {
                        sendRootRequest(root, iteration[0]);
                        answers.add(responses.take());
                    }
                    sendRootRequest(root, iteration[0]);
                    exhausted.take();
                }

                assertTrue(receivedInferredAnswer[0]);
                assertEquals(1, doneInIteration[0]);
            }
        }
    }

    private void sendRootRequest(Actor<RootResolver> root, int iteration) {
        root.tell(actor -> actor.receiveRequest(
                Request.create(new Request.Path(root), AnswerState.DownstreamVars.Root.create(), null),
                iteration)
        );
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
