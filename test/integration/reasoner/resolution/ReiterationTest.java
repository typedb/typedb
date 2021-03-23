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

package grakn.core.reasoner.resolution;

import grakn.common.concurrent.NamedThreadFactory;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.concurrent.actor.ActorExecutorGroup;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial.Compound.Root;
import grakn.core.reasoner.resolution.answer.AnswerState.Top.Match;
import grakn.core.reasoner.resolution.answer.AnswerStateImpl.TopImpl.MatchImpl.InitialImpl;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionTracer;
import grakn.core.reasoner.resolution.resolver.RootResolver;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import grakn.core.traversal.common.Identifier;
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

import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.parameters.Options.Database;
import static grakn.core.reasoner.resolution.Util.resolvedConjunction;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

public class ReiterationTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("resolution-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).logsDir(logDir);
    private static final String database = "resolution-test";
    private static RocksGrakn grakn;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        grakn = RocksGrakn.open(options);
        grakn.databases().create(database);
        ResolutionTracer.initialise(logDir);
    }

    @After
    public void tearDown() {
        grakn.close();
    }

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
                Conjunction conjunction = resolvedConjunction("{ $y isa Y; }", transaction.logic());
                Set<Identifier.Variable.Name> filter = iterate(conjunction.variables()).map(Variable::id).filter(Identifier::isName)
                        .map(Identifier.Variable::asName).toSet();
                ResolverRegistry registry = transaction.reasoner().resolverRegistry();
                LinkedBlockingQueue<Match.Finished> responses = new LinkedBlockingQueue<>();
                LinkedBlockingQueue<Integer> failed = new LinkedBlockingQueue<>();
                int[] iteration = {0};
                int[] doneInIteration = {0};
                boolean[] receivedInferredAnswer = {false};

                ResolutionTracer.get().start();
                Actor.Driver<RootResolver.Conjunction> root = registry.root(conjunction, answer -> {
                    if (answer.requiresReiteration()) receivedInferredAnswer[0] = true;
                    responses.add(answer);
                }, iterDone -> {
                    assert iteration[0] == iterDone;
                    doneInIteration[0]++;
                    failed.add(iterDone);
                }, throwable -> fail());

                Set<Match.Finished> answers = new HashSet<>();
                // iteration 0
                sendRootRequest(root, filter, iteration[0]);
                answers.add(responses.take());
                ResolutionTracer.get().finish();

                ResolutionTracer.get().start();
                sendRootRequest(root, filter, iteration[0]);
                failed.take(); // Block and wait for an failed message
                ResolutionTracer.get().finish();
                assertTrue(receivedInferredAnswer[0]);
                assertEquals(1, doneInIteration[0]);

                // iteration 1 onwards
                for (int j = 0; j <= 100; j++) {
                    ResolutionTracer.get().start();
                    sendRootRequest(root, filter, iteration[0]);
                    Match.Finished re = responses.poll(100, MILLISECONDS);
                    if (re == null) {
                        Integer ex = failed.poll(100, MILLISECONDS);
                        if (ex == null) {
                            ResolutionTracer.get().finish();
                            fail();
                        }
                        // Reset the iteration
                        iteration[0]++;
                        receivedInferredAnswer[0] = false;
                        doneInIteration[0] = 0;
                    }
                    ResolutionTracer.get().finish();
                }
            } catch (GraknException e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    private void sendRootRequest(Actor.Driver<RootResolver.Conjunction> root, Set<Identifier.Variable.Name> filter, int iteration) {
        Root.Match downstream = InitialImpl.create(filter, new ConceptMap(), root, true).toDownstream();
        root.execute(actor -> actor.receiveRequest(
                Request.create(root, downstream), iteration)
        );
    }

    private RocksSession schemaSession() {
        return grakn.session(database, Arguments.Session.Type.SCHEMA);
    }

    private RocksSession dataSession() {
        return grakn.session(database, Arguments.Session.Type.DATA);
    }

    private RocksTransaction singleThreadElgTransaction(RocksSession session) {
        RocksTransaction transaction = session.transaction(Arguments.Transaction.Type.WRITE);
        ActorExecutorGroup service = new ActorExecutorGroup(1, new NamedThreadFactory("grakn-core-actor"));
        transaction.reasoner().resolverRegistry().setExecutorService(service);
        return transaction;
    }
}
